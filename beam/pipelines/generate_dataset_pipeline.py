""" This script runs an Apache Beam pipeline that generates a new NoteSequence dataset by applying (in the given order)
some processors to each NoteSequence in the given source TFRecord dataset. The output TFRecord file will be written in
the given output path with the name "data.tfrecord".

Usage:
    python generate_dataset_pipeline.py \
        --runner=DirectRunner \
        --direct_running_mode=multi_processing \
        --direct_num_workers=8 \
        --direct_runner_bundle_repeat=0 \
        '--source_dataset_paths=./resources/datasets/canonical/jsb_chorales-v1.0.0-full/mxml/*.tfrecord' \
        --output_dataset_paths=./resources/datasets/generated/4bars_melodies/jsb_chorales-v1.0.0-full/mxml \
        --force_overwrite \
        --logging_level=INFO \
        --debug_output_type=SOURCE \
        '--debug_file_pattern=.*' \
        '--melody_extractor={"enabled": true, "filter_drums": true, "gap_bars": 1, "ignore_polyphonic_notes": true,
            "max_bars_discard": null, "max_bars_truncate": null, "max_pitch": 108, "min_bars": 4, "min_pitch": 21,
            "min_unique_pitches": 3, "order": 3, "search_start_step": 0,
            "valid_programs": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
                25, 26, 27, 28, 29, 30, 31, 40, 41, 42, 43, 44, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 64, 65, 66, 67,
                68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 104, 105, 106, 107, 108,
                 109, 110, 111]}' \
        '--quantizer={"enabled": true, "order": 2, "steps_per_quarter": 4, "steps_per_second": null}' \
        '--time_change_splitter={"enabled": true, "order": 1, "skip_splits_inside_notes": false,
            "keep_time_signatures": ["4/4"]}' \
        '--slicer={"allow_cropped_slices": false, "enabled": true, "hop_size_bars": 1, "order": 4,
            "skip_splits_inside_notes": false, "slice_size_bars": 4, "start_time": 0}'

Run the script with standard 'PipelineOptions' from BEAM and with the following arguments:

    - Required flags:
        --source_dataset_paths:
            List containing the paths to the datasets for which metrics should be computed.
            The list must be provided as a string with values separated by ','.
        --output_dataset_paths:
            List containing the paths where the output datasets will be written.
            The list must be provided as a string with values separated by ','.
        --[processor_id]: Configuration for the processor to be applied.
            Supported processors are:
                - 'melody_extractor': ExtractMelodyDoFn,
                - 'quantizer': QuantizeDoFn,
                - 'splitter': SplitDoFn,
                - 'time_change_splitter': TimeChangeSplitDoFn,
                - 'silence_splitter': SilenceSplitDoFn,
                - 'slicer': SliceDoFn,
                - 'stretcher': StretchDoFn,
                - 'sustainer': SustainDoFn,
                - 'transposer': TransposeDoFn
            Check the respective DoFn for details on the available configuration parameters.

    - Optional flags:
        --distinct: Keep only distinct sequences and discard duplicates after all processors are applied.
        --force_overwrite: Whether to overwrite the output datasets if they already exist.
        --logging_level: Set the logging level for the pipeline. Default is 'INFO'.
        --debug: Run the pipeline in debug mode.
        --debug_output_type: Debug outputs format.
        --debug_file_pattern: regEx pattern used to write debug info only if a match is found. Default is '.*'.
"""
import argparse
import json
import logging
from typing import List

import apache_beam as beam
import apache_beam.metrics as beam_metrics
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions

from resolv_mir import NoteSequence
from beam.dofn.base import DoFnDebugConfig, DebugOutputTypeEnum
from beam.dofn.note_sequence import NS_DO_FN_MAP, DistinctNoteSequences
from beam.dofn.utilities import CountElementsDoFn


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output_dataset_paths',
        dest='output_paths',
        type=str,
        required=True,
        help='List containing the paths where the output datasets will be written.'
             'The list must be provided as a string with values separated by \',\'.')
    parser.add_argument(
        '--source_dataset_paths',
        dest='source_paths',
        type=str,
        required=True,
        help='List containing the paths to the source canonical datasets.'
             'The list must be provided as a string with values separated by \',\'.')
    parser.add_argument(
        '--distinct',
        dest='distinct',
        action='store_true',
        help='Keep only distinct sequences and discard duplicates after all processors are applied.')
    parser.add_argument(
        '--force_overwrite',
        dest='force_overwrite',
        action='store_true',
        help='Whether to overwrite the output generated datasets if they already exist.')
    parser.add_argument(
        '--logging_level',
        dest='logging_level',
        type=str,
        required=False,
        default='INFO',
        help='Set the logging level for the pipeline.')
    parser.add_argument(
        '--debug',
        dest='debug',
        action='store_true',
        help='Run the pipeline in debug mode.')
    parser.add_argument(
        '--debug_output_type',
        dest='debug_output_type',
        help='Debug outputs format.')
    parser.add_argument(
        '--debug_file_pattern',
        dest='debug_file_pattern',
        type=str,
        required=False,
        default='.*',
        help='regEx pattern used to write debug info only if a match is found.')
    # Add Processors arguments
    for do_fn_id, do_fn in NS_DO_FN_MAP.items():
        parser.add_argument(
            f'--{do_fn_id}',
            dest=do_fn_id,
            type=str,
            required=False,
            help=f"Configuration for the {do_fn.name()} processor.")
    return parser


def _get_processors_from_args(known_args):
    do_fn_configs_map = {NS_DO_FN_MAP[prop]: json.loads(getattr(known_args, prop)) for prop in dir(known_args) if prop
                         and prop.endswith("_ps_do_fn") and getattr(known_args, prop)}
    filtered_do_fn_map = {key: value for key, value in do_fn_configs_map.items() if
                          value['enabled'] and value['order'] >= 0}
    sorted_do_fn_map = {key: value for key, value in
                        sorted(filtered_do_fn_map.items(), key=lambda item: item[1]['order'])}
    return sorted_do_fn_map


def run_pipelines() -> List[beam.Pipeline]:
    # Parse arguments
    parser = _build_arg_parser()
    known_args, pipeline_args = parser.parse_known_args()

    # Set logging level
    logging.getLogger().setLevel(known_args.logging_level)

    # Get pipeline arguments
    pipeline_options = PipelineOptions(pipeline_args)
    source_paths = known_args.source_paths.split(',')
    output_paths = known_args.output_paths.split(',')
    ns_processors_do_fn = _get_processors_from_args(known_args)
    logging.info(f'Processors configuration is {ns_processors_do_fn}.')

    # Launch pipeline for each dataset
    for source_path, output_path in zip(source_paths, output_paths):
        logging.info(f'Generating dataset {source_path} to {output_path}...')

        # Launch pipeline
        debug_output_type = None if not known_args.debug_output_type \
            else DebugOutputTypeEnum[known_args.debug_output_type.upper()]
        debug_do_fn_config = DoFnDebugConfig(enabled=known_args.debug,
                                             output_path=output_path,
                                             output_type=debug_output_type,
                                             output_file_pattern=known_args.debug_file_pattern)
        pipeline = beam.Pipeline(options=pipeline_options)

        # Check if generated dataset already exist
        output_dir = f'{output_path}/data'
        file_suffix = '.tfrecord'
        match_results = FileSystems.match([f'{output_dir}-*-*-*{file_suffix}'])
        existing_canonical_paths = [metadata.path for metadata in match_results[0].metadata_list]
        if existing_canonical_paths:
            if not known_args.force_overwrite:
                logging.info(f'Skipping generation for source dataset {source_path}.'
                             f'Found existing files: {existing_canonical_paths}.'
                             f'To overwrite them execute pipeline with --force-overwrite flag.')
                continue
            else:
                logging.info(f'Deleting exising dataset {existing_canonical_paths}.')
                FileSystems.delete(existing_canonical_paths)

        # Read note sequences
        input_ns = pipeline | 'ReadTFRecord' >> beam.io.ReadFromTFRecord(source_path,
                                                                         coder=beam.coders.ProtoCoder(NoteSequence))

        # TODO - Generate dataset pipeline: Logging progress DoFn
        # ns_count = input_ns | "CountTotalElements" >> beam.combiners.Count.Globally()
        # transformed_input_ns = input_ns | "LogProgress" >> beam.ParDo(LogProgress(direct_num_workers),
        #                                                               beam.pvalue.AsSingleton(ns_count))

        # Apply processors
        transformed_input_ns = input_ns
        for do_fn, do_fn_config in ns_processors_do_fn.items():
            transformed_input_ns = (
                    transformed_input_ns
                    | f'{do_fn.__name__}' >> beam.ParDo(do_fn(config=do_fn_config,
                                                              debug_config=debug_do_fn_config))
            )

        # Count transformed sequences
        transformed_input_ns = (transformed_input_ns
                                | 'CountTotalElements' >> beam.ParDo(CountElementsDoFn(name='total_sequences')))

        # Apply distinct if necessary
        output_sequences = transformed_input_ns if not known_args.distinct else (
                transformed_input_ns
                | 'DistinctNoteSequence' >> DistinctNoteSequences(debug_config=debug_do_fn_config)
                | 'CountDistinctElements' >> beam.ParDo(CountElementsDoFn(name='distinct_sequences'))
        )

        # Write note sequences
        _ = (output_sequences
             | 'WriteToTFRecord' >> beam.io.WriteToTFRecord(
                    file_path_prefix=output_dir,
                    file_name_suffix=file_suffix,
                    coder=beam.coders.ProtoCoder(NoteSequence)))

        # Wait for pipeline to finish
        results = pipeline.run()
        results.wait_until_finish()

        # Log metrics for all processors
        logging.info(f'------------- METRICS for {output_path} -------------')
        for do_fn, _ in ns_processors_do_fn.items():
            processor_metrics = results.metrics().query(
                beam_metrics.MetricsFilter().with_namespace(do_fn.namespace())
            )
            logging.info(f'\t------------- {do_fn.name()} -------------')
            for metric_type, metrics in processor_metrics.items():
                if metrics:
                    logging.info(f'\t\t------------- {metric_type} -------------'.capitalize())
                    for metric_result in metrics:
                        logging.info(f'\t\t{metric_result.key.metric.name}: {metric_result.result}')

        # Log global metrics
        total_sequences = results.metrics().query(
            beam_metrics.MetricsFilter().with_namespace('stats').with_name('total_sequences')
        )['counters'][0].result
        logging.info(f'\tTotal extracted sequences: {total_sequences}')

        if known_args.distinct:
            distinct_sequences = results.metrics().query(
                beam_metrics.MetricsFilter().with_namespace('stats').with_name('distinct_sequences')
            )['counters'][0].result
            duplicated_sequences = total_sequences - distinct_sequences
            logging.info(f'\tDistinct extracted sequences: {distinct_sequences}')
            logging.info(f'\tNumber of duplicated sequences: {duplicated_sequences}')
            logging.info(f'\tRatio of duplicated sequences: {duplicated_sequences * 100 / total_sequences}')


if __name__ == '__main__':
    run_pipelines()
