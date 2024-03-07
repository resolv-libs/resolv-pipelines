""" This script runs an Apache Beam pipeline that computes the specified metrics for NoteSequence in the specified
TFRecord datasets. The output TFRecord file will be written in the same directory with the name "metrics.tfrecord".

Usage:
    python compute_metrics_pipeline.py \
        --runner=DirectRunner \
        --direct_running_mode=multi_processing \
        --direct_num_workers=8 \
        --direct_runner_bundle_repeat=0 \
        --source_dataset_paths=./resources/datasets/generated/4bars_melodies/jsb_chorales-v1.0.0-full/mxml \
        --force_overwrite \
        --logging_level=INFO \
        '--debug_file_pattern=.*' \
        '--toussaint={"bars": 4, "binary": true, "enabled": true}' \
        '--note_density={"bars": 4, "binary": true, "enabled": true}' \
        '--pitch_range={"enabled": true, "num_midi_pitches": 88}' \
        '--contour={"enabled": true, "num_midi_pitches": 88}' \
        '--unique_notes_ratio={"enabled": true, "num_midi_pitches": 88}' \
        '--unique_bigrams_ratio={"enabled": true, "num_midi_pitches": 88}' \
        '--unique_trigrams_ratio={"enabled": true, "num_midi_pitches": 88}' \
        '--dynamic_range={"enabled": true}' \
        '--note_change_ratio={"enabled": true}' \
        '--ratio_note_off_steps={"enabled": true}' \
        '--ratio_hold_note_steps={"enabled": true}' \
        '--repetitive_section_ratio={"enabled": true, "min_repetitions": 4}' \
        '--longest_repetitive_section={"enabled": true, "min_repetitions": 4}'

Run the script with standard 'PipelineOptions' from BEAM and with the following arguments:

    - Required flags:
        --source_dataset_paths:
            List containing the paths to the datasets for which metrics should be computed.
            The list must be provided as a string with values separated by ','.
        --[metric_id]: Configuration for the metric to be computed.
            Supported metrics are:
                - 'toussaint': ToussaintMetricDoFn,
                - 'note_density': NoteDensityMetricDoFn,
                - 'pitch_range': PitchRangeMetricDoFn,
                - 'contour': ContourMetricDoFn,
                - 'unique_notes_ratio': UniqueNotesMetricDoFn,
                - 'unique_bigrams_ratio': UniqueBigramsMetricDoFn,
                - 'unique_trigrams_ratio': UniqueTrigramsMetricDoFn,
                - 'dynamic_range': DynamicRangeMetricDoFn,
                - 'note_change_ratio': NoteChangeMetricDoFn,
                - 'ratio_note_off_steps': RatioNoteOffMetricDoFn,
                - 'ratio_hold_note_steps': RatioHoldNoteMetricDoFn,
                - 'repetitive_section_ratio': RepetitiveSectionDoFn,
                - 'longest_repetitive_section': LongestRepetitiveSectionDoFn
            Check the respective DoFn for details on the available configuration parameters.

    - Optional flags:
        --force_overwrite: Whether to overwrite the output datasets if they already exist.
        --logging_level: Set the logging level for the pipeline. Default is 'INFO'.
        --debug: Run the pipeline in debug mode.
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
from beam.dofn.base import DoFnDebugConfig
from beam.dofn.metrics import METRIC_DO_FN_MAP
from beam.dofn.utilities import CountElementsDoFn


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--source_dataset_paths',
        dest='source_dataset_paths',
        type=str,
        required=True,
        help='List containing the paths to the datasets for which metrics should be computed.'
             'The list must be provided as a string with values separated by \',\'.')
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
        '--debug_file_pattern',
        dest='debug_file_pattern',
        type=str,
        required=False,
        default='.*',
        help='regEx pattern used to write debug info only if a match is found.')
    # Add Metrics arguments
    for do_fn_id, do_fn in METRIC_DO_FN_MAP.items():
        parser.add_argument(
            f'--{do_fn_id}',
            dest=do_fn_id,
            type=str,
            required=False,
            help=f"Configuration for the {do_fn.name()} metric.")
    return parser


def _get_metrics_from_args(known_args):
    do_fn_configs_map = {METRIC_DO_FN_MAP[prop]: json.loads(getattr(known_args, prop)) for prop in dir(known_args) if
                         prop.endswith("_ms_do_fn") and getattr(known_args, prop)}
    filtered_do_fn_map = {key: value for key, value in do_fn_configs_map.items() if value['enabled']}
    return filtered_do_fn_map


def run_pipelines(argv=None) -> List[beam.Pipeline]:
    # Parse arguments
    parser = _build_arg_parser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Set logging level
    logging.getLogger().setLevel(known_args.logging_level)

    # Get pipeline arguments
    pipeline_options = PipelineOptions(pipeline_args)
    source_dataset_paths = known_args.source_dataset_paths.split(',')
    ns_metrics_do_fn = _get_metrics_from_args(known_args)
    logging.info(f'Metrics configuration is {ns_metrics_do_fn}')

    # Launch pipeline for each dataset
    for source_dataset_path in source_dataset_paths:
        logging.info(f'Computing metrics for dataset {source_dataset_path}...')

        # Launch pipeline
        debug_do_fn_config = DoFnDebugConfig(enabled=known_args.debug,
                                             output_path=source_dataset_path,
                                             output_file_pattern=known_args.debug_file_pattern)
        pipeline = beam.Pipeline(options=pipeline_options)

        # Check if generated dataset already exist
        output_dir = f'{source_dataset_path}/metrics'
        match_results = FileSystems.match([f'{output_dir}-*-*-*.tfrecord'])
        existing_canonical_paths = [metadata.path for metadata in match_results[0].metadata_list]
        if existing_canonical_paths:
            if not known_args.force_overwrite:
                logging.info(f'Skipping generation for source dataset {source_dataset_path}.'
                             f'Found existing files: {existing_canonical_paths}.'
                             f'To overwrite them execute pipeline with --force-overwrite flag.')
                continue
            else:
                logging.info(f'Deleting exising dataset {existing_canonical_paths}.')
                FileSystems.delete(existing_canonical_paths)

        # Read input dataset
        input_sequences = (
                pipeline
                | 'ReadTFRecord' >> beam.io.ReadFromTFRecord(f'{source_dataset_path}/data-*-*-*.tfrecord',
                                                             coder=beam.coders.ProtoCoder(NoteSequence))
                | 'CountElements' >> beam.ParDo(CountElementsDoFn(name='num_processed_sequences'))

        )

        # Compute metrics
        output_sequences = input_sequences
        for do_fn, do_fn_config in ns_metrics_do_fn.items():
            output_sequences = (
                    output_sequences
                    | f'{do_fn.__name__}' >> beam.ParDo(do_fn(config=do_fn_config,
                                                              debug_config=debug_do_fn_config))
            )

        # Write note sequences with metrics
        _ = (output_sequences
             | 'WriteToTFRecord' >> beam.io.WriteToTFRecord(
                    file_path_prefix=output_dir,
                    file_name_suffix='.tfrecord',
                    coder=beam.coders.ProtoCoder(NoteSequence)))

        # Wait for pipeline to finish
        results = pipeline.run()
        results.wait_until_finish()

        # Log global metrics
        total_sequences = results.metrics().query(
            beam_metrics.MetricsFilter().with_namespace('stats').with_name('num_processed_sequences')
        )['counters'][0].result
        logging.info(f'\tTotal processed sequences: {total_sequences}')


if __name__ == '__main__':
    run_pipelines()
