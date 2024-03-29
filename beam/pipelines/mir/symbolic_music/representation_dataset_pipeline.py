""" TODO - pipeline doc """
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
from beam.dofn.mir.symbolic_music.augmenters import NS_AUG_DO_FN_MAP
from beam.dofn.mir.symbolic_music.representations import NS_REPR_DO_FN_MAP
from beam.dofn.utilities import CountElementsDoFn


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--source_dataset_paths',
        dest='source_dataset_paths',
        type=str,
        required=True,
        help='List containing the paths to the datasets for which representation should be generated.'
             'The list must be provided as a string with values separated by \',\'.')
    parser.add_argument(
        '--representation',
        dest='representation',
        help='The NoteSequence representation to be used for the output examples.')
    parser.add_argument(
        '--keep_attributes',
        dest='keep_attributes',
        action='store_false',
        help='Whether to to store the attributes related to the sequence in the example contest.')
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
    # Add augmenters arguments
    for do_fn_id, do_fn in NS_AUG_DO_FN_MAP.items():
        parser.add_argument(
            f'--{do_fn_id}',
            dest=do_fn_id,
            type=str,
            required=False,
            help=f"Configuration for the {do_fn.name()} augmenter.")
    return parser


def _get_augmenters_from_args(known_args):
    do_fn_configs_map = {NS_AUG_DO_FN_MAP[prop]: json.loads(getattr(known_args, prop)) for prop in dir(known_args) if
                         prop and prop.endswith("_ps_do_fn") and getattr(known_args, prop)}
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
    source_dataset_paths = known_args.source_dataset_paths.split(',')
    ns_representation_do_fn = NS_REPR_DO_FN_MAP[known_args.representation]
    ns_augmenters_do_fn = _get_augmenters_from_args(known_args)
    logging.info(f'Augmenters configuration is {ns_augmenters_do_fn}.')

    # Launch pipeline for each dataset
    for source_dataset_path in source_dataset_paths:
        logging.info(f'Generating representation for dataset {source_dataset_path}...')

        # Launch pipeline
        debug_output_type = None if not known_args.debug_output_type \
            else DebugOutputTypeEnum[known_args.debug_output_type.upper()]
        debug_do_fn_config = DoFnDebugConfig(enabled=known_args.debug,
                                             output_path=source_dataset_path,
                                             output_type=debug_output_type,
                                             output_file_pattern=known_args.debug_file_pattern)
        pipeline = beam.Pipeline(options=pipeline_options)

        # Check if generated dataset already exist
        output_dir = f'{source_dataset_path}/representation'
        file_suffix = '.tfrecord'
        match_results = FileSystems.match([f'{output_dir}-*-*-*{file_suffix}'])
        existing_representation_paths = [metadata.path for metadata in match_results[0].metadata_list]
        if existing_representation_paths:
            if not known_args.force_overwrite:
                logging.info(f'Skipping generation for source dataset {source_dataset_path}.'
                             f'Found existing files: {existing_representation_paths}.'
                             f'To overwrite them execute pipeline with --force-overwrite flag.')
                continue
            else:
                logging.info(f'Deleting exising dataset {existing_representation_paths}.')
                FileSystems.delete(existing_representation_paths)

        # Read note sequences
        input_ns = pipeline | 'ReadTFRecord' >> beam.io.ReadFromTFRecord(f'{source_dataset_path}/metrics-*-*-*.tfrecord',
                                                                         coder=beam.coders.ProtoCoder(NoteSequence))

        # Apply augmenters
        augmented_input_ns = input_ns
        for do_fn, do_fn_config in ns_augmenters_do_fn.items():
            augmented_input_ns = (
                    augmented_input_ns
                    | f'{do_fn.__name__}' >> beam.ParDo(do_fn(config=do_fn_config,
                                                              debug_config=debug_do_fn_config))
            )

        # Apply representation
        output_sequences = (augmented_input_ns
                            | beam.ParDo(ns_representation_do_fn(keep_attributes=known_args.keep_attributes)))

        # Count output sequences
        output_sequences = (output_sequences
                            | 'CountTotalElements' >> beam.ParDo(CountElementsDoFn(name='total_sequences')))

        # Write note sequences
        _ = (output_sequences
             | 'WriteToTFRecord' >> beam.io.WriteToTFRecord(
                    file_path_prefix=output_dir,
                    file_name_suffix=file_suffix,
                    coder=beam.coders.ProtoCoder(NoteSequence)))

        # Wait for the pipeline to finish
        results = pipeline.run()
        results.wait_until_finish()

        # Log metrics for all augmenters
        logging.info(f'------------- METRICS for {source_dataset_path} -------------')
        for do_fn, _ in ns_augmenters_do_fn.items():
            augmenter_metrics = results.metrics().query(
                beam_metrics.MetricsFilter().with_namespace(do_fn.namespace())
            )
            logging.info(f'\t------------- {do_fn.name()} -------------')
            for metric_type, metrics in augmenter_metrics.items():
                if metrics:
                    logging.info(f'\t\t------------- {metric_type} -------------'.capitalize())
                    for metric_result in metrics:
                        logging.info(f'\t\t{metric_result.key.metric.name}: {metric_result.result}')

        # Log global metrics
        total_sequences = results.metrics().query(
            beam_metrics.MetricsFilter().with_namespace('stats').with_name('total_sequences')
        )['counters'][0].result
        logging.info(f'\tTotal extracted sequences: {total_sequences}')


if __name__ == '__main__':
    run_pipelines()
