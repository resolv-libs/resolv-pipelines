""" This script runs an Apache Beam pipeline that transform entries in raw dataset to a well known schema called
 canonical format. The output dataset is saved to the specified output path as a TFRecord file with name
 "canonical.tfrecord".

Usage:
    python canonicalize_dataset_pipeline.py \
        --runner=DirectRunner \
        --direct_running_mode=multi_processing \
        --direct_num_workers=8 \
        --direct_runner_bundle_repeat=0 \
        --dataset_index_file_paths=./resources/datasets/raw/lakh_midi-v1.0.0-clean/index.json \
        --dataset_output_paths=./resources/datasets/canonical/lakh_midi-v1.0.0-clean/midi \
        --dataset_file_types=midi \
        --force_overwrite \
        --logging_level=INFO \
        '--debug_file_pattern=.*'

Run the script with standard 'PipelineOptions' from BEAM and with the following arguments:

    - Required flags:
        --dataset_index_file_paths:
            List of paths to the index files for the datasets to canonicalize.
            The list must be provided as a string with values separated by ','.
        --dataset_output_paths:
            List of output paths for the canonical datasets.
            The list must be provided as a string with values separated by ','.
        --dataset_file_types:
            Type of files to consider from the index of each dataset.
            The list must be provided as a string with values separated by ','.

    - Optional flags:
        --force_overwrite: Whether to overwrite the output canonical datasets if they already exist.
        --logging_level: Set the logging level for the pipeline. Default is 'INFO'.
        --debug: Run the pipeline in debug mode.
        --debug_file_pattern: regEx pattern used to write debug info only if a match is found. Default is '.*'.
"""
import argparse
import logging
from pathlib import Path

import apache_beam as beam
import apache_beam.metrics as beam_metrics
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions
from google.protobuf.json_format import Parse

from resolv_data import DatasetIndex
from resolv_data.canonical import get_canonical_format_by_source_type
from beam.dofn.base import DoFnDebugConfig
from beam.dofn.data.canonical import ReadDatasetEntryFileDoFn, ToCanonicalFormatDoFn
from beam.dofn.utilities import CountElementsDoFn


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--dataset_index_file_paths',
        dest='dataset_index_file_paths',
        type=str,
        required=True,
        help='List of paths to the index files for the datasets to canonicalize.'
             'The list must be provided as a string with values separated by \',\'.')
    parser.add_argument(
        '--dataset_output_paths',
        dest='dataset_output_paths',
        type=str,
        required=True,
        help='List of output paths for the canonical datasets.'
             'The list must be provided as a string with values separated by \',\'.')
    parser.add_argument(
        '--dataset_file_types',
        dest='dataset_file_types',
        type=str,
        required=True,
        help='Type of files to consider from the index of each dataset.'
             'The list must be provided as a string with values separated by \',\'.')
    parser.add_argument(
        '--force_overwrite',
        dest='force_overwrite',
        action='store_true',
        help='Whether to overwrite the output canonical datasets if they already exist.')
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
    return parser


def _get_canonical_format_by_file_type(dataset_index: DatasetIndex, file_type: str):
    dataset_entry = dataset_index.entries[0]
    dataset_file = dataset_entry.files[file_type]
    source_type = Path(dataset_file.path).suffix
    return get_canonical_format_by_source_type(source_type)


def run_pipelines():
    # Parse arguments
    parser = _build_arg_parser()
    known_args, pipeline_args = parser.parse_known_args()

    # Set logging level
    logging.getLogger().setLevel(known_args.logging_level)

    # Get pipeline arguments
    pipeline_options = PipelineOptions(pipeline_args)
    dataset_index_file_paths = known_args.dataset_index_file_paths.split(',')
    dataset_file_types = known_args.dataset_file_types.split(',')
    dataset_output_paths = known_args.dataset_output_paths.split(',')

    # Launch pipeline for each dataset
    for dataset_index_file_path, dataset_file_type, dataset_output_path in zip(dataset_index_file_paths,
                                                                               dataset_file_types,
                                                                               dataset_output_paths):
        logging.info(f'Canonicalizing dataset with index {dataset_index_file_path}...')

        # Launch pipeline
        debug_do_fn_config = DoFnDebugConfig(enabled=known_args.debug,
                                             output_path=dataset_output_path,
                                             output_file_pattern=known_args.debug_file_pattern)
        pipeline = beam.Pipeline(options=pipeline_options)

        # Check if canonical dataset already exist
        canonical_output_dir = f'{dataset_output_path}/canonical'
        canonical_file_suffix = '.tfrecord'
        match_results = FileSystems.match([f'{canonical_output_dir}-*-*-*{canonical_file_suffix}'])
        existing_canonical_paths = [metadata.path for metadata in match_results[0].metadata_list]
        if existing_canonical_paths:
            if not known_args.force_overwrite:
                logging.info(f'Skipping canonicalization of dataset with index {dataset_index_file_path}.'
                             f'Found existing files: {existing_canonical_paths}.'
                             f'To overwrite them execute pipeline with --force-overwrite flag.')
                continue
            else:
                logging.info(f'Deleting exising canonical dataset {existing_canonical_paths}.')
                FileSystems.delete(existing_canonical_paths)

        with FileSystems.open(dataset_index_file_path) as index_file:
            dataset_index = Parse(index_file.read(), DatasetIndex())
            dataset_entries = dataset_index.entries
            data_adapter_type = _get_canonical_format_by_file_type(dataset_index, dataset_file_type)
            _ = (
                    pipeline
                    | 'CreateDatasetTracksPColl' >> beam.Create(dataset_entries)
                    | 'ReadDatasetEntryFile' >> beam.ParDo(ReadDatasetEntryFileDoFn(dataset_name=dataset_index.id,
                                                                                    file_type=dataset_file_type))
                    | 'ToCanonicalFormat' >> beam.ParDo(ToCanonicalFormatDoFn(debug_config=debug_do_fn_config))
                    | 'CountElements' >> beam.ParDo(CountElementsDoFn(name='num_processed_sequences'))
                    | 'WriteToTFRecord' >> beam.io.WriteToTFRecord(
                                                        file_path_prefix=canonical_output_dir,
                                                        file_name_suffix=canonical_file_suffix,
                                                        coder=beam.coders.ProtoCoder(data_adapter_type))
            )

            # Wait for pipeline to finish
            results = pipeline.run()
            results.wait_until_finish()

            # Log global metrics
            metrics = results.metrics()
            num_processed_sequences = metrics.query(
                beam_metrics.MetricsFilter().with_namespace('stats').with_name('num_processed_sequences')
            )['counters'][0].result
            logging.info(f'Number of extracted sequences: {num_processed_sequences}')
            conversion_errors_counter = metrics.query(
                beam_metrics.MetricsFilter().with_namespace('stats').with_name('conversion_errors')
            )['counters']
            if conversion_errors_counter:
                logging.info(f'Number of canonicalization errors: {conversion_errors_counter[0].result}')


if __name__ == '__main__':
    run_pipelines()
