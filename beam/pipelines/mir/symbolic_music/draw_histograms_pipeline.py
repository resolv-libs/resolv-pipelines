""" This script runs an Apache Beam pipeline that draws histograms with the given number of bins for the given metrics
on the given NoteSequence datasets. The output images will be written in a "histograms" directory placed in the same
path of the source dataset.

Usage:
    python draw_histograms_pipeline.py \
        --runner=DirectRunner \
        --direct_running_mode=multi_processing \
        --direct_num_workers=8 \
        --direct_runner_bundle_repeat=0 \
        --source_dataset_paths=./resources/datasets/generated/4bars_melodies_distinct/lakh_midi-v1.0.0-clean/midi \
        --histogram_metrics=toussaint,note_density,pitch_range,contour,unique_notes_ratio,unique_bigrams_ratio \
        --histogram_bins=20,40,60 \
        --logging_level=INFO

Run the script with standard 'PipelineOptions' from BEAM and with the following arguments:

    - Required flags:
        --source_dataset_paths:
            List containing the paths to the datasets for which entries drawing the histograms.
            The list must be provided as a string with values separated by ','.
        --histogram_metrics:
            List containing the metrics for which to draw the histogram.
            The list must be provided as a string with values separated by ','.

    - Optional flags:
        --histogram_bins:
            List containing the number of bins for the histogram of each metric.
            The list must be provided as a string with values separated by ','.
            Default is '20'.
        --logging_level:
            Set the logging level for the pipeline. Default is 'INFO'.
"""
import argparse
import logging
from typing import List

import apache_beam as beam
import apache_beam.metrics as beam_metrics
from apache_beam.options.pipeline_options import PipelineOptions

from resolv_mir import NoteSequence
from beam.dofn.mir.symbolic_music.metrics import METRIC_DO_FN_MAP
from beam.dofn.utilities import CountElementsDoFn, GenerateHistogram, WriteFileToFileSystem


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--source_dataset_paths',
        dest='source_dataset_paths',
        type=str,
        required=True,
        help='List containing the paths to the source canonical datasets.'
             'The list must be provided as a string with values separated by \',\'.')
    parser.add_argument(
        '--histogram_metrics',
        dest='histogram_metrics',
        type=str,
        required=True,
        help='List containing the metrics for which to draw the histogram.'
             'The list must be provided as a string with values separated by \',\'.')
    parser.add_argument(
        '--histogram_bins',
        dest='histogram_bins',
        required=False,
        default='20',
        help='List containing the number of bins for the histogram of each metric.'
             'The list must be provided as a string with values separated by \',\'.')
    parser.add_argument(
        '--logging_level',
        dest='logging_level',
        type=str,
        required=False,
        default='INFO',
        help='Set the logging level for the pipeline.')
    return parser


def run_pipelines(argv=None) -> List[beam.Pipeline]:
    # Parse arguments
    parser = _build_arg_parser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Set logging level
    logging.getLogger().setLevel(known_args.logging_level)

    # Get pipeline arguments
    pipeline_options = PipelineOptions(pipeline_args)
    source_dataset_paths = known_args.source_dataset_paths.split(',')
    ns_metrics_do_fn = [METRIC_DO_FN_MAP[f'{metric}_ms_do_fn'] for metric in known_args.histogram_metrics.split(',')]

    # Launch pipeline for each dataset
    for source_dataset_path in source_dataset_paths:
        logging.info(f'Drawing histogram for dataset {source_dataset_path}...')
        pipeline = beam.Pipeline(options=pipeline_options)

        # Read input dataset
        input_sequences = (
                pipeline
                | 'ReadTFRecord' >> beam.io.ReadFromTFRecord(f'{source_dataset_path}/metrics-*-*-*.tfrecord',
                                                             coder=beam.coders.ProtoCoder(NoteSequence))
                | 'CountElements' >> beam.ParDo(CountElementsDoFn(name='num_processed_sequences'))

        )

        # Draw histograms
        metrics_fields = [k.proto_message_id() for k in ns_metrics_do_fn]
        histogram_bins_list = [int(b) for b in known_args.histogram_bins.split(',')]
        _ = (
            input_sequences
            | beam.FlatMap(lambda ns: [(m, getattr(ns.metrics, m)) for m in metrics_fields])
            | beam.GroupByKey()
            | beam.Map(lambda kv: (kv[0], list(kv[1])))
            | f'GenerateHistogram' >> beam.ParDo(GenerateHistogram(bins=histogram_bins_list))
            | f'WriteHistogram' >> beam.ParDo(WriteFileToFileSystem(f'{source_dataset_path}/histograms', "image/png"))
        )

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
