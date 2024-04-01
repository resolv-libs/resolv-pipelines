""" TODO - module doc """
import logging
from pathlib import Path
from typing import Dict, List, Union, Tuple

import apache_beam as beam
import apache_beam.metrics as beam_metrics

from .base import DatasetPipeline
from ...canonical import CanonicalFormat
from ..dofn.base import ConfigurableDoFn
from ..dofn.utilities import CountElementsDoFn, GenerateHistogram, WriteFileToFileSystem


class DrawHistogramPipeline(DatasetPipeline):

    def __init__(self,
                 canonical_format: CanonicalFormat,
                 attributes: List[str],
                 allowed_attributes_map: Dict[str, ConfigurableDoFn.__class__],
                 input_path: Union[str, Path],
                 source_dataset_names: List[str],
                 source_dataset_modes: List[str],
                 source_dataset_file_types: List[str],
                 input_path_prefix: str = "",
                 bins: List[int] = None,
                 force_overwrite: bool = False,
                 logging_level: str = 'INFO',
                 pipeline_options: Dict[str, str] = None):
        super(DrawHistogramPipeline, self).__init__(
            input_path=input_path,
            output_path=input_path,
            input_path_prefix=input_path_prefix,
            output_path_prefix=f"{input_path_prefix}/histograms",
            source_dataset_names=source_dataset_names,
            source_dataset_modes=source_dataset_modes,
            source_dataset_file_types=source_dataset_file_types,
            force_overwrite=force_overwrite,
            logging_level=logging_level,
            pipeline_options=pipeline_options
        )
        self._canonical_format = canonical_format
        self._attributes = attributes
        self._allowed_attributes_map = allowed_attributes_map
        self._bins = bins if bins else [30]

    @property
    def dataset_output_dir_name(self) -> str:
        return ""

    def _run_source_dataset_pipeline(self,
                                     pipeline: beam.Pipeline,
                                     source_dataset: Tuple[str, str, str],
                                     dataset_input_path: Path,
                                     dataset_output_path: Path):
        # Read input dataset
        input_file_pattern = f'{dataset_input_path}/{self._input_path_prefix}-*-*-*.tfrecord'
        input_sequences = (
                pipeline
                | 'ReadTFRecord' >> beam.io.ReadFromTFRecord(input_file_pattern,
                                                             coder=beam.coders.ProtoCoder(self._canonical_format))
                | 'CountElements' >> beam.ParDo(CountElementsDoFn(name='num_processed_sequences'))

        )

        # Draw histograms
        attributes = [self._allowed_attributes_map[attribute].proto_message_id() for attribute in self._attributes]
        _ = (
            input_sequences
            | beam.FlatMap(lambda x: [(m, getattr(x.attributes, m)) for m in attributes])
            | beam.GroupByKey()
            | beam.Map(lambda kv: (kv[0], list(kv[1])))
            | f'GenerateHistogram' >> beam.ParDo(GenerateHistogram(bins=self._bins))
            | f'WriteHistogram' >> beam.ParDo(WriteFileToFileSystem(dataset_output_path, "image/png"))
        )

    def _log_source_dataset_pipeline_metrics(self,
                                             results,
                                             source_dataset: Tuple[str, str, str],
                                             dataset_input_path: Path,
                                             dataset_output_path: Path):
        total_sequences = results.metrics().query(
            beam_metrics.MetricsFilter().with_namespace('stats').with_name('num_processed_sequences')
        )['counters'][0].result
        logging.info(f'\tTotal processed sequences: {total_sequences}')
