""" TODO - module doc """
import logging
from pathlib import Path
from typing import Dict, List, Union, Tuple

import apache_beam as beam
import apache_beam.metrics as beam_metrics
from apache_beam.io.filesystems import FileSystems
from google.protobuf.json_format import Parse
from resolv_data import DatasetIndex

from .base import DatasetPipeline
from ..dofn.base import DoFnDebugConfig
from ..dofn.data.canonical import ReadDatasetEntryFileDoFn, ToCanonicalFormatDoFn
from ..dofn.utilities import CountElementsDoFn
from ...canonical import get_canonical_format_by_source_type


class CanonicalizeDatasetPipeline(DatasetPipeline):

    def __init__(self,
                 input_path: Union[str, Path],
                 output_path: Union[str, Path],
                 source_dataset_names: List[str],
                 source_dataset_modes: List[str],
                 source_dataset_file_types: List[str],
                 input_path_prefix: str = "",
                 output_path_prefix: str = "",
                 force_overwrite: bool = False,
                 logging_level: str = 'INFO',
                 debug: bool = False,
                 debug_file_pattern: str = ".*",
                 pipeline_options: Dict[str, str] = None):
        super(CanonicalizeDatasetPipeline, self).__init__(
            input_path=input_path,
            output_path=output_path,
            input_path_prefix=input_path_prefix,
            output_path_prefix=output_path_prefix,
            source_dataset_names=source_dataset_names,
            source_dataset_modes=source_dataset_modes,
            source_dataset_file_types=source_dataset_file_types,
            force_overwrite=force_overwrite,
            logging_level=logging_level,
            pipeline_options=pipeline_options
        )
        self._debug = debug
        self._debug_file_pattern = debug_file_pattern

    @property
    def dataset_output_dir_name(self) -> str:
        return "canonical"

    def _run_source_dataset_pipeline(self,
                                     pipeline: beam.Pipeline,
                                     source_dataset: Tuple[str, str, str],
                                     dataset_input_path: Path,
                                     dataset_output_path: Path):
        def get_canonical_format_by_file_type():
            dataset_entry = dataset_index.entries[0]
            dataset_file = dataset_entry.files[dataset_file_type]
            source_type = Path(dataset_file.path).suffix
            return get_canonical_format_by_source_type(source_type)

        dataset_file_type = source_dataset[2]
        dataset_index_file_path = dataset_input_path / "index.json"
        index_file = FileSystems.open(str(dataset_index_file_path))
        dataset_index = Parse(index_file.read(), DatasetIndex())
        dataset_entries = dataset_index.entries
        data_adapter_type = get_canonical_format_by_file_type()
        index_file.close()
        debug_do_fn_config = DoFnDebugConfig(enabled=self._debug,
                                             output_path=str(dataset_output_path),
                                             output_file_pattern=self._debug_file_pattern)
        _ = (
                pipeline
                | 'CreateDatasetTracksPColl' >> beam.Create(dataset_entries)
                | 'ReadDatasetEntryFile' >> beam.ParDo(ReadDatasetEntryFileDoFn(dataset_name=dataset_index.id,
                                                                                file_type=dataset_file_type))
                | 'ToCanonicalFormat' >> beam.ParDo(ToCanonicalFormatDoFn(debug_config=debug_do_fn_config))
                | 'CountElements' >> beam.ParDo(CountElementsDoFn(name='num_processed_sequences'))
                | 'WriteToTFRecord' >> beam.io.WriteToTFRecord(file_path_prefix=str(dataset_output_path / "canonical"),
                                                               file_name_suffix=".tfrecord",
                                                               coder=beam.coders.ProtoCoder(data_adapter_type))
        )

    def _log_source_dataset_pipeline_metrics(self,
                                             results,
                                             source_dataset: Tuple[str, str, str],
                                             dataset_input_path: Path,
                                             dataset_output_path: Path):
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
