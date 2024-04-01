""" TODO - module doc """
import logging
from pathlib import Path
from typing import Dict, List, Union, Tuple, Any

import apache_beam as beam
import apache_beam.metrics as beam_metrics

from .base import DatasetPipeline
from ..dofn.base import DoFnDebugConfig, DebugOutputTypeEnum, ConfigurableDoFn
from ..dofn.mir.symbolic_music.processors import DistinctNoteSequences
from ..dofn.utilities import CountElementsDoFn
from ...canonical import CanonicalFormat


class ProcessDatasetPipeline(DatasetPipeline):

    def __init__(self,
                 canonical_format: CanonicalFormat,
                 allowed_processors_map: Dict[str, ConfigurableDoFn.__class__],
                 processors_config: Dict[str, Dict],
                 input_path: Union[str, Path],
                 output_path: Union[str, Path],
                 source_dataset_names: List[str],
                 source_dataset_modes: List[str],
                 source_dataset_file_types: List[str],
                 output_dataset_name: str = "",
                 input_path_prefix: str = "",
                 output_path_prefix: str = "data",
                 distinct: bool = False,
                 force_overwrite: bool = False,
                 logging_level: str = 'INFO',
                 debug: bool = False,
                 debug_output_type: DebugOutputTypeEnum = DebugOutputTypeEnum.SOURCE,
                 debug_file_pattern: str = ".*",
                 pipeline_options: Dict[str, str] = None):
        super(ProcessDatasetPipeline, self).__init__(
            input_path=input_path,
            output_path=output_path,
            input_path_prefix=input_path_prefix,
            output_path_prefix=output_path_prefix,
            output_dataset_name=output_dataset_name,
            source_dataset_names=source_dataset_names,
            source_dataset_modes=source_dataset_modes,
            source_dataset_file_types=source_dataset_file_types,
            force_overwrite=force_overwrite,
            logging_level=logging_level,
            pipeline_options=pipeline_options
        )
        self._canonical_format = canonical_format
        self._allowed_processors_map = allowed_processors_map
        self._processors_config = processors_config
        self._distinct = distinct
        self._debug = debug
        self._debug_output_type = debug_output_type
        self._debug_file_pattern = debug_file_pattern

    @property
    def dataset_output_dir_name(self) -> str:
        return f"generated/{self._output_dataset_name}"

    @property
    def processors_do_fn_map(self) -> Dict[ConfigurableDoFn.__class__, Dict[str, Any]]:
        do_fn_configs_map = {self._allowed_processors_map[id]: config for id, config in self._processors_config.items()}
        filtered_do_fn_map = {key: value for key, value in do_fn_configs_map.items() if value['order'] >= 0}
        sorted_do_fn_map = {key: value for key, value in
                            sorted(filtered_do_fn_map.items(), key=lambda item: item[1]['order'])}
        return sorted_do_fn_map

    def _run_source_dataset_pipeline(self,
                                     pipeline: beam.Pipeline,
                                     source_dataset: Tuple[str, str, str],
                                     dataset_input_path: Path,
                                     dataset_output_path: Path):
        # Read inputs
        input_file_pattern = str(dataset_input_path / "*.tfrecord")
        inputs = pipeline | 'ReadTFRecord' >> beam.io.ReadFromTFRecord(
                                                            file_pattern=input_file_pattern,
                                                            coder=beam.coders.ProtoCoder(self._canonical_format))

        # TODO - Generate dataset pipeline: Logging progress DoFn
        # ns_count = input_ns | "CountTotalElements" >> beam.combiners.Count.Globally()
        # transformed_input_ns = input_ns | "LogProgress" >> beam.ParDo(LogProgress(direct_num_workers),
        #                                                               beam.pvalue.AsSingleton(ns_count))

        # Apply processors
        debug_do_fn_config = DoFnDebugConfig(enabled=self._debug,
                                             output_path=dataset_output_path,
                                             output_type=self._debug_output_type,
                                             output_file_pattern=self._debug_file_pattern)
        transformed_input = inputs
        for do_fn, do_fn_config in self.processors_do_fn_map.items():
            transformed_input = (
                    transformed_input
                    | f'{do_fn.__name__}' >> beam.ParDo(do_fn(config=do_fn_config,
                                                              debug_config=debug_do_fn_config))
            )

        # Count transformed sequences
        transformed_input = (transformed_input
                                | 'CountTotalElements' >> beam.ParDo(CountElementsDoFn(name='total_sequences')))

        # Apply distinct if necessary
        output_sequences = transformed_input if not self._distinct else (
                transformed_input
                | 'DistinctNoteSequence' >> DistinctNoteSequences(debug_config=debug_do_fn_config)
                | 'CountDistinctElements' >> beam.ParDo(CountElementsDoFn(name='distinct_sequences'))
        )

        # Write note sequences
        output_tfrecord_prefix = str(dataset_output_path / self._output_path_prefix)
        _ = (output_sequences
             | 'WriteToTFRecord' >> beam.io.WriteToTFRecord(file_path_prefix=output_tfrecord_prefix,
                                                            file_name_suffix=".tfrecord",
                                                            coder=beam.coders.ProtoCoder(self._canonical_format))
             )

    def _log_source_dataset_pipeline_metrics(self,
                                             results,
                                             source_dataset: Tuple[str, str, str],
                                             dataset_input_path: Path,
                                             dataset_output_path: Path):
        # Log metrics for all processors
        logging.info(f'------------- METRICS for {dataset_output_path} -------------')
        for do_fn, _ in self.processors_do_fn_map.items():
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

        if self._distinct:
            distinct_sequences = results.metrics().query(
                beam_metrics.MetricsFilter().with_namespace('stats').with_name('distinct_sequences')
            )['counters'][0].result
            duplicated_sequences = total_sequences - distinct_sequences
            logging.info(f'\tDistinct extracted sequences: {distinct_sequences}')
            logging.info(f'\tNumber of duplicated sequences: {duplicated_sequences}')
            logging.info(f'\tRatio of duplicated sequences: {duplicated_sequences * 100 / total_sequences}')
