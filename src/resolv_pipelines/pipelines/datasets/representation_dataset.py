""" TODO - pipeline doc """
import logging
import random
from pathlib import Path
from typing import Dict, List, Union, Tuple, Any

import apache_beam as beam
import apache_beam.metrics as beam_metrics
import tensorflow as tf

from .base import DatasetPipeline
from ...canonical import CanonicalFormat
from ..dofn.base import DoFnDebugConfig, DebugOutputTypeEnum, ConfigurableDoFn
from ..dofn.utilities import CountElementsDoFn
from ...data.representation.base import Representation


class RepresentationDatasetPipeline(DatasetPipeline):

    def __init__(self,
                 representation: Representation,
                 canonical_format: CanonicalFormat,
                 augmenters_config: Dict[str, Dict],
                 allowed_augmenters_map: Dict[str, ConfigurableDoFn.__class__],
                 input_path: Union[str, Path],
                 output_path: Union[str, Path],
                 source_dataset_names: List[str],
                 source_dataset_modes: List[str],
                 source_dataset_file_types: List[str],
                 output_dataset_name: str = "",
                 input_path_prefix: str = "",
                 output_path_prefix: str = "data",
                 split_ratios: Dict[str, float] = None,
                 force_overwrite: bool = False,
                 logging_level: str = 'INFO',
                 debug: bool = False,
                 debug_output_type: DebugOutputTypeEnum = DebugOutputTypeEnum.SOURCE,
                 debug_file_pattern: str = ".*",
                 pipeline_options: Dict[str, str] = None):
        super(RepresentationDatasetPipeline, self).__init__(
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
        if split_ratios and (len(split_ratios) < 2 or round(sum(split_ratios.values())) != 1):
            raise ValueError("Split ratios must sum to 1.")
        self._canonical_format = canonical_format
        self._representation = representation
        self._allowed_augmenters_map = allowed_augmenters_map
        self._augmenters_config = augmenters_config
        self._split_ratios = split_ratios
        self._debug = debug
        self._debug_output_type = debug_output_type
        self._debug_file_pattern = debug_file_pattern
        # Set seed to run partitioning deterministically
        random.seed(42)

    @property
    def dataset_output_dir_name(self) -> str:
        return f"representation/{self._output_dataset_name}"

    @property
    def augmenters_do_fn_map(self) -> Dict[ConfigurableDoFn.__class__, Dict[str, Any]]:
        do_fn_configs_map = {self._allowed_augmenters_map[id]: config for id, config in self._augmenters_config.items()}
        filtered_do_fn_map = {key: value for key, value in do_fn_configs_map.items() if value['order'] >= 0}
        sorted_do_fn_map = {key: value for key, value in
                            sorted(filtered_do_fn_map.items(), key=lambda item: item[1]['order'])}
        return sorted_do_fn_map

    def _run_source_dataset_pipeline(self,
                                     pipeline: beam.Pipeline,
                                     source_dataset: Tuple[str, str, str],
                                     dataset_input_path: Path,
                                     dataset_output_path: Path):
        # Read note sequences
        input_ns = pipeline | 'ReadTFRecord' >> beam.io.ReadFromTFRecord(
            file_pattern=str(dataset_input_path / "*.tfrecord"),
            coder=beam.coders.ProtoCoder(self._canonical_format))

        # Apply augmenters
        debug_do_fn_config = DoFnDebugConfig(enabled=self._debug,
                                             output_path=dataset_output_path,
                                             output_type=self._debug_output_type,
                                             output_file_pattern=self._debug_file_pattern)
        augmented_input_ns = input_ns
        for do_fn, do_fn_config in self.augmenters_do_fn_map.items():
            augmented_input_ns = (
                    augmented_input_ns
                    | f'{do_fn.__name__}' >> beam.ParDo(do_fn(config=do_fn_config,
                                                              debug_config=debug_do_fn_config))
            )

        # Apply representation
        output_sequences = augmented_input_ns | beam.Map(self._representation.to_example)

        # Count output sequences
        output_sequences = (output_sequences
                            | 'CountTotalElements' >> beam.ParDo(CountElementsDoFn(name='total_sequences')))

        # Apply split ratios
        datasets = [output_sequences]
        if self._split_ratios:
            datasets = output_sequences | beam.Partition(self._partition_fn,
                                                         len(self._split_ratios.keys()),
                                                         ratios=list(self._split_ratios.values()))

        # Write datasets
        for idx, dataset in enumerate(datasets):
            dataset_name = list(self._split_ratios.keys())[idx]
            dataset_name_cap = dataset_name.capitalize()
            dataset_prefix = f'{dataset_name}_' if self._split_ratios else ""
            output_tfrecord_prefix = str(dataset_output_path / dataset_name /
                                         (dataset_prefix + self._output_path_prefix))
            _ = (dataset
                 | f'Count{dataset_name_cap}Elements' >> beam.ParDo(CountElementsDoFn(name=f'{dataset_name}_sequences'))
                 | f'Write{dataset_name_cap}ToTFRecord' >> beam.io.WriteToTFRecord(
                        file_path_prefix=output_tfrecord_prefix,
                        file_name_suffix=".tfrecord",
                        coder=beam.coders.ProtoCoder(self._canonical_format))
                 )

    def _log_source_dataset_pipeline_metrics(self,
                                             results,
                                             source_dataset: Tuple[str, str, str],
                                             dataset_input_path: Path,
                                             dataset_output_path: Path):
        # Log metrics for all augmenters
        logging.info(f'------------- METRICS for {dataset_input_path} -------------')
        for do_fn, _ in self.augmenters_do_fn_map.items():
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
        for dataset_name, ratio in self._split_ratios.items():
            expected_dataset_sequences = int(total_sequences * ratio)
            extracted_dataset_sequence = results.metrics().query(
                beam_metrics.MetricsFilter().with_namespace('stats').with_name(f'{dataset_name}_sequences')
            )['counters'][0].result
            logging.info(f'\t{dataset_name.capitalize()} extracted sequences: {extracted_dataset_sequence} '
                         f'(expected {expected_dataset_sequences})')

    @staticmethod
    def _partition_fn(_, num_partitions, ratios):
        assert num_partitions == len(ratios)
        return random.choices(range(num_partitions), ratios)[0]
