import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Union, Tuple

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions
from resolv_data import get_dataset_root_dir_name


class DatasetPipeline(ABC):

    def __init__(self,
                 input_path: Union[str, Path],
                 output_path: Union[str, Path],
                 source_dataset_names: List[str],
                 source_dataset_modes: List[str],
                 source_dataset_file_types: List[str] = None,
                 input_path_prefix: str = "",
                 output_path_prefix: str = "",
                 force_overwrite: bool = False,
                 logging_level: str = 'INFO',
                 pipeline_options: Dict[str, str] = None):
        self._input_path = Path(input_path) if input_path else None
        self._output_path = Path(output_path) if output_path else None
        self._input_path_prefix = input_path_prefix
        self._output_path_prefix = output_path_prefix
        if not source_dataset_file_types:
            source_dataset_file_types = [None] * len(source_dataset_names)
        self._source_datasets = zip(source_dataset_names, source_dataset_modes, source_dataset_file_types)
        self._force_overwrite = force_overwrite
        self._logging_level = logging_level
        self._pipeline_options = pipeline_options if pipeline_options else self.default_pipeline_options

    @property
    @abstractmethod
    def dataset_output_dir_name(self) -> str:
        pass

    @abstractmethod
    def _run_source_dataset_pipeline(self,
                                     pipeline: beam.Pipeline,
                                     source_dataset: Tuple[str, str, str],
                                     dataset_input_path: Path,
                                     dataset_output_path: Path):
        pass

    @abstractmethod
    def _log_source_dataset_pipeline_metrics(self,
                                             results,
                                             source_dataset: Tuple[str, str, str],
                                             dataset_input_path: Path,
                                             dataset_output_path: Path):
        pass

    @property
    def default_pipeline_options(self):
        return {
            "runner": "DirectRunner",
            "direct_running_mode": "in_memory",
            "direct_num_workers": 1,
            "direct_runner_bundle_repeat": 0
        }

    def output_dataset_exists(self, dataset_output_path: Path) -> bool:
        match_results = FileSystems.match([str(dataset_output_path)])
        existing_paths = [metadata.path for metadata in match_results[0].metadata_list]
        if existing_paths:
            if not self._force_overwrite:
                logging.info(f'Skipping pipeline {self.__class__.__name__} for source dataset {dataset_output_path}.'
                             f'Found existing files: {existing_paths}.'
                             f'To overwrite them execute pipeline with --force-overwrite flag.')
                return True
            else:
                logging.info(f'Running pipeline in overwrite mode. Deleting exising dataset {existing_paths}...')
                FileSystems.delete(existing_paths)
        return False

    def run_pipeline(self):
        logging.getLogger().setLevel(self._logging_level)
        pipeline_options = PipelineOptions.from_dictionary(self._pipeline_options)
        for source_dataset in self._source_datasets:
            dataset_name, dataset_mode, dataset_file_type = source_dataset
            dataset_root_dir_name = get_dataset_root_dir_name(dataset_name, dataset_mode)
            dataset_input_path = Path(self._input_path) / dataset_root_dir_name / self._input_path_prefix
            dataset_output_path = (Path(self._output_path) / self.dataset_output_dir_name / dataset_root_dir_name /
                                   self._output_path_prefix)
            if self.output_dataset_exists(dataset_output_path):
                continue
            logging.info(f"Running pipeline {self.__class__.__name__} for source dataset {source_dataset}...")
            pipeline = beam.Pipeline(options=pipeline_options)
            self._run_source_dataset_pipeline(pipeline, source_dataset, dataset_input_path, dataset_output_path)
            results = pipeline.run()
            results.wait_until_finish()
            self._log_source_dataset_pipeline_metrics(results, source_dataset, dataset_input_path, dataset_output_path)
