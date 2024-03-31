""" TODO - module doc """
from pathlib import Path
from typing import Dict, List, Union, Tuple

import apache_beam as beam
from resolv_data import import_directory_dataset

from .base import DatasetPipeline
from ...dofn import utilities


class ImportArchiveDatasetPipeline(DatasetPipeline):

    def __init__(self,
                 output_path: Union[str, Path],
                 source_dataset_names: List[str],
                 source_dataset_modes: List[str],
                 output_path_prefix: str = "",
                 force_overwrite: bool = False,
                 allow_invalid_checksum: bool = False,
                 logging_level: str = 'INFO',
                 pipeline_options: Dict[str, str] = None):
        super(ImportArchiveDatasetPipeline, self).__init__(
            input_path=output_path,
            output_path=output_path,
            output_path_prefix=output_path_prefix,
            source_dataset_names=source_dataset_names,
            source_dataset_modes=source_dataset_modes,
            source_dataset_file_types=None,
            force_overwrite=force_overwrite,
            logging_level=logging_level,
            pipeline_options=pipeline_options
        )
        self._allow_invalid_checksum = allow_invalid_checksum

    @property
    def dataset_output_dir_name(self) -> str:
        return "raw"

    def _run_source_dataset_pipeline(self,
                                     pipeline: beam.Pipeline,
                                     source_dataset: Tuple[str, str, str],
                                     dataset_input_path: Path,
                                     dataset_output_path: Path):
        dataset_name, dataset_mode, dataset_file_type = source_dataset
        dataset_path, _ = import_directory_dataset(
            dataset_name=dataset_name,
            dataset_mode=dataset_mode,
            index_path_prefix=dataset_output_path,
            temp=True,
            cleanup=True,
            allow_invalid_checksum=self._allow_invalid_checksum
        )
        _ = (
                pipeline
                | "CreatePColl" >> beam.Create([dataset_path])
                | "WriteOutput" >> beam.ParDo(
                            utilities.WriteDirectoryToFileSystem(output_path=dataset_output_path,
                                                                 force_overwrite=self._force_overwrite,
                                                                 cleanup=True)
                )
        )

    def _log_source_dataset_pipeline_metrics(self,
                                             results,
                                             source_dataset: Tuple[str, str, str],
                                             dataset_input_path: Path,
                                             dataset_output_path: Path):
        pass
