""" This module defines Apache Beam DoFns for reading dataset entry files and converting them to canonical format.

Classes:
    ReadDatasetEntryFileDoFn: Apache Beam DoFn for reading dataset entry files.
    ToCanonicalFormatDoFn: Apache Beam DoFn for converting data to canonical format.

Usage:
    - Use ReadDatasetEntryFileDoFn to read dataset entry files and yield their content along with metadata.
    - Use ToCanonicalFormatDoFn to convert data to canonical format and handle any conversion errors.

Example:
    See '/beam/pipelines/canonicalize_dataset_pipeline.py'.
"""
import logging
import re
from pathlib import Path
from typing import Dict, Any, Tuple

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.metrics import Metrics as beam_metrics
from google.protobuf.json_format import MessageToJson, MessageToDict

from resolv_data import DatasetEntry
from resolv_data.canonical import CanonicalFormat, to_canonical_format
from resolv_data.canonical.exceptions import CanonicalizationError
from beam.dofn.base import ConfigurableDoFn, DoFnDebugConfig


@beam.typehints.with_input_types(DatasetEntry)
@beam.typehints.with_output_types(Tuple)
class ReadDatasetEntryFileDoFn(beam.DoFn):
    """
    Apache Beam DoFn for extracting file content along with metadata from a DatasetEntry proto message.

    Args:
        file_type (str): Type of the file to read from the DatasetEntry message. Only entries with 'key=file_type' in
            'entry.files' will be considered.
        dataset_name (str): Name of the dataset from which the entry is taken.

    Methods:
        process(element, *args, **kwargs): Reads the file and yields its content along with metadata.
    """

    def __init__(self, file_type: str, dataset_name: str):
        super(ReadDatasetEntryFileDoFn, self).__init__()
        self._file_type = file_type
        self._dataset_name = dataset_name

    def process(self, element, *args, **kwargs):
        """
        Reads the file of type 'self.file_type' associated with the DatasetEntry and yields its content along with the
        original file type and additional metadata that depend on the entry.

        Args:
            element (DatasetEntry): DatasetEntry proto message to process.

        Yields:
            Tuple: Tuple containing file content, original file type, and metadata.
        """
        metadata_type = element.WhichOneof('metadata')
        dict_metadata = {}
        if metadata_type:
            metadata = element.__getattribute__(metadata_type)
            dict_metadata = MessageToDict(metadata, metadata_type)
        file = element.files[self._file_type]
        path = file.path
        dict_metadata.update({'id': Path(element.id).name, 'filepath': path, 'collection_name': self._dataset_name})
        with FileSystems.open(path) as f:
            source_type = Path(path).suffix
            yield f.read(), source_type, dict_metadata


@beam.typehints.with_input_types(Tuple)
@beam.typehints.with_output_types(CanonicalFormat)
class ToCanonicalFormatDoFn(ConfigurableDoFn):
    """
    Apache Beam DoFn for converting data to a CanonicalFormat. The output CanonicalFormat is implicitly chosen by the
    input file type. For example, MIDI files are converted to the NoteSequence format. The association is defined in the
    'canonical' module.

    Inherits:
        ConfigurableDoFn: Base class for configurable Apache Beam DoFns.

    Methods:
        process(element, *args, **kwargs): Converts data to a CanonicalFormat and yields conversion results.
    """

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(ToCanonicalFormatDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
        return "Canonicalizer"

    @staticmethod
    def namespace() -> str:
        return "stats"

    def default_config(self) -> Dict[str, Any]:
        return {}

    def _init_statistics(self) -> Dict[str, Any]:
        return {
            'conversion_errors': beam_metrics.counter(self.namespace(), 'conversion_errors')
        }

    def process(self, element, *args, **kwargs):
        """
        Converts data to CanonicalFormat and yields conversion results. Writes debug output if necessary.
        Conversion errors are counted in the 'conversion_errors' statistic.

        Args:
            element (Tuple): Tuple containing content, file_type, and metadata.

        Yields:
            canonical_format: Canonical format of the converted data.
        """

        content, source_type, dict_metadata = element
        try:
            canonical_format = to_canonical_format(source_type, content, dict_metadata)
            self._write_debug(canonical_format, dict_metadata)
            yield canonical_format
        except CanonicalizationError as e:
            logging.error(f'Canonicalization error: {e}')
            self.statistics['conversion_errors'].inc()

    def _write_debug(self, canonical_format: CanonicalFormat, dict_metadata: Dict[str, Any]):
        """
        If debug is enabled, it serializes the canonical format and writes it to the path specified by 'output_path'
        in the provided debug configuration.

        Args:
            canonical_format (CanonicalFormat): the canonical format to write debug info for.
            dict_metadata (Dict[str, Any]): metadata about the input file
        """
        if self._debug_config and self._debug_config.enabled:
            file_name_pattern = re.compile(self._debug_config.output_file_pattern)
            if file_name_pattern.match(canonical_format.id):
                key = (f"{self._debug_config.output_path}/debug/{Path(dict_metadata['filepath']).stem}_"
                       f"{self._worker_id}.json")
                with FileSystems.create(key, 'application/json') as writer:
                    logging.info(f'Writing debug info to {key}...')
                    data = MessageToJson(canonical_format).encode()
                    writer.write(data)
