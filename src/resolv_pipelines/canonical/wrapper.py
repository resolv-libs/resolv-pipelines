""" This module provides functions to use for converting source data to a canonical format and vice
versa. It is meant to be used as the only enry point in the canonicalization process. """
import typing
from typing import Any, Type, List, Dict

from .adapters.mir.symbolic_music.midi import MIDIFileAdapter
from .adapters.mir.symbolic_music.musicxml import MusicXMLDocumentAdapter
from .exceptions import CanonicalizationError
from .base import CanonicalFormat, DataAdapter

# Mapping of source file type extensions to their corresponding data adapters.
SOURCE_TYPE_DATA_ADAPTER_MAP: Dict[Type[DataAdapter], List[str]] = {
    MIDIFileAdapter: ['.mid', '.midi'],
    MusicXMLDocumentAdapter: ['.mxml', '.mxl']
}


def get_canonical_format_by_source_type(source_type: str) -> CanonicalFormat:
    """ Get the canonical format associated to the source type.

    Args:
        source_type (str): The type of the source file.

    Returns:
        (CanonicalFormat): The canonical format to the source type.

    Raises:
        ValueError: If the source type is unsupported.
    """
    data_adapter = get_data_adapter_by_source_type(source_type)
    return typing.get_args(data_adapter.__orig_bases__[0])


def get_data_adapter_by_source_type(source_type: str) -> DataAdapter:
    """ Get the appropriate data adapter based on the source type.

    Args:
        source_type (str): The type of the source file.

    Returns:
        (DataAdapter): The data adapter corresponding to the source type.

    Raises:
        ValueError: If the source type is unsupported.
    """
    for data_adapter, extensions_list in SOURCE_TYPE_DATA_ADAPTER_MAP.items():
        if source_type in extensions_list:
            return data_adapter()
    raise ValueError(f'Unsupported file type: {source_type}')


def to_canonical_format(source_type: str, content: bytes, metadata: Any) -> CanonicalFormat:
    """ Convert the input source file to a canonical format.

    Args:
        source_type (str): The type of the source file.
        content (bytes): The content of the source file.
        metadata (Any): Metadata associated with the source file.

    Returns:
        (CanonicalFormat): The canonical representation of the input source file.

    Raises:
        CanonicalizationError: If an error occurs during canonicalization.
    """
    try:
        data_adapter = get_data_adapter_by_source_type(source_type)
        return data_adapter.to_canonical_message(source_type, content, metadata)
    except Exception as e:
        raise CanonicalizationError(e)


def to_source_format(source_type: str, canonical_message: CanonicalFormat, **kwargs) -> bytes:
    """ Convert a canonical format to the specified source format.

    Args:
        source_type (str): The type of the source file.
        canonical_message (CanonicalFormat): The canonical representation of the source file.

    Returns:
        (bytes): The content of the source file in the specified format.
    """
    data_adapter = get_data_adapter_by_source_type(source_type)
    return data_adapter.to_source_format(canonical_message, **kwargs)
