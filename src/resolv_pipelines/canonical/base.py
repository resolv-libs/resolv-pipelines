"""This module provides abstract base classes and types for data adapters used in canonicalization."""
from abc import ABC, abstractmethod
from typing import Any, TypeVar, Union, Generic

from resolv_mir import NoteSequence


# Update this generic type when adding a new CanonicalFormat
CanonicalFormat = TypeVar('CanonicalFormat', bound=Union[NoteSequence])


class DataAdapter(ABC, Generic[CanonicalFormat]):
    """ Abstract base class for data adapters used in converting data to and from canonical formats.

    Attributes:
        - CanonicalFormat: A generic type representing the canonical format.
    """

    @abstractmethod
    def to_canonical_message(self, source_type: str, content: Any, metadata: Any) -> CanonicalFormat:
        """ Abstract method to convert the input source file to a canonical format.

        Args:
            source_type (str): The type of the source file.
            content (bytes): The content of the source file.
            metadata (Any): Metadata associated with the source file.

        Returns:
            (CanonicalFormat): The canonical representation of the input source file.
        """
        pass

    @abstractmethod
    def to_source_format(self, canonical_message: CanonicalFormat, **kwargs) -> bytes:
        """ Abstract method to convert a canonical format to the specified source format.

        Args:
            canonical_message (CanonicalFormat): The canonical representation of the source file.

        Returns:
            (bytes): The content of the source file in the specified format.
        """
        pass
