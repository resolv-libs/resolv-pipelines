""" This module provides Beam DoFns for computing various attributes from NoteSequence objects.

Classes:
    AttributeDoFn: Abstract base class for computing attributes from NoteSequence objects.
    ToussaintAttributeDoFn: Computes Toussaint attribute from NoteSequence objects.
    NoteDensityAttributeDoFn: Computes note density attribute from NoteSequence objects.
    PitchRangeAttributeDoFn: Computes pitch range attribute from NoteSequence objects.
    ContourAttributeDoFn: Computes contour attribute from NoteSequence objects.
    UniqueNotesAttributeDoFn: Computes ratio of unique notes attribute from NoteSequence objects.
    UniqueBigramsAttributeDoFn: Computes ratio of unique bigrams attribute from NoteSequence objects.
    UniqueTrigramsAttributeDoFn: Computes ratio of unique trigrams attribute from NoteSequence objects.
    DynamicRangeAttributeDoFn: Computes dynamic range attribute from NoteSequence objects.
    NoteChangeAttributeDoFn: Computes ratio of note change attribute from NoteSequence objects.
    RatioNoteOffAttributeDoFn: Computes ratio of note off steps attribute from NoteSequence objects.
    RatioHoldNoteAttributeDoFn: Computes ratio of hold note steps attribute from NoteSequence objects.
    RepetitiveSectionDoFn: Computes ratio of repetitive sections attribute from NoteSequence objects.
    LongestRepetitiveSectionDoFn: Computes length of longest repetitive section attribute from NoteSequence objects.

Usage:
    - Subclass AttributeDoFn to define custom attribute computation logic.
    - Implement the proto_message_id() method to define the name of the attribute in NoteSequence 'attributes' field.
    - Implement the _process_internal() method to compute the attribute for a given NoteSequence.
    - Implement the name() and default_config() methods to provide metadata for the attribute.
    - Optionally, override the namespace() and _init_statistics() methods to initialize statistics tracking.
    - Register the custom AttributeDoFn subclass in the attribute_DO_FN_MAP dictionary for dynamic dispatch.

Example:
    class CustomAttributeDoFn(AttributeDoFn):
        def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
            super().__init__(config, debug_config)

        @staticmethod
        def name() -> str:
            return "CustomAttribute"

        @staticmethod
        def default_config() -> Dict[str, Any]:
            return {
                "custom_parameter": "default_value"
            }

        @staticmethod
        def proto_message_id() -> str:
            return "custom_attribute"

        def _process_internal(self, note_sequence: NoteSequence) -> float:
            # Custom attribute computation logic
            return 0.0

    See also '/beam/pipelines/compute_attributes_pipeline.py'.
"""
import re
from abc import ABC, abstractmethod
from numbers import Number
from typing import Dict, Any

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from google.protobuf.json_format import MessageToJson

from resolv_mir import NoteSequence
from resolv_mir.note_sequence import constants, attributes
from resolv_pipelines.pipelines.dofn.base import ConfigurableDoFn, DoFnDebugConfig


@beam.typehints.with_input_types(NoteSequence)
@beam.typehints.with_output_types(NoteSequence)
class AttributeDoFn(ConfigurableDoFn, ABC):
    """
    Abstract base class for computing attributes from NoteSequence objects. attribute specific computation logic must be
    implemented in the '_process_internal' method: return value from this method is rounded to a configurable number
    of digits (default to 5) and saved in the NoteSequence.SequenceAttributes proto object in the attribute specified by
    the method proto_message_id().

    Inherits:
        ConfigurableDoFn: Base class for configurable Apache Beam DoFns.

    Methods:
        proto_message_id() -> str: Abstract method to get name of the attribute in NoteSequence 'attributes' field.
        _process_internal(note_sequence: NoteSequence) -> float: Abstract method to compute the attribute.
        process(element: NoteSequence, *args, **kwargs): Process method for the attribute computation.
        _write_debug(note_sequence: NoteSequence): Write debug information for the attribute computation.
    """

    def __init__(self,
                 config: Dict[str, Any] = None,
                 debug_config: DoFnDebugConfig = None,
                 name: str = "",
                 namespace: str = ""):
        super(AttributeDoFn, self).__init__(config, debug_config, name, namespace)

    @staticmethod
    @abstractmethod
    def proto_message_id() -> str:
        """
        Abstract method to get name of the attribute in NoteSequence 'attributes' field.

        Returns:
            str: Name of the attribute.
        """
        pass

    @abstractmethod
    def _process_internal(self, note_sequence: NoteSequence) -> Number:
        """
        Abstract method to compute the attribute.

        Args:
            note_sequence (NoteSequence): Input NoteSequence for computing the attribute.

        Returns:
            Number: Computed attribute value.
        """
        pass

    @staticmethod
    def default_namespace() -> str:
        return ""

    def process(self, element: NoteSequence, *args, **kwargs):
        """
        Process method for the attribute computation: it rounds the value returned by '_process_internal(element)' and
        adds it to the NoteSequence proto by setting the attribute specified by 'proto_message_id()'. This allows for
        dynamic implementation of subclasses. Writes debug info if necessary.

        Args:
            element (NoteSequence): Input NoteSequence to compute the attribute.

        Yields:
            element: The same input NoteSequence but with the attribute value
        """
        attribute = self._process_internal(element)
        try:
            round_digits = self._config['round_digits']
        except KeyError:
            round_digits = 5
        rounded_attribute = round(attribute, round_digits)
        setattr(element.attributes, self.proto_message_id(), rounded_attribute)
        self._write_debug(element)
        yield element

    def _init_statistics(self) -> Dict[str, Any]:
        return None

    def _write_debug(self, note_sequence: NoteSequence):
        """
        If debug is enabled, it serializes the NoteSequence proto and writes it to the path specified by 'output_path'
        in the provided debug configuration.

        Args:
            note_sequence (NoteSequence): the NoteSequence proto to write debug info for.
        """
        if self._debug_config and self._debug_config.enabled:
            self._processed_elements += 1
            file_name_pattern = re.compile(self._debug_config.output_file_pattern)
            if file_name_pattern.match(note_sequence.id):
                key = (f"{self._debug_config.output_path}/debug/attributes/{self.name()}/{note_sequence.id}_"
                       f"{self._worker_id}_{self._processed_elements}.json")
                data = MessageToJson(note_sequence)
                with FileSystems.create(key, 'application/json') as writer:
                    writer.write(data.encode())


class ToussaintAttributeDoFn(AttributeDoFn):
    """Computes Toussaint attribute from NoteSequence objects."""

    def __init__(self,
                 config: Dict[str, Any] = None,
                 debug_config: DoFnDebugConfig = None,
                 name: str = "",
                 namespace: str = ""):
        super(ToussaintAttributeDoFn, self).__init__(config, debug_config, name, namespace)

    @staticmethod
    def default_name() -> str:
        return "Toussaint"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            "bars": 2,
            "binary": True
        }

    @staticmethod
    def proto_message_id() -> str:
        return "toussaint"

    def _process_internal(self, note_sequence: NoteSequence) -> float:
        return attributes.rhythmic.toussaint(note_sequence, self._config['bars'], self._config['binary'])


class NoteDensityAttributeDoFn(AttributeDoFn):
    """Computes note density attribute from NoteSequence objects."""

    def __init__(self,
                 config: Dict[str, Any] = None,
                 debug_config: DoFnDebugConfig = None,
                 name: str = "",
                 namespace: str = ""):
        super(NoteDensityAttributeDoFn, self).__init__(config, debug_config, name, namespace)

    @staticmethod
    def default_name() -> str:
        return "NoteDensity"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            "bars": 2,
            "binary": True
        }

    @staticmethod
    def proto_message_id() -> str:
        return "note_density"

    def _process_internal(self, note_sequence: NoteSequence) -> float:
        return attributes.rhythmic.note_density(note_sequence, self._config['bars'], self._config['binary'])


class PitchRangeAttributeDoFn(AttributeDoFn):
    """Computes pitch range attribute from NoteSequence objects."""

    def __init__(self,
                 config: Dict[str, Any] = None,
                 debug_config: DoFnDebugConfig = None,
                 name: str = "",
                 namespace: str = ""):
        super(PitchRangeAttributeDoFn, self).__init__(config, debug_config, name, namespace)

    @staticmethod
    def default_name() -> str:
        return "PitchRange"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            "num_midi_pitches": constants.NUM_PIANO_MIDI_PITCHES
        }

    @staticmethod
    def proto_message_id() -> str:
        return "pitch_range"

    def _process_internal(self, note_sequence: NoteSequence) -> float:
        return attributes.pitch.pitch_range(note_sequence, self._config['num_midi_pitches'])


class ContourAttributeDoFn(AttributeDoFn):
    """Computes contour attribute from NoteSequence objects."""

    def __init__(self,
                 config: Dict[str, Any] = None,
                 debug_config: DoFnDebugConfig = None,
                 name: str = "",
                 namespace: str = ""):
        super(ContourAttributeDoFn, self).__init__(config, debug_config, name, namespace)

    @staticmethod
    def default_name() -> str:
        return "Contour"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            "num_midi_pitches": constants.NUM_PIANO_MIDI_PITCHES
        }

    @staticmethod
    def proto_message_id() -> str:
        return "contour"

    def _process_internal(self, note_sequence: NoteSequence) -> float:
        return attributes.pitch.contour(note_sequence, self._config['num_midi_pitches'])


class UniqueNotesAttributeDoFn(AttributeDoFn):
    """Computes ratio of unique notes attribute from NoteSequence objects."""

    def __init__(self,
                 config: Dict[str, Any] = None,
                 debug_config: DoFnDebugConfig = None,
                 name: str = "",
                 namespace: str = ""):
        super(UniqueNotesAttributeDoFn, self).__init__(config, debug_config, name, namespace)

    @staticmethod
    def default_name() -> str:
        return "UniqueNotesRatio"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            "num_midi_pitches": constants.NUM_PIANO_MIDI_PITCHES
        }

    @staticmethod
    def proto_message_id() -> str:
        return "unique_notes_ratio"

    def _process_internal(self, note_sequence: NoteSequence) -> float:
        return attributes.pitch.ratio_unique_notes(note_sequence)


class UniqueBigramsAttributeDoFn(AttributeDoFn):
    """Computes ratio of unique bigrams attribute from NoteSequence objects."""

    def __init__(self,
                 config: Dict[str, Any] = None,
                 debug_config: DoFnDebugConfig = None,
                 name: str = "",
                 namespace: str = ""):
        super(UniqueBigramsAttributeDoFn, self).__init__(config, debug_config, name, namespace)

    @staticmethod
    def default_name() -> str:
        return "UniqueBigramsRatio"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            "num_midi_pitches": constants.NUM_PIANO_MIDI_PITCHES
        }

    @staticmethod
    def proto_message_id() -> str:
        return "unique_bigrams_ratio"

    def _process_internal(self, note_sequence: NoteSequence) -> float:
        return attributes.pitch.ratio_unique_ngrams(note_sequence, 2)


class UniqueTrigramsAttributeDoFn(AttributeDoFn):
    """Computes ratio of unique trigrams attribute from NoteSequence objects."""

    def __init__(self,
                 config: Dict[str, Any] = None,
                 debug_config: DoFnDebugConfig = None,
                 name: str = "",
                 namespace: str = ""):
        super(UniqueTrigramsAttributeDoFn, self).__init__(config, debug_config, name, namespace)

    @staticmethod
    def default_name() -> str:
        return "UniqueTrigramsRatio"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            "num_midi_pitches": constants.NUM_PIANO_MIDI_PITCHES
        }

    @staticmethod
    def proto_message_id() -> str:
        return "unique_trigrams_ratio"

    def _process_internal(self, note_sequence: NoteSequence) -> float:
        return attributes.pitch.ratio_unique_ngrams(note_sequence, 3)


class DynamicRangeAttributeDoFn(AttributeDoFn):
    """Computes dynamic range attribute from NoteSequence objects."""

    def __init__(self,
                 config: Dict[str, Any] = None,
                 debug_config: DoFnDebugConfig = None,
                 name: str = "",
                 namespace: str = ""):
        super(DynamicRangeAttributeDoFn, self).__init__(config, debug_config, name, namespace)

    @staticmethod
    def default_name() -> str:
        return "DynamicRange"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {}

    @staticmethod
    def proto_message_id() -> str:
        return "dynamic_range"

    def _process_internal(self, note_sequence: NoteSequence) -> float:
        return attributes.dynamics.dynamic_range(note_sequence)


class NoteChangeAttributeDoFn(AttributeDoFn):
    """Computes ratio of note change attribute from NoteSequence objects."""

    def __init__(self,
                 config: Dict[str, Any] = None,
                 debug_config: DoFnDebugConfig = None,
                 name: str = "",
                 namespace: str = ""):
        super(NoteChangeAttributeDoFn, self).__init__(config, debug_config, name, namespace)

    @staticmethod
    def default_name() -> str:
        return "NoteChangeRatio"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {}

    @staticmethod
    def proto_message_id() -> str:
        return "note_change_ratio"

    def _process_internal(self, note_sequence: NoteSequence) -> float:
        return attributes.dynamics.ratio_note_change(note_sequence)


class RatioNoteOffAttributeDoFn(AttributeDoFn):
    """Computes ratio of note off steps attribute from NoteSequence objects."""

    def __init__(self,
                 config: Dict[str, Any] = None,
                 debug_config: DoFnDebugConfig = None,
                 name: str = "",
                 namespace: str = ""):
        super(RatioNoteOffAttributeDoFn, self).__init__(config, debug_config, name, namespace)

    @staticmethod
    def default_name() -> str:
        return "NoteOffStepsRatio"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {}

    @staticmethod
    def proto_message_id() -> str:
        return "ratio_note_off_steps"

    def _process_internal(self, note_sequence: NoteSequence) -> float:
        return attributes.dynamics.ratio_note_off_steps(note_sequence)


class RatioHoldNoteAttributeDoFn(AttributeDoFn):
    """Computes ratio of hold note steps attribute from NoteSequence objects."""

    def __init__(self,
                 config: Dict[str, Any] = None,
                 debug_config: DoFnDebugConfig = None,
                 name: str = "",
                 namespace: str = ""):
        super(RatioHoldNoteAttributeDoFn, self).__init__(config, debug_config, name, namespace)

    @staticmethod
    def default_name() -> str:
        return "HoldNoteStepsRatio"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {}

    @staticmethod
    def proto_message_id() -> str:
        return "ratio_hold_note_steps"

    def _process_internal(self, note_sequence: NoteSequence) -> float:
        return attributes.dynamics.ratio_hold_note_steps(note_sequence)


class RepetitiveSectionDoFn(AttributeDoFn):
    """Computes ratio of repetitive sections attribute from NoteSequence objects."""

    def __init__(self,
                 config: Dict[str, Any] = None,
                 debug_config: DoFnDebugConfig = None,
                 name: str = "",
                 namespace: str = ""):
        super(RepetitiveSectionDoFn, self).__init__(config, debug_config, name, namespace)

    @staticmethod
    def default_name() -> str:
        return "RepetitiveSectionRatio"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            "min_repetitions": 4
        }

    @staticmethod
    def proto_message_id() -> str:
        return "repetitive_section_ratio"

    def _process_internal(self, note_sequence: NoteSequence) -> float:
        return attributes.dynamics.ratio_repetitive_sections(note_sequence, self._config['min_repetitions'])


class LongestRepetitiveSectionDoFn(AttributeDoFn):
    """Computes length of longest repetitive section attribute from NoteSequence objects."""

    def __init__(self,
                 config: Dict[str, Any] = None,
                 debug_config: DoFnDebugConfig = None,
                 name: str = "",
                 namespace: str = ""):
        super(LongestRepetitiveSectionDoFn, self).__init__(config, debug_config, name, namespace)

    @staticmethod
    def default_name() -> str:
        return "LongestRepetitiveSection"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            "min_repetitions": 4
        }

    @staticmethod
    def proto_message_id() -> str:
        return "len_longest_rep_section"

    def _process_internal(self, note_sequence: NoteSequence) -> float:
        return attributes.dynamics.length_longest_repetitive_section(note_sequence, self._config['min_repetitions'])


# Dictionary mapping attribute names to their respective computation classes
NS_ATTRIBUTE_DO_FN_MAP = {
    'toussaint': ToussaintAttributeDoFn,
    'note_density': NoteDensityAttributeDoFn,
    'pitch_range': PitchRangeAttributeDoFn,
    'contour': ContourAttributeDoFn,
    'unique_notes_ratio': UniqueNotesAttributeDoFn,
    'unique_bigrams_ratio': UniqueBigramsAttributeDoFn,
    'unique_trigrams_ratio': UniqueTrigramsAttributeDoFn,
    'dynamic_range': DynamicRangeAttributeDoFn,
    'note_change_ratio': NoteChangeAttributeDoFn,
    'ratio_note_off_steps': RatioNoteOffAttributeDoFn,
    'ratio_hold_note_steps': RatioHoldNoteAttributeDoFn,
    'repetitive_section_ratio': RepetitiveSectionDoFn,
    'longest_repetitive_section': LongestRepetitiveSectionDoFn
}
