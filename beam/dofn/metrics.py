""" This module provides Beam DoFns for computing various metrics from NoteSequence objects.

Classes:
    MetricDoFn: Abstract base class for computing metrics from NoteSequence objects.
    ToussaintMetricDoFn: Computes Toussaint metric from NoteSequence objects.
    NoteDensityMetricDoFn: Computes note density metric from NoteSequence objects.
    PitchRangeMetricDoFn: Computes pitch range metric from NoteSequence objects.
    ContourMetricDoFn: Computes contour metric from NoteSequence objects.
    UniqueNotesMetricDoFn: Computes ratio of unique notes metric from NoteSequence objects.
    UniqueBigramsMetricDoFn: Computes ratio of unique bigrams metric from NoteSequence objects.
    UniqueTrigramsMetricDoFn: Computes ratio of unique trigrams metric from NoteSequence objects.
    DynamicRangeMetricDoFn: Computes dynamic range metric from NoteSequence objects.
    NoteChangeMetricDoFn: Computes ratio of note change metric from NoteSequence objects.
    RatioNoteOffMetricDoFn: Computes ratio of note off steps metric from NoteSequence objects.
    RatioHoldNoteMetricDoFn: Computes ratio of hold note steps metric from NoteSequence objects.
    RepetitiveSectionDoFn: Computes ratio of repetitive sections metric from NoteSequence objects.
    LongestRepetitiveSectionDoFn: Computes length of longest repetitive section metric from NoteSequence objects.

Usage:
    - Subclass MetricDoFn to define custom metric computation logic.
    - Implement the proto_message_id() method to define the name of the attribute in NoteSequence 'metrics' field.
    - Implement the _process_internal() method to compute the metric for a given NoteSequence.
    - Implement the name() and default_config() methods to provide metadata for the metric.
    - Optionally, override the namespace() and _init_statistics() methods to initialize statistics tracking.
    - Register the custom MetricDoFn subclass in the METRIC_DO_FN_MAP dictionary for dynamic dispatch.

Example:
    class CustomMetricDoFn(MetricDoFn):
        def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
            super().__init__(config, debug_config)

        @staticmethod
        def name() -> str:
            return "CustomMetric"

        @staticmethod
        def default_config() -> Dict[str, Any]:
            return {
                "custom_parameter": "default_value"
            }

        @staticmethod
        def proto_message_id() -> str:
            return "custom_metric"

        def _process_internal(self, note_sequence: NoteSequence) -> float:
            # Custom metric computation logic
            return 0.0

    See also '/beam/pipelines/compute_metrics_pipeline.py'.
"""
import re
from abc import ABC, abstractmethod
from numbers import Number
from typing import Dict, Any

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from google.protobuf.json_format import MessageToJson

from resolv_mir import NoteSequence
from resolv_mir.note_sequence import constants, metrics
from beam.dofn.base import ConfigurableDoFn, DoFnDebugConfig


@beam.typehints.with_input_types(NoteSequence)
@beam.typehints.with_output_types(NoteSequence)
class MetricDoFn(ConfigurableDoFn, ABC):
    """
    Abstract base class for computing metrics from NoteSequence objects. Metric specific computation logic must be
    implemented in the '_process_internal' method: return value from this method is rounded to a configurable number
    of digits (default to 5) and saved in the NoteSequence.SequenceMetrics proto object in the attribute specified by
    the method proto_message_id().

    Inherits:
        ConfigurableDoFn: Base class for configurable Apache Beam DoFns.

    Methods:
        proto_message_id() -> str: Abstract method to get name of the attribute in NoteSequence 'metrics' field.
        _process_internal(note_sequence: NoteSequence) -> float: Abstract method to compute the metric.
        process(element: NoteSequence, *args, **kwargs): Process method for the metric computation.
        _write_debug(note_sequence: NoteSequence): Write debug information for the metric computation.
    """

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(MetricDoFn, self).__init__(config, debug_config)

    @staticmethod
    @abstractmethod
    def proto_message_id() -> str:
        """
        Abstract method to get name of the attribute in NoteSequence 'metrics' field.

        Returns:
            str: Name of the attribute.
        """
        pass

    @abstractmethod
    def _process_internal(self, note_sequence: NoteSequence) -> Number:
        """
        Abstract method to compute the metric.

        Args:
            note_sequence (NoteSequence): Input NoteSequence for computing the metric.

        Returns:
            Number: Computed metric value.
        """
        pass

    @staticmethod
    def namespace() -> str:
        return ""

    def process(self, element: NoteSequence, *args, **kwargs):
        """
        Process method for the metric computation: it rounds the value returned by '_process_internal(element)' and
        adds it to the NoteSequence proto by setting the attribute specified by 'proto_message_id()'. This allows for
        dynamic implementation of subclasses. Writes debug info if necessary.

        Args:
            element (NoteSequence): Input NoteSequence to compute the metric.

        Yields:
            element: The same input NoteSequence but with the metric value
        """
        metric = self._process_internal(element)
        try:
            round_digits = self._config['round_digits']
        except KeyError:
            round_digits = 5
        rounded_metric = round(metric, round_digits)
        setattr(element.metrics, self.proto_message_id(), rounded_metric)
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
                key = (f"{self._debug_config.output_path}/debug/metrics/{self.name()}/{note_sequence.id}_"
                       f"{self._worker_id}_{self._processed_elements}.json")
                data = MessageToJson(note_sequence)
                with FileSystems.create(key, 'application/json') as writer:
                    writer.write(data.encode())


class ToussaintMetricDoFn(MetricDoFn):
    """Computes Toussaint metric from NoteSequence objects."""

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(ToussaintMetricDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
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
        return metrics.rhythmic.toussaint(note_sequence, self._config['bars'], self._config['binary'])


class NoteDensityMetricDoFn(MetricDoFn):
    """Computes note density metric from NoteSequence objects."""

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(NoteDensityMetricDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
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
        return metrics.rhythmic.note_density(note_sequence, self._config['bars'], self._config['binary'])


class PitchRangeMetricDoFn(MetricDoFn):
    """Computes pitch range metric from NoteSequence objects."""

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(PitchRangeMetricDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
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
        return metrics.pitch.pitch_range(note_sequence, self._config['num_midi_pitches'])


class ContourMetricDoFn(MetricDoFn):
    """Computes contour metric from NoteSequence objects."""

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(ContourMetricDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
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
        return metrics.pitch.contour(note_sequence, self._config['num_midi_pitches'])


class UniqueNotesMetricDoFn(MetricDoFn):
    """Computes ratio of unique notes metric from NoteSequence objects."""

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(UniqueNotesMetricDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
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
        return metrics.pitch.ratio_unique_notes(note_sequence)


class UniqueBigramsMetricDoFn(MetricDoFn):
    """Computes ratio of unique bigrams metric from NoteSequence objects."""

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(UniqueBigramsMetricDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
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
        return metrics.pitch.ratio_unique_ngrams(note_sequence, 2)


class UniqueTrigramsMetricDoFn(MetricDoFn):
    """Computes ratio of unique trigrams metric from NoteSequence objects."""

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(UniqueTrigramsMetricDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
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
        return metrics.pitch.ratio_unique_ngrams(note_sequence, 3)


class DynamicRangeMetricDoFn(MetricDoFn):
    """Computes dynamic range metric from NoteSequence objects."""

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(DynamicRangeMetricDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
        return "DynamicRange"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {}

    @staticmethod
    def proto_message_id() -> str:
        return "dynamic_range"

    def _process_internal(self, note_sequence: NoteSequence) -> float:
        return metrics.dynamics.dynamic_range(note_sequence)


class NoteChangeMetricDoFn(MetricDoFn):
    """Computes ratio of note change metric from NoteSequence objects."""

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(NoteChangeMetricDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
        return "NoteChangeRatio"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {}

    @staticmethod
    def proto_message_id() -> str:
        return "note_change_ratio"

    def _process_internal(self, note_sequence: NoteSequence) -> float:
        return metrics.dynamics.ratio_note_change(note_sequence)


class RatioNoteOffMetricDoFn(MetricDoFn):
    """Computes ratio of note off steps metric from NoteSequence objects."""

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(RatioNoteOffMetricDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
        return "NoteOffStepsRatio"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {}

    @staticmethod
    def proto_message_id() -> str:
        return "ratio_note_off_steps"

    def _process_internal(self, note_sequence: NoteSequence) -> float:
        return metrics.dynamics.ratio_note_off_steps(note_sequence)


class RatioHoldNoteMetricDoFn(MetricDoFn):
    """Computes ratio of hold note steps metric from NoteSequence objects."""

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(RatioHoldNoteMetricDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
        return "HoldNoteStepsRatio"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {}

    @staticmethod
    def proto_message_id() -> str:
        return "ratio_hold_note_steps"

    def _process_internal(self, note_sequence: NoteSequence) -> float:
        return metrics.dynamics.ratio_hold_note_steps(note_sequence)


class RepetitiveSectionDoFn(MetricDoFn):
    """Computes ratio of repetitive sections metric from NoteSequence objects."""

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(RepetitiveSectionDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
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
        return metrics.dynamics.ratio_repetitive_sections(note_sequence, self._config['min_repetitions'])


class LongestRepetitiveSectionDoFn(MetricDoFn):
    """Computes length of longest repetitive section metric from NoteSequence objects."""

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(LongestRepetitiveSectionDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
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
        return metrics.dynamics.length_longest_repetitive_section(note_sequence, self._config['min_repetitions'])


# Dictionary mapping metric names to their respective computation classes
METRIC_DO_FN_MAP = {
    'toussaint_ms_do_fn': ToussaintMetricDoFn,
    'note_density_ms_do_fn': NoteDensityMetricDoFn,
    'pitch_range_ms_do_fn': PitchRangeMetricDoFn,
    'contour_ms_do_fn': ContourMetricDoFn,
    'unique_notes_ratio_ms_do_fn': UniqueNotesMetricDoFn,
    'unique_bigrams_ratio_ms_do_fn': UniqueBigramsMetricDoFn,
    'unique_trigrams_ratio_ms_do_fn': UniqueTrigramsMetricDoFn,
    'dynamic_range_ms_do_fn': DynamicRangeMetricDoFn,
    'note_change_ratio_ms_do_fn': NoteChangeMetricDoFn,
    'ratio_note_off_steps_ms_do_fn': RatioNoteOffMetricDoFn,
    'ratio_hold_note_steps_ms_do_fn': RatioHoldNoteMetricDoFn,
    'repetitive_section_ratio_ms_do_fn': RepetitiveSectionDoFn,
    'longest_repetitive_section_ms_do_fn': LongestRepetitiveSectionDoFn
}
