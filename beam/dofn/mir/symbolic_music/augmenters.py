""" TODO - module doc """
from abc import ABC, abstractmethod
from typing import List, Dict, Any

import apache_beam as beam
import numpy as np
from apache_beam.metrics import Metrics as beam_metrics
from resolv_mir.protobuf import NoteSequence
from resolv_mir.note_sequence import constants, processors

from beam.dofn.base import ConfigurableDoFn, DoFnDebugConfig


@beam.typehints.with_input_types(NoteSequence)
@beam.typehints.with_output_types(NoteSequence)
class AugmentNoteSequenceDoFn(ConfigurableDoFn, ABC):

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(AugmentNoteSequenceDoFn, self).__init__(config, debug_config)

    @abstractmethod
    def _augment_sequence(self, note_sequence: NoteSequence) -> List[NoteSequence]:
        """
        Internal method for augmenting a single NoteSequence object.

        Args:
            note_sequence (NoteSequence): The NoteSequence object to be processed.

        Returns:
            List[NoteSequence]: A list containing the augmented NoteSequence objects.
        """
        pass

    @abstractmethod
    def _init_statistics_internal(self) -> Dict[str, Any]:
        """
        Internal method for initializing statistics specific to the derived DoFn class. The statistic will be merged to
        the ones returned by '_init_statistics()'.

        Returns:
            Dict[str, Any]: A dictionary containing the initialized statistics.
        """
        pass

    def _init_statistics(self) -> Dict[str, Any]:
        """
        Initializes statistics for the DoFn.

        Returns:
            Dict[str, Any]: A dictionary containing the initialized statistics.
        """
        default_metrics = {
            'input_sequences': beam_metrics.counter(self.namespace(), 'input_sequences'),
            'output_sequences': beam_metrics.counter(self.namespace(), 'output_sequences'),
            'augmented_sequences': beam_metrics.counter(self.namespace(), 'augmented_sequences'),
        }
        internal_metrics = self._init_statistics_internal()
        if internal_metrics:
            default_metrics.update(internal_metrics)
        return default_metrics

    def process(self, note_sequence: NoteSequence, *args, **kwargs):
        """
        Augment a single NoteSequence object, updates statistics and writes debug info if necessary.

        Args:
            note_sequence (NoteSequence): The NoteSequence object to be augmented.

        Yields:
            augmented_sequences: The list of augmented NoteSequence objects.
        """
        self.statistics['input_sequences'].inc()
        if np.random.rand() > self._config['threshold']:
            augmented_sequences = self._augment_sequence(note_sequence)
            self.statistics['augmented_sequences'].inc()
            for augmented_sequence in augmented_sequences:
                self.statistics['output_sequences'].inc()
                yield augmented_sequence
        else:
            self.statistics['output_sequences'].inc()
            yield note_sequence


class TransposeAugmenterDoFn(AugmentNoteSequenceDoFn):
    """ A DoFn for transposing the notes of a NoteSequence proto message.
        Check out the 'transpose_note_sequence' method in the 'transposer' processor module in 'note_sequence' lib
        for more information.
    """

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(TransposeAugmenterDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
        return "TransposeAugmenter"

    @staticmethod
    def namespace() -> str:
        return "transpose_augmenter"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            'threshold': 0.5,
            'min_transpose_amount': -constants.NOTES_PER_OCTAVE,
            'max_transpose_amount': constants.NOTES_PER_OCTAVE,
            'min_allowed_pitch': constants.PIANO_MIN_MIDI_PITCH,
            'max_allowed_pitch': constants.PIANO_MAX_MIDI_PITCH,
            'transpose_chords': True,
            'delete_notes': True,
            'in_place': True
        }

    def _init_statistics_internal(self):
        statistics = dict((stat_name, beam_metrics.counter(self.namespace(), stat_name)) for stat_name in
                          ['skipped_due_to_range_exceeded'])
        statistics['deleted_notes'] = beam_metrics.distribution(self.namespace(), 'deleted_notes')
        return statistics

    def _augment_sequence(self, note_sequence: NoteSequence) -> List[NoteSequence]:
        def clamp_transpose_amount(transpose_amount: int, ns_min_pitch: int, ns_max_pitch: int):
            if transpose_amount < 0:
                return -min(ns_min_pitch - self._config['min_allowed_pitch'], abs(transpose_amount))
            else:
                return min(self._config['max_allowed_pitch'] - ns_max_pitch, transpose_amount)

        def get_transpose_amount():
            min_transpose = self._config['min_transpose_amount']
            max_transpose = self._config['max_transpose_amount']
            if not self._config['delete_notes']:
                # Prevent transposition from taking a note outside the allowed note bounds by clamping the range we
                # sample from.
                ns_min_pitch = min(note_sequence.notes, key=lambda note: note.pitch).pitch
                ns_max_pitch = max(note_sequence.notes, key=lambda note: note.pitch).pitch
                min_transpose = clamp_transpose_amount(min_transpose, ns_min_pitch, ns_max_pitch)
                max_transpose = clamp_transpose_amount(max_transpose, ns_min_pitch, ns_max_pitch)
            return np.random.randint(min_transpose, max_transpose)

        transposed_ns, del_notes = (
            processors.transposer.transpose_note_sequence(note_sequence,
                                                          amount=get_transpose_amount(),
                                                          min_allowed_pitch=self._config['min_allowed_pitch'],
                                                          max_allowed_pitch=self._config['max_allowed_pitch'],
                                                          transpose_chords=self._config['transpose_chords'],
                                                          in_place=self._config['in_place'],
                                                          delete_notes=self._config['delete_notes']))
        self.statistics['deleted_notes'].update(del_notes)
        if not transposed_ns.notes:
            self.statistics['skipped_due_to_range_exceeded'].inc()
            return []
        return [transposed_ns] if self._config['in_place'] else [note_sequence, transposed_ns]


# Dictionary mapping augmentation processor names to their respective classes
NS_AUG_DO_FN_MAP = {
    'transposer_ps_do_fn': TransposeAugmenterDoFn
}