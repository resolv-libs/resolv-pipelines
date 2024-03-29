""" This module provides a collection of Apache Beam DoFns for processing NoteSequence objects.

Classes:
    - NoteSequenceDoFn: An abstract base class for NoteSequence processing DoFns.
    - ExtractMelodyDoFn: A DoFn for extracting melody from NoteSequence objects.
    - QuantizeDoFn: A DoFn for quantizing NoteSequence objects.
    - SplitDoFn: A DoFn for splitting NoteSequence objects.
    - TimeChangeSplitDoFn: A DoFn for splitting NoteSequence objects based on time changes.
    - SilenceSplitDoFn: A DoFn for splitting NoteSequence objects based on silence.
    - SliceDoFn: A DoFn for slicing NoteSequence objects.
    - StretchDoFn: A DoFn for stretching NoteSequence objects.
    - SustainDoFn: A DoFn for applying sustain control changes to NoteSequence objects.
    - TransposeDoFn: A DoFn for transposing NoteSequence objects.
    - DebugNoteSequenceDoFn: A dummy DoFn for debugging NoteSequence objects.

Functions:
    - DistinctNoteSequences: A PTransform for obtaining distinct NoteSequence objects.

"""
import logging
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict, Any

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.internal.metrics.metric import Metrics as internal_beam_metrics
from apache_beam.metrics import Metrics as beam_metrics
from apache_beam.utils.histogram import LinearBucket
from google.protobuf.json_format import MessageToJson
from resolv_mir.protobuf import NoteSequence
from resolv_mir.note_sequence import exceptions, constants, processors

from resolv_data.canonical import to_source_format
from beam.dofn.base import ConfigurableDoFn, DoFnDebugConfig, DebugOutputTypeEnum


@beam.typehints.with_input_types(NoteSequence)
@beam.typehints.with_output_types(NoteSequence)
class NoteSequenceDoFn(ConfigurableDoFn, ABC):
    """
    An abstract base class for NoteSequence processing DoFns. NoteSequence specific processing must be implemented in
    the '_process_internal' method. The 'process()' method of this class takes care of updating the statistics and
    eventually write debug infos. Statistics handled by default are:
        - input_sequences: total number of sequences processed by the class
        - output_sequences: total number of sequences in output of the class

    Inherits:
        ConfigurableDoFn: Base class for configurable Apache Beam DoFns.

    Methods:
        process: Process a single NoteSequence object.
        _write_debug: Write debug information about processed NoteSequence objects.
    """

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(NoteSequenceDoFn, self).__init__(config, debug_config)

    @abstractmethod
    def _process_internal(self, note_sequence: NoteSequence) -> List[NoteSequence]:
        """
        Internal method for processing a single NoteSequence object.

        Args:
            note_sequence (NoteSequence): The NoteSequence object to be processed.

        Returns:
            List[NoteSequence]: A list of processed NoteSequence objects.
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
            'output_sequences': beam_metrics.counter(self.namespace(), 'output_sequences')
        }
        internal_metrics = self._init_statistics_internal()
        if internal_metrics:
            default_metrics.update(internal_metrics)
        return default_metrics

    def process(self, note_sequence: NoteSequence, *args, **kwargs):
        """
        Process a single NoteSequence object, updates statistics and writes debug info if necessary.

        Args:
            note_sequence (NoteSequence): The NoteSequence object to be processed.

        Yields:
            processed_sequences: The list of processed NoteSequence objects.
        """
        self.statistics['input_sequences'].inc()
        processed_sequences = self._process_internal(note_sequence)
        for processed_sequence in processed_sequences:
            self._processed_elements += 1
            self._write_debug(processed_sequence)
            self.statistics['output_sequences'].inc()
            yield processed_sequence

    def _write_debug(self, processed_sequence: NoteSequence):
        """
        According to the provided debug configuration: ff debug is enabled, it converts the NoteSequence proto to the
        type specified by 'output_type' and writes it to the path specified by 'output_path'.

        Args:
            processed_sequence (NoteSequence): the processed NoteSequence proto to write debug info for.
        """
        if self._debug_config is not None and self._debug_config.enabled:
            file_name_pattern = re.compile(self._debug_config.output_file_pattern)
            if file_name_pattern.match(processed_sequence.id):
                base_filename = f'{processed_sequence.id}_{self._worker_id}_{self._processed_elements}'
                if self._debug_config.output_type == DebugOutputTypeEnum.JSON:
                    filename = f'{base_filename}.json'
                    key = f"{self._debug_config.output_path}/debug/processors/{self.__class__.__name__}/{filename}"
                    processed_sequence.id = base_filename
                    processed_sequence.filepath = key
                    data = MessageToJson(processed_sequence).encode()
                    mime_type = 'application/json'
                else:
                    source_type = Path(processed_sequence.filepath).suffix
                    filename = f'{base_filename}{source_type}'
                    key = f"{self._debug_config.output_path}/debug/processors/{self.__class__.__name__}/{filename}"
                    data = to_source_format(source_type, processed_sequence)
                    mime_type = 'application/octet-stream'
                with FileSystems.create(key, mime_type) as writer:
                    writer.write(data)


class ExtractMelodyDoFn(NoteSequenceDoFn):
    """ A DoFn for extracting melody from NoteSequence proto message.
        Check out 'extract_melodies_from_note_sequence' method from 'extractor' processor module in 'note_sequence' lib
        for a complete description of the configuration parameters and statistics.
    """

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(ExtractMelodyDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
        return "Melody Extractor"

    @staticmethod
    def namespace() -> str:
        return "melody_extractor"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            'search_start_step': 0,
            'min_bars': 2,
            'max_bars_truncate': None,
            'max_bars_discard': None,
            'min_pitch': constants.PIANO_MIN_MIDI_PITCH,
            'max_pitch': constants.PIANO_MAX_MIDI_PITCH,
            'gap_bars': 1,
            'min_unique_pitches': 3,
            'ignore_polyphonic_notes': True,
            'filter_drums': True,
            'valid_programs': constants.MEL_PROGRAMS
        }

    def _init_statistics_internal(self):
        statistics = dict((stat_name, beam_metrics.counter(self.namespace(), stat_name)) for stat_name in
                          ['polyphonic_tracks_discarded',
                           'non_integer_steps_per_bar_tracks_discarded',
                           'melodies_discarded_too_short',
                           'melodies_discarded_too_few_pitches',
                           'melodies_discarded_too_long',
                           'melodies_truncated'])
        # statistics['melody_lengths_in_bars'] = beam_metrics.distribution(self.namespace(), 'melody_lengths_in_bars')
        statistics['melody_lengths_in_bars'] = internal_beam_metrics.histogram(self.namespace(),
                                                                               'melody_lengths_in_bars',
                                                                               LinearBucket(0, 1, 1000))
        return statistics

    def _process_internal(self, quantized_sequence: NoteSequence) -> List[NoteSequence]:
        melodies, stats = processors.extractor.extract_melodies_from_note_sequence(
            quantized_sequence,
            max_bars_truncate=self._config['max_bars_truncate'],
            min_bars=self._config['min_bars'],
            max_bars_discard=self._config['max_bars_discard'],
            min_unique_pitches=self._config['min_unique_pitches'],
            search_start_step=self._config['search_start_step'],
            min_pitch=self._config['min_pitch'],
            max_pitch=self._config['max_pitch'],
            gap_bars=self._config['gap_bars'],
            ignore_polyphonic_notes=self._config['ignore_polyphonic_notes'],
            filter_drums=self._config['filter_drums'],
            valid_programs=self._config['valid_programs']
        )
        self.statistics['polyphonic_tracks_discarded'].inc(stats['polyphonic_tracks_discarded'].count)
        self.statistics['non_integer_steps_per_bar_tracks_discarded'].inc(
            stats['non_integer_steps_per_bar_tracks_discarded'].count
        )
        self.statistics['melodies_truncated'].inc(stats['melodies_truncated'].count)
        self.statistics['melodies_discarded_too_short'].inc(stats['melodies_discarded_too_short'].count)
        self.statistics['melodies_discarded_too_long'].inc(stats['melodies_discarded_too_long'].count)
        self.statistics['melodies_discarded_too_few_pitches'].inc(stats['melodies_discarded_too_few_pitches'].count)
        # TODO - Melody extraction: melody_lengths_in_bars histogram
        # self.statistics['melody_lengths_in_bars'].update(melody.total_quantized_steps // steps_per_bar)

        return melodies


class QuantizeDoFn(NoteSequenceDoFn):
    """ A DoFn for quantizing NoteSequence proto message.
        Check out 'quantizer' processor module in 'note_sequence' lib for more information.
    """

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(QuantizeDoFn, self).__init__(config, debug_config)
        if (self._config['steps_per_quarter'] is not None) == (self._config['steps_per_second'] is not None):
            raise ValueError('Exactly one of steps_per_quarter or steps_per_second must be set.')

    @staticmethod
    def name() -> str:
        return "Quantizer"

    @staticmethod
    def namespace() -> str:
        return "quantizer"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            'steps_per_quarter': constants.DEFAULT_STEPS_PER_QUARTER,
            'steps_per_second': None
        }

    def _init_statistics_internal(self):
        statistics = dict((stat_name, beam_metrics.counter(self.namespace(), stat_name)) for stat_name in
                          ['sequences_discarded_multiple_time_signatures',
                           'sequences_discarded_multiple_tempos',
                           'sequences_discarded_bad_time_signature',
                           'sequences_discarded_negative_time'])
        return statistics

    def _process_internal(self, note_sequence: NoteSequence) -> List[NoteSequence]:
        try:
            if self._config['steps_per_quarter'] is not None:
                quantized_sequence = processors.quantizer.quantize_note_sequence(note_sequence,
                                                                                 self._config['steps_per_quarter'])
            else:
                quantized_sequence = processors.quantizer.quantize_note_sequence_absolute(note_sequence,
                                                                                          self._config[
                                                                                              'steps_per_second'])
            return [quantized_sequence]
        except exceptions.MultipleTimeSignatureError as e:
            logging.warning('Multiple time signatures in NoteSequence %s: %s', note_sequence.filepath, e)
            self.statistics['sequences_discarded_multiple_time_signatures'].inc()
        except exceptions.MultipleTempoError as e:
            logging.warning('Multiple tempos found in NoteSequence %s: %s', note_sequence.filepath, e)
            self.statistics['sequences_discarded_multiple_tempos'].inc()
        except exceptions.BadTimeSignatureError as e:
            logging.warning('Bad time signature in NoteSequence %s: %s', note_sequence.filepath, e)
            self.statistics['sequences_discarded_bad_time_signature'].inc()
        except exceptions.NegativeTimeError as e:
            logging.warning('Negative time note in NoteSequence %s: %s', note_sequence.filepath, e)
            self.statistics['sequences_discarded_negative_time'].inc()

        return []


class SplitDoFn(NoteSequenceDoFn):
    """ A DoFn for splitting a NoteSequence proto message in subsequences.
        Check out the 'split_note_sequence' method in the 'splitter' processor module in 'note_sequence' lib for more
        information.
    """

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(SplitDoFn, self).__init__(config, debug_config)
        if (self._config['hop_size_seconds'] is not None) == (self._config['hop_size_bars'] is not None):
            raise ValueError('Exactly one of hop_size_seconds or hope_size_bars must be set.')

    @staticmethod
    def name() -> str:
        return "Splitter"

    @staticmethod
    def namespace() -> str:
        return "splitter"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            'hop_size_seconds': None,
            'hop_size_bars': 2,
            'skip_splits_inside_notes': False
        }

    def _init_statistics_internal(self):
        return None

    def _process_internal(self, note_sequence: NoteSequence) -> List[NoteSequence]:
        if self._config['hop_size_bars']:
            return processors.splitter.split_note_sequence(note_sequence, self._config['hop_size_bars'])
        return processors.splitter.split_note_sequence(note_sequence, self._config['hop_size_seconds'],
                                                       self._config['skip_splits_inside_notes'])


class TimeChangeSplitDoFn(NoteSequenceDoFn):
    """ A DoFn for splitting a NoteSequence proto message in subsequences on time or tempo change events.
        Check out the 'split_note_sequence_on_time_changes' method in the 'splitter' processor module in
        'note_sequence' lib for more information. In addition, this DoFn allows only to keep sequences which time
        signatures is among the ones specified by the configuration parameter 'keep_time_signatures'.
    """

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(TimeChangeSplitDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
        return "Time Change Splitter"

    @staticmethod
    def namespace() -> str:
        return "time_change_splitter"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            'skip_splits_inside_notes': False,
            'keep_time_signatures': ['4/4']
        }

    def _init_statistics_internal(self):
        return {
            'filtered_sequences': beam_metrics.counter(self.namespace(), 'filtered_sequences')
        }

    def _process_internal(self, note_sequence: NoteSequence) -> List[NoteSequence]:
        split_sequences = processors.splitter.split_note_sequence_on_time_changes(note_sequence,
                                                                                  skip_splits_inside_notes=self._config[
                                                                                      'skip_splits_inside_notes'])
        # Filter sequences that do not match a time signature in 'keep_time_signatures'
        if self._config['keep_time_signatures']:
            filtered_sequences = []
            for sequence in split_sequences:
                time_signature = f'{sequence.time_signatures[0].numerator}/{sequence.time_signatures[0].denominator}' \
                    if sequence.time_signatures else '4/4'
                if time_signature in self._config['keep_time_signatures']:
                    filtered_sequences.append(sequence)
                else:
                    self.statistics['filtered_sequences'].inc()
            return filtered_sequences
        else:
            return split_sequences


class SilenceSplitDoFn(NoteSequenceDoFn):
    """ A DoFn for splitting a NoteSequence proto message in subsequences when a silence of 'gap_seconds' is found.
        Check out the 'split_note_sequence_on_silence' method in the 'splitter' processor module in 'note_sequence' lib
        for more information.
    """

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(SilenceSplitDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
        return "Silence Splitter"

    @staticmethod
    def namespace() -> str:
        return "silence_splitter"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            'gap_seconds': 3.0
        }

    def _init_statistics_internal(self):
        return None

    def _process_internal(self, note_sequence: NoteSequence) -> List[NoteSequence]:
        return processors.splitter.split_note_sequence_on_silence(note_sequence, self._config['gap_seconds'])


class SliceDoFn(NoteSequenceDoFn):
    """ A DoFn for slicing a NoteSequence proto message in subsequences.
        Check out the 'slice_note_sequence_in_bars' method in the 'slicer' processor module in 'note_sequence' lib
        for more information.
    """

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(SliceDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
        return "Slicer"

    @staticmethod
    def namespace() -> str:
        return "slicer"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            'hop_size_bars': 1,
            'slice_size_bars': 2,
            'start_time': 0,
            'skip_splits_inside_notes': False,
            'allow_cropped_slices': False
        }

    def _init_statistics_internal(self):
        return None

    def _process_internal(self, note_sequence: NoteSequence) -> List[NoteSequence]:
        return processors.slicer.slice_note_sequence_in_bars(note_sequence, self._config['slice_size_bars'],
                                                             self._config['hop_size_bars'], self._config['start_time'],
                                                             self._config['skip_splits_inside_notes'],
                                                             self._config['allow_cropped_slices'])


class StretchDoFn(NoteSequenceDoFn):
    """ A DoFn for stretching the time duration of a NoteSequence proto message.
        Check out the 'stretch_note_sequence' method in the 'stretcher' processor module in 'note_sequence' lib
        for more information.
    """

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(StretchDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
        return "Stretcher"

    @staticmethod
    def namespace() -> str:
        return "stretcher"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            'stretch_factor': 1.0,
            'in_place': False
        }

    def _init_statistics_internal(self):
        return None

    def _process_internal(self, note_sequence: NoteSequence) -> List[NoteSequence]:
        return [processors.stretcher.stretch_note_sequence(note_sequence, self._config['stretch_factor'],
                                                           self._config['in_place'])]


class SustainDoFn(NoteSequenceDoFn):
    """ A DoFn for apply sustain control changes to the notes of a NoteSequence proto message.
        Check out the 'apply_sustain_control_changes' method in the 'sustainer' processor module in 'note_sequence' lib
        for more information.
    """

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(SustainDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
        return "Sustainer"

    @staticmethod
    def namespace() -> str:
        return "sustainer"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            'sustain_control_number': 64
        }

    def _init_statistics_internal(self):
        return None

    def _process_internal(self, note_sequence: NoteSequence) -> List[NoteSequence]:
        return [
            processors.sustainer.apply_sustain_control_changes(note_sequence, self._config['sustain_control_number'])]


class TransposeDoFn(NoteSequenceDoFn):
    """ A DoFn for transposing the notes of a NoteSequence proto message.
        Check out the 'transpose_note_sequence' method in the 'transposer' processor module in 'note_sequence' lib
        for more information.
    """

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(TransposeDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
        return "Transposer"

    @staticmethod
    def namespace() -> str:
        return "transposer"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            'amount': 0,
            'min_allowed_pitch': constants.MIN_MIDI_PITCH,
            'max_allowed_pitch': constants.MAX_MIDI_PITCH,
            'transpose_chords': True,
            'delete_notes': True,
            'in_place': False,
        }

    def _init_statistics_internal(self):
        statistics = dict((stat_name, beam_metrics.counter(self.namespace(), stat_name)) for stat_name in
                          ['skipped_due_to_range_exceeded', 'transpositions_generated'])
        statistics['deleted_notes'] = beam_metrics.distribution(self.namespace(), 'deleted_notes')
        return statistics

    def _process_internal(self, note_sequence: NoteSequence) -> List[NoteSequence]:
        transposed_ns, del_notes = (
            processors.transposer.transpose_note_sequence(note_sequence,
                                                          amount=self._config['amount'],
                                                          min_allowed_pitch=self._config['min_allowed_pitch'],
                                                          max_allowed_pitch=self._config['max_allowed_pitch'],
                                                          transpose_chords=self._config['transpose_chords'],
                                                          delete_notes=self._config['delete_notes'],
                                                          in_place=self._config['in_place']))
        self.statistics['deleted_notes'].update(del_notes)
        if not transposed_ns.notes:
            self.statistics['skipped_due_to_range_exceeded'].inc()
            return []
        else:
            self.statistics['transpositions_generated'].inc()
            return [transposed_ns]


class DebugNoteSequenceDoFn(NoteSequenceDoFn):
    """ A dummy NoteSequenceDoFn useful to debug note sequences in BEAM pipelines"""

    def __init__(self, config: Dict[str, Any] = None, debug_config: DoFnDebugConfig = None):
        super(DebugNoteSequenceDoFn, self).__init__(config, debug_config)

    @staticmethod
    def name() -> str:
        return "DebugNoteSequence"

    @staticmethod
    def namespace() -> str:
        return "debug_note_sequence"

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {}

    def _init_statistics_internal(self):
        return None

    def _process_internal(self, note_sequence: NoteSequence) -> List[NoteSequence]:
        return [note_sequence]


@beam.ptransform_fn
@beam.typehints.with_input_types(NoteSequence)
@beam.typehints.with_output_types(NoteSequence)
def DistinctNoteSequences(pcollection, debug_config: DoFnDebugConfig):
    """
    A PTransform for obtaining distinct NoteSequence objects.

    Args:
        pcollection: Input PCollection of NoteSequence objects.
        debug_config (DoFnDebugConfig): Debugging configuration for the DebugNoteSequenceDoFn.

    Returns:
        PCollection: Output PCollection of distinct NoteSequence objects.
    """
    distinct_seqs = (
            pcollection
            | 'Tuple' >> beam.Map(lambda ns: (ns, ns.id))
            | 'Group' >> beam.GroupByKey()
            | 'Uniquify' >> beam.Keys()
            | 'Shuffle' >> beam.Reshuffle()
    )

    if debug_config.enabled:
        return distinct_seqs | 'DebugDistinct' >> beam.ParDo(DebugNoteSequenceDoFn(debug_config=debug_config))
    else:
        return distinct_seqs


# Dictionary mapping processor names to their respective classes
NS_DO_FN_MAP = {
    'melody_extractor_ps_do_fn': ExtractMelodyDoFn,
    'quantizer_ps_do_fn': QuantizeDoFn,
    'splitter_ps_do_fn': SplitDoFn,
    'time_change_splitter_ps_do_fn': TimeChangeSplitDoFn,
    'silence_splitter_ps_do_fn': SilenceSplitDoFn,
    'slicer_ps_do_fn': SliceDoFn,
    'stretcher_ps_do_fn': StretchDoFn,
    'sustainer_ps_do_fn': SustainDoFn,
    'transposer_ps_do_fn': TransposeDoFn
}
