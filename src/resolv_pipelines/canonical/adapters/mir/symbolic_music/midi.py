"""
Module Name: midi.py.
Description: Data Adapter for MIDI files.
Author: Matteo Pettenò.
Copyright (c) 2024, Matteo Pettenò
License: Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0)
"""
import io
from typing import Any

from resolv_mir import NoteSequence
from resolv_mir.note_sequence.io import midi_io

from ....base import DataAdapter


class MIDIFileAdapter(DataAdapter[NoteSequence]):

    def to_canonical_message(self, source_type: str, content: bytes, metadata: Any) -> NoteSequence:
        return midi_io.midi_to_note_sequence(content, metadata)

    def to_source_format(self, note_sequence: NoteSequence, **kwargs) -> bytes:
        try:
            drop_events_n_seconds_after_last_note = kwargs['drop_events_n_seconds_after_last_note']
        except KeyError:
            drop_events_n_seconds_after_last_note = False
        pretty_midi = midi_io.note_sequence_to_midi(note_sequence, drop_events_n_seconds_after_last_note)
        midi_bytes_io = io.BytesIO()
        pretty_midi.write(midi_bytes_io)
        return midi_bytes_io.getvalue()
