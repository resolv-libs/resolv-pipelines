from abc import abstractmethod
from typing import List, Dict

import tensorflow as tf
from resolv_mir import NoteSequence
from resolv_mir.note_sequence import representations

from ..base import SequenceRepresentation


class NoteSequenceRepresentation(SequenceRepresentation):

    def __init__(self, sequence_length: int, keep_attributes: bool = False):
        super(NoteSequenceRepresentation, self).__init__(sequence_length, keep_attributes)

    @property
    def attributes_field_name(self) -> str:
        return "attributes"

    @property
    def attribute_fields(self) -> List[str]:
        return [f.name for f in NoteSequence.SequenceAttributes.DESCRIPTOR.fields]

    @abstractmethod
    def to_sequence_example(self, note_sequence: NoteSequence) -> tf.train.SequenceExample:
        pass

    @abstractmethod
    def sequence_features(self, *args, **kwargs) -> Dict[str, tf.train.Feature]:
        pass


class PitchSequenceRepresentation(NoteSequenceRepresentation):

    def __init__(self, sequence_length: int, keep_attributes: bool = False):
        super(PitchSequenceRepresentation, self).__init__(sequence_length, keep_attributes)

    def to_sequence_example(self, note_sequence: NoteSequence) -> tf.train.SequenceExample:
        pitch_seq = representations.pitch_sequence_representation(note_sequence)
        feature_lists = tf.train.FeatureLists(feature_list={
            "pitch_seq": tf.train.FeatureList(feature=[
                tf.train.Feature(int64_list=tf.train.Int64List(value=pitch_seq))
            ])
        })
        sequence_example = tf.train.SequenceExample(feature_lists=feature_lists)
        return sequence_example

    def sequence_features(self, *args, **kwargs) -> Dict[str, tf.train.Feature]:
        return {
            "pitch_seq": tf.io.FixedLenSequenceFeature([self._sequence_length], dtype=tf.int64)
        }
