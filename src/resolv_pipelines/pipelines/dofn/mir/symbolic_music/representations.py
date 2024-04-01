from abc import abstractmethod
from typing import List

import tensorflow as tf
from resolv_mir.note_sequence import representations
from resolv_mir.protobuf import NoteSequence

from ....dofn.base import RepresentationDoFn


class NoteSequenceRepresentationDoFn(RepresentationDoFn):

    def __init__(self, keep_attributes: bool = False):
        super(NoteSequenceRepresentationDoFn, self).__init__()
        self._keep_attributes = keep_attributes

    @property
    def attributes_field_name(self) -> str:
        return "attributes"

    @property
    def attribute_fields(self) -> List[str]:
        return [f.name for f in NoteSequence.SequenceAttributes.DESCRIPTOR.fields]

    @abstractmethod
    def _process_internal(self, note_sequence: NoteSequence) -> tf.train.SequenceExample:
        pass


class PitchSequenceRepresentationDoFn(NoteSequenceRepresentationDoFn):

    def _process_internal(self, note_sequence: NoteSequence) -> tf.train.SequenceExample:
        pitch_seq = representations.pitch_sequence_representation(note_sequence)
        feature_lists = tf.train.FeatureLists(feature_list={
            "pitch_seq": tf.train.FeatureList(feature=[
                tf.train.Feature(int64_list=tf.train.Int64List(value=pitch_seq))
            ])
        })
        sequence_example = tf.train.SequenceExample(feature_lists=feature_lists)
        return sequence_example


# Dictionary mapping augmentation processor names to their respective classes
NS_REPR_DO_FN_MAP = {
    'pitch_sequence': PitchSequenceRepresentationDoFn
}
