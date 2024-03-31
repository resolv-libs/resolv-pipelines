import logging
from abc import ABC, abstractmethod
from typing import Dict

import apache_beam as beam
import tensorflow as tf
from resolv_mir.note_sequence import representations
from resolv_mir.protobuf import NoteSequence


@beam.typehints.with_input_types(NoteSequence)
@beam.typehints.with_output_types(tf.train.SequenceExample)
class NoteSequenceRepresentationDoFn(ABC, beam.DoFn):

    def __init__(self, keep_attributes: bool = False):
        super(NoteSequenceRepresentationDoFn, self).__init__()
        self._keep_attributes = keep_attributes

    @abstractmethod
    def _process_internal(self, note_sequence: NoteSequence) -> tf.train.SequenceExample:
        pass

    def process(self, note_sequence: NoteSequence, *args, **kwargs):
        sequence_example = self._process_internal(note_sequence)
        if self._keep_attributes:
            context = tf.train.Features(feature=self._get_attributes_features(note_sequence))
            sequence_example.context.CopyFrom(context)
        yield sequence_example

    @staticmethod
    def _get_attributes_features(note_sequence: NoteSequence) -> Dict[str, tf.train.Feature]:
        field_values = {}
        sequence_attributes = note_sequence.metrics
        for field_descriptor in NoteSequence.SequenceMetrics.DESCRIPTOR.fields:
            field_name = field_descriptor.name
            field_value = getattr(sequence_attributes, field_name)
            if not field_value:
                field_value = tf.keras.backend.epsilon()
            field_values[field_name] = tf.train.Feature(float_list=tf.train.FloatList(value=[field_value]))
        return field_values


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
