from abc import ABC, abstractmethod

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
            context_features = {metric: tf.train.Feature(float_list=tf.train.FloatList(value=[value]))
                                for metric, value in note_sequence.metrics.ListFields()}
            context = tf.train.Features(feature=context_features)
            sequence_example.context.CopyFrom(context)
        yield sequence_example


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
