import functools
import unittest
from pathlib import Path

import tensorflow as tf

from resolv_pipelines.data.loaders.tfrecord_loader import TFRecordLoader
from resolv_pipelines.data.representation.mir.note_sequence import PitchSequenceRepresentation


class TFRecordLoaderTest(unittest.TestCase):

    @property
    def input_dir(self) -> Path:
        return Path("./output/datasets/representation/4bars_melodies_distinct/jsb_chorales-v1.0.0-full/mxml/pitchseq")

    def test_raw_sequence_tfrecord_loader(self):
        @tf.py_function(Tout=tf.string)
        def map_fn(seq_example):
            example = tf.train.SequenceExample()
            example.ParseFromString(seq_example.numpy())
            tf.print(example)
            return seq_example

        tfrecord_loader = TFRecordLoader(
            file_pattern=f"{self.input_dir}/*.tfrecord",
            parse_fn=lambda x: map_fn(x),
            deterministic=True,
            seed=42
        )
        next(iter(tfrecord_loader.load_dataset()))
        self.assertTrue(tfrecord_loader)

    def test_pitch_sequence_tfrecord_loader(self):
        representation = PitchSequenceRepresentation(sequence_length=64)
        tfrecord_loader = TFRecordLoader(
            file_pattern=f"{self.input_dir}/*.tfrecord",
            parse_fn=functools.partial(
                representation.parse_example,
                attributes_to_parse=["contour"],
                parse_sequence_feature=False
            ),
            deterministic=True,
            seed=42
        )
        for batch in iter(tfrecord_loader.load_dataset()):
            print(batch)
        self.assertTrue(tfrecord_loader)


if __name__ == '__main__':
    unittest.main()
