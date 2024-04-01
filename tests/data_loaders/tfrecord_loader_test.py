import functools
import unittest
from pathlib import Path

from resolv_pipelines.data.loaders.tfrecord_loader import TFRecordLoader
from resolv_pipelines.data.representation.mir.note_sequence import PitchSequenceRepresentation


class TFRecordLoaderTest(unittest.TestCase):

    @property
    def input_dir(self) -> Path:
        return Path("./output/datasets/representation/4bars_melodies_distinct/jsb_chorales-v1.0.0-full/mxml/pitchseq")

    def test_tfrecord_loader(self):
        representation = PitchSequenceRepresentation(sequence_length=64)
        tfrecord_loader = TFRecordLoader(
            file_pattern=f"{self.input_dir}/*.tfrecord",
            parse_fn=functools.partial(
                representation.parse_example,
                attributes_to_parse=["toussaint"],
                parse_sequence_feature=False
            )
        )
        print(next(iter(tfrecord_loader.load_dataset())))
        self.assertTrue(tfrecord_loader)


if __name__ == '__main__':
    unittest.main()
