import unittest
from pathlib import Path

from resolv_pipelines.data_loaders.tfrecord_loader import TFRecordLoader


class TFRecordLoaderTest(unittest.TestCase):

    @property
    def input_dir(self) -> Path:
        return Path("./output/datasets/representation/jsb_chorales-v1.0.0-full/pitchseq")

    def test_tfrecord_loader(self):
        tfrecord_loader = TFRecordLoader(file_pattern=f"{self.input_dir}/*.tfrecord")
        dataset = tfrecord_loader.load_dataset("toussaint")
        for batch in iter(dataset):
            print(batch)


if __name__ == '__main__':
    unittest.main()
