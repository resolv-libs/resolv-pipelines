import unittest
from pathlib import Path

from resolv_pipelines.pipelines.datasets.canonicalize_dataset import CanonicalizeDatasetPipeline


class CanonicalizeDatasetPipelineTest(unittest.TestCase):

    @property
    def input_dir(self) -> Path:
        return Path("./output/datasets/raw")

    @property
    def output_dir(self) -> Path:
        return Path("./output/datasets")

    def setUp(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def test_canonicalize_mxml_dataset_pipeline(self):
        CanonicalizeDatasetPipeline(
            source_dataset_names=["jsb-chorales-v1"],
            source_dataset_modes=["full"],
            source_dataset_file_types=["mxml"],
            input_path=self.input_dir,
            output_path=self.output_dir,
            force_overwrite=True,
            debug=False
        ).run_pipeline()


if __name__ == '__main__':
    unittest.main()
