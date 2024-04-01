import unittest
from pathlib import Path

from resolv_pipelines.pipelines.datasets import ImportArchiveDatasetPipeline


class ImportArchivePipelineTest(unittest.TestCase):

    @property
    def output_dir(self) -> Path:
        return Path("./output/datasets")

    def setUp(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def test_import_archive_dataset_pipeline(self):
        ImportArchiveDatasetPipeline(
            source_dataset_names=["jsb-chorales-v1"],
            source_dataset_modes=["full"],
            output_path=self.output_dir,
            force_overwrite=True
        ).run_pipeline()


if __name__ == '__main__':
    unittest.main()
