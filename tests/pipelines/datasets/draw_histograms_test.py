import unittest
from pathlib import Path

from resolv_mir import NoteSequence

import resolv_pipelines.pipelines as resolv_pipelines
from resolv_pipelines.pipelines.datasets.utilities import DrawHistogramPipeline


class DrawHistogramsPipelineTest(unittest.TestCase):

    @property
    def input_dir(self) -> Path:
        return Path("./output/datasets/generated/4bars_melodies_distinct")

    def test_draw_histograms_pipeline(self):
        DrawHistogramPipeline(
            canonical_format=NoteSequence,
            allowed_attributes_map=resolv_pipelines.SUPPORTED_NOTE_SEQ_ATTRIBUTES,
            attributes=["toussaint"],
            bins=[30, 40, 50],
            source_dataset_names=["jsb-chorales-v1"],
            source_dataset_modes=["full"],
            source_dataset_file_types=["mxml"],
            input_path=self.input_dir,
            input_path_prefix="metrics",
            force_overwrite=True
        ).run_pipeline()


if __name__ == '__main__':
    unittest.main()
