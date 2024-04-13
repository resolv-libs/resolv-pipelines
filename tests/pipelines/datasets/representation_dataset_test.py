import unittest
from pathlib import Path
from typing import Dict, Any

from resolv_mir import NoteSequence

import resolv_pipelines.pipelines as resolv_pipelines
from resolv_pipelines.data.representation.mir import PitchSequenceRepresentation
from resolv_pipelines.pipelines.datasets import RepresentationDatasetPipeline


class RepresentationDatasetPipelineTest(unittest.TestCase):

    @property
    def output_dir(self) -> Path:
        return Path("./output/datasets")

    @property
    def augmenters_config(self) -> Dict[str, Dict[str, Any]]:
        return {
            "transposer": {
                "order": 1,
                "threshold": 0.7,
                "min_transpose_amount": -12,
                "max_transpose_amount": 12,
                "max_allowed_pitch": 108,
                "min_allowed_pitch": 21,
                "transpose_chords": False,
                "delete_notes": False,
                "in_place": True
            }
        }

    def setUp(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def test_pitch_sequence_representation_pipeline(self):
        RepresentationDatasetPipeline(
            representation=PitchSequenceRepresentation(sequence_length=64, keep_attributes=True),
            canonical_format=NoteSequence,
            augmenters_config=self.augmenters_config,
            allowed_augmenters_map=resolv_pipelines.SUPPORTED_NOTE_SEQ_AUGMENTERS,
            source_dataset_names=["jsb-chorales-v1"],
            source_dataset_modes=["full"],
            source_dataset_file_types=["mxml"],
            input_path=Path("./output/datasets/generated/4bars_melodies_distinct"),
            input_path_prefix="attributes",
            output_path=self.output_dir,
            output_path_prefix="pitchseq",
            split_ratios={"train": 70, "validation": 20, "test": 10},
            output_dataset_name="4bars_melodies_distinct",
            force_overwrite=True,
            debug=False
        ).run_pipeline()


if __name__ == '__main__':
    unittest.main()
