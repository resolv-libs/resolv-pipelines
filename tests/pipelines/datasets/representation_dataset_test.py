import unittest
from pathlib import Path
from typing import Dict, Any

from resolv_mir import NoteSequence

from resolv_pipelines.pipelines.dofn.mir.symbolic_music.augmenters import NS_AUG_DO_FN_MAP
from resolv_pipelines.pipelines.dofn.mir.symbolic_music.representations import PitchSequenceRepresentationDoFn
from resolv_pipelines.pipelines.datasets.representation_dataset import RepresentationDatasetPipeline


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
            representation_do_fn=PitchSequenceRepresentationDoFn(keep_attributes=True),
            canonical_format=NoteSequence,
            augmenters_config=self.augmenters_config,
            allowed_augmenters_map=NS_AUG_DO_FN_MAP,
            source_dataset_names=["jsb-chorales-v1"],
            source_dataset_modes=["full"],
            source_dataset_file_types=["mxml"],
            input_path=Path("./output/datasets/generated"),
            input_path_prefix="metrics",
            output_path=self.output_dir,
            output_path_prefix="pitchseq",
            force_overwrite=True,
            debug=False
        ).run_pipeline()


if __name__ == '__main__':
    unittest.main()
