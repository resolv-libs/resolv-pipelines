import unittest
from pathlib import Path
from typing import Dict, Any

from resolv_mir import note_sequence, NoteSequence

from resolv_pipelines.dofn.mir.symbolic_music.attributes import ATTRIBUTE_DO_FN_MAP
from resolv_pipelines.dofn.mir.symbolic_music.processors import NS_DO_FN_MAP
from resolv_pipelines.pipelines.datasets.process_dataset import ProcessDatasetPipeline


class ProcessDatasetPipelineTest(unittest.TestCase):

    @property
    def output_dir(self) -> Path:
        return Path("./output/datasets")

    @property
    def processors_config(self) -> Dict[str, Dict[str, Any]]:
        return {
            "time_change_splitter": {
                "order": 1,
                "skip_splits_inside_notes": False,
                "keep_time_signatures": [
                    "4/4"
                ]
            },
            "quantizer": {
                "order": 2,
                "steps_per_quarter": 4
            },
            "melody_extractor": {
                "order": 3,
                "filter_drums": True,
                "gap_bars": 1,
                "ignore_polyphonic_notes": True,
                "min_pitch": 21,
                "max_pitch": 108,
                "min_bars": 4,
                "min_unique_pitches": 3,
                "search_start_step": 0,
                "valid_programs": note_sequence.constants.MEL_PROGRAMS
            },
            "slicer": {
                "order": 4,
                "allow_cropped_slices": False,
                "hop_size_bars": 1,
                "skip_splits_inside_notes": False,
                "slice_size_bars": 4,
                "start_time": 0,
                "keep_shorter_slices": False
            }
        }

    @property
    def attributes_config(self) -> Dict[str, Dict[str, Any]]:
        return {
            "toussaint": {
                "order": 1,
                "bars": 4,
                "binary": True
            }
        }

    def setUp(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def test_process_dataset_pipeline(self):
        ProcessDatasetPipeline(
            canonical_format=NoteSequence,
            allowed_processors_map=NS_DO_FN_MAP,
            processors_config=self.processors_config,
            source_dataset_names=["jsb-chorales-v1"],
            source_dataset_modes=["full"],
            source_dataset_file_types=["mxml"],
            input_path=Path("./output/datasets/canonical"),
            output_path=self.output_dir,
            distinct=True,
            force_overwrite=True,
            debug=False
        ).run_pipeline()

    def test_compute_attributes_pipeline(self):
        ProcessDatasetPipeline(
            canonical_format=NoteSequence,
            allowed_processors_map=ATTRIBUTE_DO_FN_MAP,
            processors_config=self.attributes_config,
            source_dataset_names=["jsb-chorales-v1"],
            source_dataset_modes=["full"],
            source_dataset_file_types=["mxml"],
            input_path=Path("./output/datasets/generated"),
            input_path_prefix="data",
            output_path=self.output_dir,
            output_path_prefix="metrics",
            force_overwrite=True,
            debug=False
        ).run_pipeline()


if __name__ == '__main__':
    unittest.main()