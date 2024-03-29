import os

import keras
import matplotlib.pyplot as plt
import tensorflow as tf

from resolv_mir import NoteSequence
from resolv_ml.utilities.statistic.power_transforms import BoxCox

from scripts.utilities.constants import Paths

_DATASET_FILE_PATTERN = (Paths.GENERATED_DATASETS_DIR /
                         '4bars_melodies/jsb_chorales-v1.0.0-full/mxml/representation-*.tfrecord')


def parse_sequence_example(serialized_example):
    context_features = {metric: tf.io.FixedLenFeature([], dtype=tf.float32)
                        for metric in [field.name for field in NoteSequence.SequenceMetrics.DESCRIPTOR.fields]}
    sequence_features = {
        "pitch_seq": tf.io.FixedLenSequenceFeature([64], dtype=tf.int64),
    }
    context_parsed, sequence_parsed = tf.io.parse_single_sequence_example(
        serialized_example,
        context_features=context_features,
        sequence_features=sequence_features
    )
    return context_parsed, sequence_parsed


def plot_distribution(x, output_fig_name: str):
    plt.hist(x, bins=30, color='blue', alpha=0.7)
    plt.title(f'note_change_ratio - 30 bins')
    plt.savefig(f'{output_fig_name}.png', format='png', dpi=300)
    plt.close()


class BoxCoxModel(keras.Model):

    # noinspection PyAttributeOutsideInit
    def build(self, input_shape):
        self._box_cox_layer = BoxCox(lambda_init=0.33, batch_norm=keras.layers.BatchNormalization())

    def call(self, inputs):
        return self._box_cox_layer(inputs)


if __name__ == '__main__':
    os.environ["KERAS_BACKEND"] = "tensorflow"
    batch_size = 64
    options = tf.data.Options()
    options.deterministic = True
    options.autotune.enabled = True
    options.experimental_optimization.apply_default_optimizations = True
    files = tf.data.Dataset.list_files(str(_DATASET_FILE_PATTERN)).with_options(options)
    dataset = files.interleave(tf.data.TFRecordDataset)
    dataset = dataset.map(parse_sequence_example)
    dataset = dataset.batch(batch_size=batch_size, drop_remainder=True)

    box_cox_model = BoxCoxModel()
    box_cox_model.trainable = False
    box_cox_model.compile(run_eagerly=True)
    out = box_cox_model.predict(dataset)

    plot_distribution(out, "note_change_ratio")
    # keras.utils.plot_model(box_cox_model, "my_first_model_with_shape_info.png", show_shapes=True)
