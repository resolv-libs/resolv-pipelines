from pathlib import Path
from typing import Union, Callable

import tensorflow as tf

from .base import DataLoader


class TFRecordLoader(DataLoader):

    def __init__(self,
                 file_pattern: str,
                 options: tf.data.Options = None,
                 batch_size: int = 64,
                 cache_dataset: bool = True,
                 shuffle: bool = None,
                 shuffle_buffer_size: int = None,
                 decode_fn: Callable = None,
                 map_fn: Callable = None,
                 filter_fn: Callable = None,
                 augment_fn: Callable = None,
                 to_tensor_fn: Callable = None,
                 prefetch_buffer_size: int = tf.data.experimental.AUTOTUNE):
        self._file_pattern = file_pattern
        self._options = options
        self._batch_size = batch_size
        self._cache_dataset = cache_dataset
        self._shuffle = shuffle
        self._shuffle_buffer_size = shuffle_buffer_size
        self._prefetch_buffer_size = prefetch_buffer_size
        self._decode_fn = decode_fn
        self._map_fn = map_fn
        self._filter_fn = filter_fn
        self._augment_fn = augment_fn
        self._to_tensor_fn = to_tensor_fn

    # def load_dataset(self):
    #     dataset = tf.data.TFRecordDataset(
    #         filenames=str(self._file_path),
    #         compression_type='',
    #         buffer_size=None,
    #         num_parallel_reads=None,
    #         name='LoadTFRecordDataset'
    #     ).with_options(self._options)
    #
    #     _SEQUENCE_LENGTH = 64
    #     context_features = {metric: tf.io.FixedLenFeature([], dtype=tf.float32)
    #                         for metric in [field.name for field in NoteSequence.SequenceMetrics.DESCRIPTOR.fields]}
    #     sequence_features = {
    #         "pitch_seq": tf.io.FixedLenSequenceFeature([_SEQUENCE_LENGTH], dtype=tf.int64),
    #     }
    #
    #     if self._decode_fn:
    #         dataset = dataset.map(self._decode_fn,
    #                               num_parallel_calls=None,
    #                               deterministic=True,
    #                               name='DecodeTFRecordDataset')
    #     if self._map_fn:
    #         dataset = dataset.map(self._map_fn,
    #                               num_parallel_calls=None,
    #                               deterministic=True,
    #                               name='MapTFRecordDataset')
    #     if self._filter_fn:
    #         dataset = dataset.filter(self._filter_fn, name='FilterTFRecordDataset')
    #     if self._augment_fn:
    #         dataset = dataset.map(self._augment_fn,
    #                               num_parallel_calls=None,
    #                               deterministic=True,
    #                               name='AugmentTFRecordDataset')
    #     if self._to_tensor_fn:
    #         dataset = dataset.map(self._to_tensor_fn,
    #                               num_parallel_calls=None,
    #                               deterministic=True,
    #                               name='ToTensorTFRecordDataset')
    #     if self._shuffle:
    #         dataset = dataset.shuffle(
    #             buffer_size=self._shuffle_buffer_size if self._shuffle_buffer_size else 10 * self._batch_size,
    #             seed=None,
    #             reshuffle_each_iteration=None,
    #             name='ShuffleTFRecordDataset'
    #         )
    #     dataset = dataset.cache(filename='', name='CacheTFRecordDataset') if self._cache_dataset else dataset
    #     dataset = dataset.batch(
    #         batch_size=self._batch_size,
    #         drop_remainder=True,
    #         num_parallel_calls=None,
    #         deterministic=True,
    #         name='BatchTFRecordDataset'
    #     )
    #     dataset = dataset.prefetch(buffer_size=self._prefetch_buffer_size, name='PrefetchTFRecordDataset')
    #
    #     return dataset

    def load_dataset(self, attribute: str):
        def parse_sequence_example(serialized_example):
            context_parsed, _ = tf.io.parse_single_sequence_example(
                serialized_example,
                context_features={
                    attribute: tf.io.FixedLenFeature([], dtype=tf.float32)
                },
                sequence_features={}
            )
            return context_parsed[attribute]

        options = tf.data.Options()
        options.deterministic = True
        options.autotune.enabled = True
        options.experimental_optimization.apply_default_optimizations = True
        files = tf.data.Dataset.list_files(self._file_pattern).with_options(options)
        dataset = files.interleave(tf.data.TFRecordDataset)
        dataset = dataset.map(parse_sequence_example)
        dataset = dataset.batch(batch_size=self._batch_size, drop_remainder=True)
        return dataset
