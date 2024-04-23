
from typing import Callable

import tensorflow as tf

from .base import DataLoader


class TFRecordLoader(DataLoader):

    def __init__(self,
                 file_pattern: str,
                 parse_fn: Callable,
                 map_fn: Callable = None,
                 filter_fn: Callable = None,
                 options: tf.data.Options = None,
                 batch_size: int = 64,
                 batch_drop_reminder: bool = False,
                 repeat_count: int = None,
                 shuffle: bool = None,
                 shuffle_buffer_size: int = None,
                 shuffle_repeat: bool = False,
                 cache_dataset: bool = False,
                 cache_filename: str = "",
                 prefetch_buffer_size: int = tf.data.experimental.AUTOTUNE,
                 interleave_cycle_length: int = None,
                 interleave_block_length: int = None,
                 num_parallel_calls: int = tf.data.experimental.AUTOTUNE,
                 deterministic: bool = False,
                 seed: int = None):
        self._file_pattern = file_pattern
        self._parse_fn = parse_fn
        self._map_fn = map_fn
        self._filter_fn = filter_fn
        self._deterministic = deterministic
        self._options = options if options else self.default_options
        self._batch_size = batch_size
        self._batch_drop_reminder = batch_drop_reminder
        self._repeat_count = repeat_count
        self._shuffle = shuffle
        self._shuffle_buffer_size = shuffle_buffer_size
        self._shuffle_repeat = shuffle_repeat
        self._cache_dataset = cache_dataset
        self._cache_filename = cache_filename
        self._prefetch_buffer_size = prefetch_buffer_size
        self._interleave_cycle_length = interleave_cycle_length
        self._interleave_block_length = interleave_block_length
        self._num_parallel_calls = num_parallel_calls
        self._seed = seed

    @property
    def default_options(self) -> tf.data.Options:
        options = tf.data.Options()
        options.deterministic = self._deterministic
        options.autotune.enabled = True
        options.experimental_optimization.apply_default_optimizations = True
        return options

    def load_dataset(self) -> tf.data.TFRecordDataset:
        files = tf.data.Dataset.list_files(
            file_pattern=self._file_pattern,
            shuffle=False,
            seed=self._seed,
            name='ListFilesTFRecordDataset'
        ).with_options(self._options)

        dataset = files.interleave(
            tf.data.TFRecordDataset,
            cycle_length=self._interleave_cycle_length,
            block_length=self._interleave_block_length,
            num_parallel_calls=self._num_parallel_calls,
            deterministic=self._deterministic,
            name='InterleaveTFRecordDataset'
        )

        dataset = dataset.map(
            self._parse_fn,
            num_parallel_calls=self._num_parallel_calls,
            deterministic=self._deterministic,
            name='ParseTFRecordDataset'
        )

        if self._map_fn:
            dataset = dataset.map(
                self._map_fn,
                num_parallel_calls=self._num_parallel_calls,
                deterministic=self._deterministic,
                name='MapTFRecordDataset'
            )

        if self._filter_fn:
            dataset = dataset.filter(self._filter_fn, name='FilterTFRecordDataset')

        if self._cache_dataset:
            dataset = dataset.cache(filename=self._cache_filename, name='CacheTFRecordDataset')

        if self._shuffle:
            dataset = dataset.shuffle(
                buffer_size=self._shuffle_buffer_size if self._shuffle_buffer_size else dataset.cardinality(),
                seed=self._seed,
                reshuffle_each_iteration=self._shuffle_repeat,
                name='ShuffleTFRecordDataset'
            )

        if self._repeat_count:
            dataset = dataset.repeat(self._repeat_count)

        if self._batch_size:
            dataset = dataset.batch(
                batch_size=self._batch_size,
                drop_remainder=self._batch_drop_reminder,
                num_parallel_calls=self._num_parallel_calls,
                deterministic=self._deterministic,
                name='BatchTFRecordDataset'
            )

        if self._prefetch_buffer_size:
            dataset = dataset.prefetch(buffer_size=self._prefetch_buffer_size, name='PrefetchTFRecordDataset')

        return dataset
