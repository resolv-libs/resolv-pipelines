""" TODO - Module DOC """
from pathlib import Path
from typing import Callable, Union

import tensorflow as tf

from scripts.ml.data_loaders.base import DataLoader


def _get_dataset_options() -> tf.data.DatasetOptions:
    options = tf.data.Options()
    options.deterministic = True
    options.experimental_external_state_policy = tf.data.experimental.ExternalStatePolicy.WARN
    options.experimental_slack = False
    options.experimental_symbolic_checkpoint = False
    options.experimental_warm_start = False
    # Autotune options
    autotune_options = tf.data.experimental.AutotuneOptions()
    autotune_options.enabled = False
    autotune_options.autotune_algorithm = tf.data.experimental.AutotuneAlgorithm.DEFAULT
    autotune_options.cpu_budget = None
    autotune_options.ram_budget = None
    options.autotune = autotune_options
    # Distribute options
    distribute_options = tf.data.experimental.DistributeOptions()
    distribute_options.auto_shard_policy = tf.data.experimental.AutoShardPolicy.ON
    distribute_options.num_devices = None
    options.experimental_distribute = distribute_options
    # Optimization options
    optimization_options = tf.data.experimental.OptimizationOptions()
    optimization_options.apply_default_optimizations = True
    optimization_options.filter_fusion = False
    optimization_options.filter_parallelization = False
    optimization_options.inject_prefetch = True
    optimization_options.map_and_batch_fusion = True
    optimization_options.map_and_filter_fusion = False
    optimization_options.map_fusion = False
    optimization_options.map_parallelization = True
    optimization_options.noop_elimination = True
    optimization_options.parallel_batch = True
    optimization_options.shuffle_and_repeat_fusion = True
    options.experimental_optimization = optimization_options
    # Threading options
    threading_options = tf.data.ThreadingOptions()
    threading_options.max_intra_op_parallelism = None
    threading_options.private_threadpool_size = 0
    options.threading = threading_options
    return options


class TFRecordLoader(DataLoader):

    def __init__(self,
                 file_path: Union[Path, str],
                 options: tf.data.DatasetOptions,
                 batch_size: int = 32,
                 cache_dataset: bool = True,
                 shuffle: bool = None,
                 shuffle_buffer_size: int = None,
                 decode_fn: Callable = None,
                 map_fn: Callable = None,
                 filter_fn: Callable = None,
                 augment_fn: Callable = None,
                 to_tensor_fn: Callable = None,
                 prefetch_buffer_size: int = tf.data.experimental.AUTOTUNE):
        self._file_path = file_path
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

    def load_dataset(self):
        dataset = tf.data.TFRecordDataset(
            filenames=str(self._file_path),
            compression_type='',
            buffer_size=None,
            num_parallel_reads=None,
            name='LoadTFRecordDataset'
        ).with_options(_get_dataset_options())

        if self._decode_fn:
            dataset = dataset.map(self._decode_fn,
                                  num_parallel_calls=None,
                                  deterministic=True,
                                  name='DecodeTFRecordDataset')
        if self._map_fn:
            dataset = dataset.map(self._map_fn,
                                  num_parallel_calls=None,
                                  deterministic=True,
                                  name='MapTFRecordDataset')
        if self._filter_fn:
            dataset = dataset.filter(self._filter_fn, name='FilterTFRecordDataset')
        if self._augment_fn:
            dataset = dataset.map(self._augment_fn,
                                  num_parallel_calls=None,
                                  deterministic=True,
                                  name='AugmentTFRecordDataset')
        if self._to_tensor_fn:
            dataset = dataset.map(self._to_tensor_fn,
                                  num_parallel_calls=None,
                                  deterministic=True,
                                  name='ToTensorTFRecordDataset')
        if self._shuffle:
            dataset = dataset.shuffle(
                buffer_size=self._shuffle_buffer_size if self._shuffle_buffer_size else 10 * self._batch_size,
                seed=None,
                reshuffle_each_iteration=None,
                name='ShuffleTFRecordDataset'
            )
        dataset = dataset.cache(filename='', name='CacheTFRecordDataset') if self._cache_dataset else dataset
        dataset = dataset.batch(
            batch_size=self._batch_size,
            drop_remainder=True,
            num_parallel_calls=None,
            deterministic=True,
            name='BatchTFRecordDataset'
        )
        dataset = dataset.prefetch(buffer_size=self._prefetch_buffer_size, name='PrefetchTFRecordDataset')

        return dataset
