from abc import ABC, abstractmethod
from typing import Union, Any, List, Dict

import tensorflow as tf

from ...canonical import CanonicalFormat


class Representation(ABC):

    @abstractmethod
    def to_example(self, canonical_format: CanonicalFormat, **kwargs) -> tf.train.Example:
        pass

    @abstractmethod
    def parse_example(self, serialized_example, **kwargs) -> Any:
        pass


class SequenceRepresentation(Representation):

    def __init__(self, sequence_length: int, keep_attributes: bool = False):
        super(SequenceRepresentation, self).__init__()
        self._sequence_length = sequence_length
        self._keep_attributes = keep_attributes

    @property
    @abstractmethod
    def attributes_field_name(self) -> str:
        pass

    @property
    @abstractmethod
    def attribute_fields(self) -> List[str]:
        pass

    @abstractmethod
    def to_sequence_example(self, canonical_format: CanonicalFormat) -> tf.train.SequenceExample:
        pass

    @abstractmethod
    def sequence_features(self, *args, **kwargs) -> Dict[str, tf.train.Feature]:
        pass

    def context_features(self, attributes_to_parse: List[str], default_value: int = None) \
            -> Dict[str, tf.train.Feature]:
        if not attributes_to_parse:
            return {}
        else:
            return {
                attribute: tf.io.FixedLenFeature([], dtype=tf.float32, default_value=default_value)
                for attribute in self.attribute_fields if attribute in attributes_to_parse
            }

    def to_example(self, canonical_format: CanonicalFormat, **kwargs) -> tf.train.Example:
        sequence_example = self.to_sequence_example(canonical_format)
        if self._keep_attributes:
            context = tf.train.Features(feature=self._get_attributes_features(canonical_format))
            sequence_example.context.CopyFrom(context)
        return sequence_example

    def parse_example(self, serialized_example, **kwargs) -> Any:
        return tf.io.parse_single_sequence_example(
            serialized_example,
            context_features=self.context_features(
                attributes_to_parse=kwargs.get('attributes_to_parse'),
                default_value=kwargs.get('default_attributes_value')
            ),
            sequence_features=self.sequence_features(**kwargs) if kwargs.get('parse_sequence_feature') else {}
        )

    def _get_attributes_features(self, canonical_format: CanonicalFormat) -> Dict[str, tf.train.Feature]:
        field_values = {}
        sequence_attributes = getattr(canonical_format, self.attributes_field_name)
        for attribute_field in self.attribute_fields:
            field_value = getattr(sequence_attributes, attribute_field)
            field_values[attribute_field] = tf.train.Feature(float_list=tf.train.FloatList(value=[field_value]))
        return field_values
