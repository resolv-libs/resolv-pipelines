""" This module provides Apache Beam DoFns for data processing tasks.

Classes:
    - CountElementsDoFn: A DoFn for counting elements in a PCollection.
    - GenerateHistogram: A DoFn for generating histograms from metric data.
    - UploadToS3: A DoFn for uploading data to an S3 storage.

"""
import io
import logging
from typing import TypeVar, List, Tuple

import apache_beam as beam
import matplotlib.pyplot as plt
from apache_beam.io.filesystems import FileSystems
from apache_beam.metrics import Metrics as beam_metrics


# Type variables
T = TypeVar('T')


@beam.typehints.with_input_types(T)
@beam.typehints.with_output_types(T)
class CountElementsDoFn(beam.DoFn):
    """ A DoFn for counting elements in a PCollection. """

    def __init__(self, namespace: str = "stats", name: str = "total_elements"):
        """ Initializes a CountElementsDoFn object.

        Args:
            namespace (str, optional): The namespace for the counter metric. Defaults to "stats".
            name (str, optional): The name of the counter metric. Defaults to "total_elements".
        """
        super(CountElementsDoFn, self).__init__()
        self._namespace = namespace
        self._name = name
        self._elements_counter = None

    def start_bundle(self):
        self._elements_counter = beam_metrics.counter(self._namespace, self._name)

    def process(self, element, *args, **kwargs):
        self._elements_counter.inc()
        yield element


@beam.typehints.with_input_types(Tuple[str, List[float]])
@beam.typehints.with_output_types(Tuple[str, bytes])
class GenerateHistogram(beam.DoFn):
    """ A DoFn for generating histograms from metric data.

    Attributes:
        _bins (List[int]): A list containing the number of bins for the histograms.

    Methods:
        process: Process a single element and yield the generated histogram.
    """

    def __init__(self, bins: List[int] = None):
        """ Initializes a GenerateHistogram object.

        Args:
            bins (List[int], optional): A list containing the number of bins for the histograms. Defaults to None (uses
             [20] as default).
        """
        super(GenerateHistogram, self).__init__()
        self._bins = bins if bins is not None else [20]

    def process(self, element, *args, **kwargs):
        """ Process a single element and yield the generated histogram.

        Args:
            element (Tuple[str, List[float]]): A tuple containing the metric ID and its corresponding data.

        Yields:
            Tuple[str, bytes]: A tuple containing the name of the generated histogram image and its binary data.
        """
        metric_id, metric_data = element
        for bins in self._bins:
            logging.info(f"Generating histogram with {bins} bins for metric {metric_id}...")
            plt.hist(metric_data, bins=bins, color='blue', alpha=0.7)
            plt.title(f'{metric_id.replace("_", " ").capitalize()} - {bins} bins')
            image_bytes_io = io.BytesIO()
            plt.savefig(image_bytes_io, format='png')
            plt.close()
            image_bytes = image_bytes_io.getvalue()
            image_name = f'{metric_id}_histogram_{bins}_bins.png'
            yield image_name, image_bytes


class UploadToS3(beam.DoFn):
    """ A DoFn for uploading data to an S3 storage.

    Attributes:
        _output_path (str): The S3 output path for uploading data.
        _mime_type (str): The MIME type of the data.

    Methods:
        process: Process a single element by uploading it to S3.
    """

    def __init__(self, output_path: str, mime_type: str = "application/octet-stream"):
        """ Initializes an UploadToS3 object.

        Args:
            output_path (str): The S3 path where to upload the data.
            mime_type (str, optional): The MIME type of the data to upload. Defaults to "application/octet-stream".
        """
        super(UploadToS3, self).__init__()
        self._mime_type = mime_type
        self._output_path = output_path

    def process(self, element, *args, **kwargs):
        """ Process a single element and upload it to S3.

        Args:
            element (Tuple[str, bytes]): A tuple containing the filename and data to be uploaded.
        """
        filename, data = element
        with FileSystems.create(f'{self._output_path}/{filename}', self._mime_type) as writer:
            writer.write(data)


# TODO - BEAM utilities: Logging DoFn
# class LogProgress(beam.DoFn):
#     def __init__(self, nun_workers: int):
#         super(LogProgress, self).__init__()
#         self._processed_elements = None
#         self._nun_workers = nun_workers
#
#     def setup(self):
#         self._processed_elements = 0
#
#     def process(self, element, *args, **kwargs):
#         total_elements = args[0] / self._nun_workers
#         self._processed_elements += 1
#         remaining_elements = max(0, total_elements - self._processed_elements)
#         progress = (self._processed_elements / total_elements) * 100
#         logging.info(f"Progress: {int(progress)}%. "
#                      f"Processed {self._processed_elements} elements, {remaining_elements} remaining...")
#         yield element
