"""
Iterable Dataset for AIS

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

from aistore.pytorch.base_iter_dataset import AISBaseIterDataset
from typing import List, Union, Dict
from aistore.sdk.ais_source import AISSource
from torch.utils.data import get_worker_info
from itertools import islice


class AISIterDataset(AISBaseIterDataset):
    """
    An iterable-style dataset that iterates over objects in AIS.
    If `etl_name` is provided, that ETL must already exist on the AIStore cluster.

    Args:
        ais_source_list (Union[AISSource, List[AISSource]]): Single or list of AISSource objects to load data
        prefix_map (Dict(AISSource, Union[str, List[str]]), optional): Map of AISSource objects to list of prefixes that only allows
        objects with the specified prefixes to be used from each source
        etl_name (str, optional): Optional ETL on the AIS cluster to apply to each object

    Note:
        Each object is represented as a tuple of object_name (str) and object_content (bytes)
    """

    def __init__(
        self,
        ais_source_list: Union[AISSource, List[AISSource]],
        prefix_map: Dict[AISSource, Union[str, List[str]]] = {},
        etl_name: str = None,
    ):
        super().__init__(ais_source_list, prefix_map)
        self._etl_name = etl_name

    def __iter__(self):
        worker_info = get_worker_info()

        if worker_info is None:
            # If not using multiple workers, load directly
            for obj in self._iterator:
                yield obj.name, obj.get(etl_name=self._etl_name).read_all()
        else:
            # Slice iterator based on worker id as starting index (0, 1, 2, ..) and steps of total workers
            for obj in islice(
                self._iterator, worker_info.id, None, worker_info.num_workers
            ):
                # Update each object to use WorkerRequestSession for multithreading support
                yield obj.name, obj.get(etl_name=self._etl_name).read_all()