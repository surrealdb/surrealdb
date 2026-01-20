"""
This file contains utility functions for the engine.
"""
import os
import uuid


def create_file_cache_path(cache_folder: str = ".surmlcache") -> os.path:
    """
    Creates a file cache path for the model (creating the file cache if not there).

    :return: the path to the cache created with a unique id to prevent collisions.
    """
    if not os.path.exists(cache_folder):
        os.makedirs(cache_folder)
    unique_id = str(uuid.uuid4())
    file_name = f"{unique_id}.surml"
    return os.path.join(cache_folder, file_name)


class TensorflowCache:
    """
    A class to create a cache for tensorflow models.

    Attributes:
        cache_path: The path to the cache created with a unique id to prevent collisions.
    """
    def __init__(self) -> None:
        create_file_cache_path()
        self.cache_path = os.path.join(".surmlcache", "tensorflow")
        create_file_cache_path(cache_folder=self.cache_path)

    @property
    def new_cache_path(self) -> str:
        return str(os.path.join(self.cache_path, str(uuid.uuid4())))
