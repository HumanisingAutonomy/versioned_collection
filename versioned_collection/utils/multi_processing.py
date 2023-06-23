from multiprocessing import cpu_count
from typing import List, Any, Optional


def get_chunk_size(
    lst: List[Any],
    proportion_of_available_cpus: float = 1,
) -> int:
    if proportion_of_available_cpus < 0 or proportion_of_available_cpus > 1:
        raise ValueError("proportion_of_available_cpus should be in [0, 1]")
    chunk_size = int(len(lst) // (proportion_of_available_cpus * cpu_count()))
    if chunk_size == 0:
        chunk_size = len(lst)
    return chunk_size


def chunk_list(
    lst: List[Any],
    chunk_size: Optional[int] = None,
) -> List[List[Any]]:
    chunk_size = chunk_size if chunk_size is not None else get_chunk_size(lst)
    return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]
