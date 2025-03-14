import os
from functools import lru_cache
from async_substrate_interface import SubstrateInterface
import threading

thread_local = threading.local()


def get_substrate_client():
    if not hasattr(thread_local, "client"):
        url = os.getenv("SUBSTRATE_ARCHIVE_NODE_URL")
        thread_local.client = SubstrateInterface(url)
    return thread_local.client


def reconnect_substrate():
    print("Reconnecting Substrate...")
    if hasattr(thread_local, "client"):
        del thread_local.client
    get_substrate_client()


@lru_cache
def create_storage_key_cached(pallet, storage, args):
    return get_substrate_client().create_storage_key(pallet, storage, list(args))
