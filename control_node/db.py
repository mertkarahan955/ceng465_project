from pymongo import MongoClient, ReadPreference
import config

_primary_client = None
_secondary_client = None


def get_primary():
    global _primary_client
    if _primary_client is None:
        _primary_client = MongoClient(config.PRIMARY_URI)
    return _primary_client[config.DATABASE]


def get_secondary():
    global _secondary_client
    if _secondary_client is None:
        _secondary_client = MongoClient(
            config.SECONDARY_URI,
            readPreference="secondary"
        )
    return _secondary_client[config.DATABASE]
