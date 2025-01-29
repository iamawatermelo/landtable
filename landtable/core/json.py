"""
Use accelerated JSON libraries where they are available.
"""

import logging
import json

from pydantic import BaseModel

logger = logging.getLogger(__name__)

try:
    import orjson
except ImportError:
    logger.warn("orjson is not available (pip install landtable[speedups])")
    orjson = None

class ModelSerializationError(Exception):
    pass

def serialize_model_bytes(
    model: BaseModel
) -> bytes:


    if orjson is not None:
        return orjson.dumps(model.dict())
    else:
        return json.dumps(model.dict()).encode("UTF-8")

def deserialize_model_bytes[T: BaseModel](
    buf: bytes,
    model: T
) -> T:
    if orjson is not None:
        deser = orjson.loads(buf)
    else:
        deser = json.loads(buf)

    return model(**deser)
