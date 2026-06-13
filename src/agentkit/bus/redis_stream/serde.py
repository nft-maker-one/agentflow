"""Envelope (de)serialization for the Redis Streams bus.

Same JSON-via-orjson format as the Kafka adapter; kept local so the
redis adapter never imports the kafka package (which would pull in
aiokafka).
"""

from __future__ import annotations

import orjson
from pydantic import ValidationError as PydanticValidationError

from agentkit.common.errors import ValidationError
from agentkit.models.envelope import Envelope


def encode(envelope: Envelope) -> bytes:
    """Serialize an Envelope to bytes for the stream value field."""
    return orjson.dumps(envelope.model_dump(mode="json", by_alias=True))


def decode(data: bytes) -> Envelope:
    """Deserialize stream value bytes back to an Envelope."""
    try:
        obj = orjson.loads(data)
    except orjson.JSONDecodeError as e:
        raise ValidationError(f"envelope JSON decode failed: {e}") from e
    try:
        return Envelope.model_validate(obj)
    except PydanticValidationError as e:
        raise ValidationError(f"envelope schema validation failed: {e}") from e
