"""Envelope (de)serialization for Kafka.

Format choice: **JSON via orjson**.

Why JSON over Avro / Protobuf for v0.1:

* Self-describing — easy to inspect with ``rpk topic consume``;
* No external schema registry to operate;
* ``orjson`` keeps it fast (~5x faster than stdlib ``json``).

We can switch encoding later behind this module without touching
adapter logic — that is precisely the reason to keep serde isolated.
"""

from __future__ import annotations

import orjson
from pydantic import ValidationError as PydanticValidationError

from agentkit.common.errors import ValidationError
from agentkit.models.envelope import Envelope


def encode(envelope: Envelope) -> bytes:
    """Serialize an Envelope to bytes for the Kafka value field.

    Uses Pydantic's ``mode='json'`` dump so that ``datetime`` becomes
    ISO 8601 and the ``from_`` field is correctly aliased to ``from``.
    """
    payload = envelope.model_dump(mode="json", by_alias=True)
    return orjson.dumps(payload)


def decode(data: bytes) -> Envelope:
    """Deserialize Kafka value bytes back to an Envelope.

    Raises :class:`ValidationError` on corrupt / non-conforming input.
    """
    try:
        obj = orjson.loads(data)
    except orjson.JSONDecodeError as e:
        raise ValidationError(f"envelope JSON decode failed: {e}") from e
    try:
        return Envelope.model_validate(obj)
    except PydanticValidationError as e:
        raise ValidationError(f"envelope schema validation failed: {e}") from e


def partition_key(envelope: Envelope) -> bytes:
    """Return the Kafka partition key for ``envelope``.

    Per ``Doc02 §4.2``: ``run_id`` is the default key — same Run's
    events fall on the same partition so within-Run ordering is
    preserved. Empty ``run_id`` falls back to ``event_id`` to spread
    system/debug events across partitions.
    """
    key = envelope.run_id or envelope.event_id
    return key.encode("utf-8")
