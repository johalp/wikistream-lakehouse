from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class WikimediaChange(BaseModel):
    """Stable event contract emitted to Kafka by this project."""

    model_config = ConfigDict(extra="ignore")

    event_id: str
    event_time: str | None = None
    domain: str
    wiki: str | None = None
    change_type: str | None = None
    title: str
    namespace: int | None = None
    user: str | None = None
    bot: bool = False
    minor: bool = False
    comment: str | None = None
    old_length: int | None = None
    new_length: int | None = None
    length_delta: int | None = None
    old_revision: int | None = None
    new_revision: int | None = None

    @classmethod
    def from_raw(cls, raw: dict[str, Any]) -> WikimediaChange:
        meta = raw.get("meta") or {}
        lengths = raw.get("length") or {}
        revisions = raw.get("revision") or {}

        old_length = lengths.get("old")
        new_length = lengths.get("new")
        length_delta = None
        if isinstance(old_length, int) and isinstance(new_length, int):
            length_delta = new_length - old_length

        return cls(
            event_id=str(meta.get("id") or raw.get("id") or "unknown"),
            event_time=meta.get("dt"),
            domain=str(meta.get("domain") or raw.get("server_name") or "unknown"),
            wiki=raw.get("wiki"),
            change_type=raw.get("type"),
            title=str(raw.get("title") or "unknown"),
            namespace=raw.get("namespace"),
            user=raw.get("user"),
            bot=bool(raw.get("bot", False)),
            minor=bool(raw.get("minor", False)),
            comment=raw.get("comment"),
            old_length=old_length,
            new_length=new_length,
            length_delta=length_delta,
            old_revision=revisions.get("old"),
            new_revision=revisions.get("new"),
        )

    @property
    def partition_key(self) -> str:
        """Keep edits to the same wiki/page on the same Kafka partition."""
        return f"{self.wiki or self.domain}:{self.title}"
