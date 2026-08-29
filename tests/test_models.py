from wikistream.models import WikimediaChange


def sample_raw_event() -> dict:
    return {
        "meta": {
            "id": "abc-123",
            "dt": "2026-08-29T10:00:00Z",
            "domain": "en.wikipedia.org",
        },
        "wiki": "enwiki",
        "type": "edit",
        "title": "Apache Kafka",
        "namespace": 0,
        "user": "ExampleUser",
        "bot": False,
        "minor": True,
        "comment": "Update section",
        "length": {"old": 1000, "new": 1125},
        "revision": {"old": 10, "new": 11},
    }


def test_normalises_event_and_calculates_length_delta() -> None:
    event = WikimediaChange.from_raw(sample_raw_event())

    assert event.event_id == "abc-123"
    assert event.domain == "en.wikipedia.org"
    assert event.title == "Apache Kafka"
    assert event.length_delta == 125
    assert event.new_revision == 11


def test_partition_key_is_stable_per_page() -> None:
    event = WikimediaChange.from_raw(sample_raw_event())

    assert event.partition_key == "enwiki:Apache Kafka"
