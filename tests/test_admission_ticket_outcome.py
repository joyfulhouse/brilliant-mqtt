"""First native outcomes are stored before synchronous listener delivery."""

from brilliant_mqtt.model import CaptureProvenance
from brilliant_mqtt.write_admission import AdmissionTicket


def test_first_outcome_replays_once_per_registration_and_clear() -> None:
    ticket = AdmissionTicket()
    first = RuntimeError("first")
    ticket.mark_settled(first)
    ticket.mark_settled("later")
    assert ticket.settled
    assert ticket.outcome is first
    assert ticket.accepted_at is None
    seen: list[str | BaseException] = []
    ticket.set_settle_callback(seen.append)
    assert seen == [first]
    ticket.clear_settle_callback()
    ticket.mark_settled("ignored")
    assert seen == [first]
    ticket.set_settle_callback(seen.append)
    assert seen == [first, first]


def test_return_provenance_precedes_listener_and_is_first_outcome_owned() -> None:
    ticket = AdmissionTicket()
    boundary = CaptureProvenance(3, 17)
    seen: list[object] = []
    ticket.set_settle_callback(lambda outcome: seen.append((outcome, ticket.accepted_at)))
    ticket.mark_settled("'ok'", boundary)
    ticket.mark_settled(RuntimeError(), CaptureProvenance(3, 18))
    assert seen == [("'ok'", boundary)]
    assert ticket.returned_at == boundary
    assert ticket.accepted_at == boundary


def test_finished_listener_is_removed_and_explicit_lifecycle_clear_is_complete() -> None:
    ticket = AdmissionTicket()
    seen: list[str | BaseException] = []
    ticket.set_settle_callback(seen.append)
    ticket.mark_settled("receipt")
    assert ticket._on_settled is None
    ticket.set_issue_callback(lambda _boundary: None)
    ticket.clear_callbacks()
    assert ticket._on_issued is None
    assert ticket._on_refused is None
    assert ticket._on_settled is None
    assert ticket.outcome == "receipt"
