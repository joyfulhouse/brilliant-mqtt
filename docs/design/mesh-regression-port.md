# PR #232 Regression Port

This is the test-only Phase B baseline on production main
`376f745ae1273bd4b1312a01eeba41066470285b`. No production file or mirrored
payload is changed. Historical committed sources are test files at
`74f97a4aa7769276192c1f6f9096947b0f6fc33c`, relative to
`0b329e48a2592ac519ec4f21130fd7ad3aab8461`. External sources are the four
read-only S4 files named in the mapping. Every one of the authoritative
inventory's **61 committed IDs plus 14 external IDs (75/75)** maps below;
overlapping historical IDs remain individually listed. Multiple IDs can share
one stronger production scenario; no old distributed `MeshPending` runtime or
fake reducer is introduced.

## Transport And Diagnostic Adaptations

- `tests/mesh_regression_support.py` records transport inputs and reads actual
  bridge state. It neither settles a ticket nor decides state/expiry. Fake
  clocks, native gates and MQTT gates are controlled explicitly by each test.
- Real `RpcBusAdapter` plus `Bridge` run behind native/MQTT fakes. Captures come
  from actual `get_all()` or the real `_make_observer_class` notification
  handler. Notification envelopes contain `updated_device`,
  `modified_peripherals`, `modified_variables` and deletions; target reports
  are explicit. Main currently discards that mask. Future integration must
  pass the reliable mask to ingress, adapting the dispatch signature as needed;
  whole-mirror presence must not become new target evidence.
- Native issue/receipt/lock ordering is production behavior. The fixture wraps
  `set_variables`/`_run_write` only to record actual ticket identities (including
  tickets internally allocated by the adapter) and records fake native payloads.
  It supplies the real command-admission context as the MQTT lane does.
- Main's `_pending_mesh`, `_mesh_confirm_tasks`, `_mesh_feedback` and publication
  handles are read-only diagnostic seams for identity and cleanup assertions.
  Runtime integration may re-point diagnostics to owner views; it must retain
  wire, cancellation, fence, ownership and cleanup assertions. Old unit hooks
  map to cancel-before-handoff and stale-effect ownership checks, not a second
  mutator. Read-only views are checked against the actual future owner.
- `SettlementTicket` is a typing Protocol cast only. It does not implement the
  missing methods. Eleven ticket tests and one owner-view test expose API gaps;
  those are not counted as semantic reproductions.
- The approved shutdown adaptation replaces only sleeping-task liveness with
  timer/worker cleanup. Both an expiry delivered after shutdown and an expiry
  already queued before shutdown are exercised; neither can publish.
- Ordinary caller timeout remains unresolved. Actual native timeout or native
  cancellation is separately exercised with four outcomes, preserving the
  original deadline and requiring the real return fence. Worker abandonment is
  not synthesized by canceling a live native task; the approved abandonment
  schedules remain additional implementation-phase tests.

## Historical Mapping

All committed rows refer to historical head `74f97a4aa7769276192c1f6f9096947b0f6fc33c`.
`PASS` means the unchanged production baseline already satisfies that case;
`BEHAVIOR RED` is an actual state/wire/receipt/evidence assertion;
`CLEANUP RED` is observable leaked ownership/registry cleanup; `API GAP` means
only the future boundary is missing. Counts in each row include parameters and
may overlap other rows.

| Original path / test ID | Ported path / test ID | Seam / retained assertion | Baseline |
| --- | --- | --- | --- |
| `tests/test_bridge.py::TestMeshConfirmedWrites::test_stale_write_completion_does_not_resurrect_pending` | `tests/test_bridge.py::TestMeshConfirmedWrites::test_stale_write_completion_does_not_resurrect_pending` | Existing test retained, with historical capture at t=75 then expiry t=80; stale completion cannot install a timer. | 1 PASS |
| `tests/test_bridge.py::TestMeshConfirmedWrites::test_completed_resolver_clears_its_registry_entry` | `tests/test_bridge.py::TestMeshConfirmedWrites::test_completed_resolver_clears_its_registry_entry` | Existing main identity/cleanup assertions retained; no obsolete MeshPending name introduced. | 1 PASS |
| `tests/test_bridge.py::TestMeshWriteTimeout::test_pre_acceptance_contradiction_is_ignored` | `tests/test_mesh_settlement_regressions.py::test_pre_acceptance_contradiction_is_ignored` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_bridge.py::TestMeshWriteTimeout::test_fast_native_timeout_is_contradicted_within_one_poll` | `tests/test_mesh_settlement_regressions.py::test_fast_native_timeout_is_contradicted_within_one_poll` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 PASS |
| `tests/test_bridge.py::TestMeshWriteTimeout::test_late_failure_publishes_failed_with_observed_state` | `tests/test_mesh_settlement_regressions.py::test_late_failure_publishes_failed_with_observed_state` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_bridge.py::TestMeshWriteTimeout::test_late_acceptance_rearms_then_is_contradicted` | `tests/test_mesh_settlement_regressions.py::test_late_acceptance_rearms_then_is_contradicted` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_bridge.py::TestMeshWriteTimeout::test_rearm_publish_stays_cancellable` | `tests/test_mesh_settlement_regressions.py::test_adapter_rearm_publish_stays_cancellable` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 2 BEHAVIOR RED |
| `tests/test_bridge.py::TestMeshWriteTimeout::test_late_acceptance_then_matching_value_confirms` | `tests/test_mesh_settlement_regressions.py::test_late_acceptance_then_matching_value_confirms` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_bridge.py::TestMeshWriteTimeout::test_late_timeout_or_cancellation_resumes_contradiction` | `tests/test_mesh_settlement_regressions.py::test_late_timeout_or_cancellation_resumes_contradiction` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 4 PASS |
| `tests/test_bridge.py::TestMeshWriteTimeout::test_superseded_write_late_outcome_is_ignored` | `tests/test_mesh_settlement_regressions.py::test_superseded_write_late_outcome_is_ignored` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 2 PASS |
| `tests/test_bridge.py::TestMeshWriteTimeout::test_abandoned_reused_ticket_never_settles_the_reissue` | `tests/test_mesh_settlement_regressions.py::test_abandoned_reused_ticket_never_settles_the_reissue` | Actual production TicketAdoptionError rejects reused ticket; fresh native request stays unresolved, ignores prior association, then only fresh rejection publishes failed. Both orderings; serial native payloads asserted. | 2 BEHAVIOR RED |
| `tests/test_bridge.py::TestMeshWriteTimeout::test_unresolved_write_without_observations_closes_unconfirmed` | `tests/test_mesh_settlement_regressions.py::test_unresolved_write_without_observations_closes_unconfirmed` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 PASS |
| `tests/test_bridge.py::TestMeshWriteTimeout::test_settle_task_losing_ownership_clears_its_registry_entry` | `tests/test_mesh_settlement_regressions.py::test_settle_task_losing_ownership_clears_its_registry_entry` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 CLEANUP RED |
| `tests/test_bridge.py::TestMeshWriteTimeout::test_resolver_losing_ownership_releases_its_entry` | `tests/test_mesh_settlement_regressions.py::test_resolver_losing_ownership_releases_its_entry` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 CLEANUP RED |
| `tests/test_bridge.py::TestMeshWriteTimeout::test_matching_observation_holds_null_until_confirm_window` | `tests/test_mesh_settlement_regressions.py::test_matching_observation_holds_null_until_confirm_window[native_timeout]`; `tests/test_mesh_settlement_regressions.py::test_inconclusive_outcome_uses_return_fence_and_original_expiry` | Actual settled native timeout differs from caller timeout; original deadline and post-return evidence preserved for four inconclusive outcomes. | 4 BEHAVIOR RED, 5 PASS |
| `tests/test_bridge.py::TestMeshWriteTimeout::test_unresolved_match_cannot_confirm_at_expiry` | `tests/test_mesh_settlement_regressions.py::test_matching_observation_holds_null_until_confirm_window[caller_timeout]` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_bus_adapter.py::TestInteractiveScheduling::test_cancelled_lane_revokes_folded_waiter_but_retains_issued_rpc` | `tests/test_bus_adapter.py::TestInteractiveScheduling::test_cancelled_lane_revokes_folded_waiter_but_retains_issued_rpc` | Existing production lane schedule retained, including issued-RPC lock ownership and empty pending registry. | 2 PASS |
| `tests/test_bus_adapter.py::TestInteractiveScheduling::test_ingress_only_newest_payload_issues_after_gate_opens` | `tests/test_bus_adapter.py::TestInteractiveScheduling::test_ingress_only_newest_payload_issues_after_gate_opens` | Existing native payload/concurrency/timer assertions retained; adapter and bridge share deterministic clock. | 4 PASS |
| `tests/test_bus_adapter.py::TestDetachedWrites::test_late_completion_reaches_the_settle_callback` | `tests/test_mesh_ticket_regressions.py::test_late_completion_reaches_the_settle_callback` | Existing assertions retained; ticket-only contracts use the declared future API without supplying behavior. | 1 API GAP |
| `tests/test_bus_adapter.py::TestDetachedWrites::test_late_failure_reaches_the_settle_callback` | `tests/test_mesh_ticket_regressions.py::test_late_failure_reaches_the_settle_callback` | Existing assertions retained; ticket-only contracts use the declared future API without supplying behavior. | 1 API GAP |
| `tests/test_bus_adapter.py::TestDetachedWrites::test_shutdown_of_detached_write_settles_write_cancelled` | `tests/test_mesh_ticket_regressions.py::test_shutdown_of_detached_write_settles_write_cancelled` | Existing assertions retained; ticket-only contracts use the declared future API without supplying behavior. | 1 API GAP |
| `tests/test_bus_adapter.py::TestDetachedWrites::test_failing_settle_callback_is_contained` | `tests/test_mesh_ticket_regressions.py::test_failing_settle_callback_is_contained` | Existing assertions retained; ticket-only contracts use the declared future API without supplying behavior. | 1 API GAP |
| `tests/test_mesh_feedback.py::test_nonfinite_wall_clock_cannot_supply_a_confirmation_deadline` | `tests/test_mesh_feedback.py::test_nonfinite_wall_clock_cannot_supply_a_confirmation_deadline` | Existing main test and all substantive assertions retained; deterministic clock seam where changed. | 3 PASS |
| `tests/test_mesh_feedback.py::test_shutdown_fences_sleeping_resolver_without_sweeping_it` | `tests/test_mesh_feedback.py::test_shutdown_fences_sleeping_resolver_without_sweeping_it`; `tests/test_mesh_settlement_regressions.py::test_shutdown_fences_obsolete_expiry_and_cleans_workers` | Only historical sleeping-task-not-done assertion replaced by timer/worker cleanup. Post-shutdown expiry, already-ready expiry and zero publications retained. | 3 CLEANUP RED |
| `tests/test_mesh_feedback.py::test_contradiction_revokes_blocked_publish_with_same_generation` | `tests/test_mesh_feedback.py::test_contradiction_revokes_blocked_publish_with_same_generation` | Existing main test and all substantive assertions retained; deterministic clock seam where changed. | 1 PASS |
| `tests/test_mesh_feedback.py::test_terminal_publish_repairs_cancelled_pending_already_at_broker` | `tests/test_mesh_feedback.py::test_terminal_publish_repairs_cancelled_pending_already_at_broker` | Existing main test and all substantive assertions retained; deterministic clock seam where changed. | 2 PASS |
| `tests/test_mesh_feedback.py::test_detached_rpc_late_outcome_settles_only_its_own_pending` | `tests/test_mesh_settlement_regressions.py::test_detached_rpc_late_outcome_settles_only_its_own_pending` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 2 BEHAVIOR RED, 2 PASS |
| `tests/test_mesh_pending.py::test_hook_runs_before_every_feedback_change` | `tests/test_mesh_feedback.py::test_contradiction_revokes_blocked_publish_with_same_generation`; `tests/test_mesh_settlement_regressions.py::test_adapter_rearm_publish_stays_cancellable` | The callback representation becomes synchronous publication revocation: canceled before gate release on contradiction/rearm; no old request on wire. | 2 BEHAVIOR RED, 1 PASS |
| `tests/test_mesh_pending.py::test_begin_supersedes_and_stale_generation_cannot_arm` | `tests/test_mesh_settlement_regressions.py::test_superseded_write_late_outcome_is_ignored`; `tests/test_mesh_settlement_regressions.py::test_adapter_rearm_publish_stays_cancellable[supersede]` | Old completion cannot arm a successor; one live successor timer/target; old timer retires before gate release. | 1 BEHAVIOR RED, 2 PASS |
| `tests/test_mesh_pending.py::test_views_are_read_only` | `tests/test_mesh_settlement_regressions.py::test_views_are_read_only` | Actual future owner diagnostic views must reject mutation; no test-owned immutable copy. API-only gap on main. | 1 API GAP |
| `tests/test_mesh_pending.py::test_unresolved_rpc_ignores_a_differing_observation` | `tests/test_mesh_settlement_regressions.py::test_pre_acceptance_contradiction_is_ignored` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_pending.py::test_late_acceptance_rearms_a_full_window` | `tests/test_mesh_settlement_regressions.py::test_late_acceptance_rearms_then_is_contradicted` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_pending.py::test_late_failure_publishes_failed_forced` | `tests/test_mesh_settlement_regressions.py::test_late_failure_publishes_failed_with_observed_state` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_pending.py::test_inconclusive_outcome_only_clears_the_flag` | `tests/test_mesh_settlement_regressions.py::test_late_timeout_or_cancellation_resumes_contradiction`; `tests/test_mesh_settlement_regressions.py::test_inconclusive_outcome_uses_return_fence_and_original_expiry` | Four actual native outcomes keep original timer/deadline, use native-return fence, permit later contradiction/fresh expiry confirmation; no acceptance rearm. | 4 BEHAVIOR RED, 8 PASS |
| `tests/test_mesh_pending.py::test_outcome_for_a_resolved_or_foreign_record_is_ignored` | `tests/test_write_admission.py::test_mark_settled_keeps_only_the_first_outcome`; `tests/test_mesh_settlement_regressions.py::test_superseded_write_late_outcome_is_ignored`; `tests/test_mesh_settlement_regressions.py::test_abandoned_reused_ticket_never_settles_the_reissue` | First ticket outcome wins; resolved duplicate cannot redeliver; foreign/superseded/rejected-reuse tickets cannot settle the successor. No invented old PendingMeshWrite. | 1 API GAP, 2 BEHAVIOR RED, 2 PASS |
| `tests/test_mesh_pending.py::test_resolver_confirms_with_a_fresh_match` | `tests/test_mesh_settlement_regressions.py::test_late_acceptance_then_matching_value_confirms` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_pending.py::test_resolver_without_fresh_match_closes_unconfirmed` | `tests/test_mesh_settlement_regressions.py::test_unresolved_write_without_observations_closes_unconfirmed` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 PASS |
| `tests/test_mesh_pending.py::test_revoke_feedback_fences_and_resolver_releases_its_entry` | `tests/test_mesh_settlement_regressions.py::test_shutdown_fences_obsolete_expiry_and_cleans_workers`; `tests/test_mesh_settlement_regressions.py::test_acquisition_push_cannot_skip_discovery_and_command_route` | Revocation rejects sleeping/already-ready expiry, cleans worker/timer registries and has zero wire effects; reacquisition restores routing. | 2 CLEANUP RED, 1 PASS |
| `tests/test_mesh_settlement_runtime.py::test_pre_acceptance_snapshot_is_not_contradiction` | `tests/test_mesh_settlement_regressions.py::test_pre_acceptance_snapshot_is_not_contradiction` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_fresh_contradiction_after_native_return_is_not_lost` | `tests/test_mesh_settlement_regressions.py::test_fresh_contradiction_after_native_return_is_not_lost` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 PASS |
| `tests/test_mesh_settlement_runtime.py::test_expiry_does_not_wait_for_rearm_publish` | `tests/test_mesh_settlement_regressions.py::test_expiry_does_not_wait_for_rearm_publish` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_pre_acceptance_match_delivered_near_expiry_cannot_confirm` | `tests/test_mesh_settlement_regressions.py::test_pre_acceptance_match_delivered_near_expiry_cannot_confirm` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_adapter_rearm_publish_stays_cancellable` | `tests/test_mesh_settlement_regressions.py::test_adapter_rearm_publish_stays_cancellable` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 2 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_pre_acceptance_push_delayed_until_after_acceptance` | `tests/test_mesh_settlement_regressions.py::test_pre_acceptance_push_delayed_until_after_acceptance` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_matching_push_at_expiry_cannot_confirm` | `tests/test_mesh_settlement_regressions.py::test_matching_push_at_expiry_cannot_confirm` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_reconnect_new_generation_cannot_echo_old_matching_off` | `tests/test_mesh_settlement_regressions.py::test_reconnect_new_generation_cannot_echo_old_matching_off` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 PASS |
| `tests/test_mesh_settlement_runtime.py::test_fresh_contradiction_at_expiry_cannot_be_overwritten` | `tests/test_mesh_settlement_regressions.py::test_fresh_contradiction_at_expiry_cannot_be_overwritten` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 2 PASS |
| `tests/test_mesh_settlement_runtime.py::test_early_post_acceptance_capture_delivered_at_79_is_stale_at_80` | `tests/test_mesh_settlement_regressions.py::test_early_post_acceptance_capture_delivered_at_79_is_stale_at_80` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_native_acceptance_before_caller_timeout_preserves_receipt_and_fence` | `tests/test_mesh_settlement_regressions.py::test_native_acceptance_before_caller_timeout_preserves_receipt_and_fence` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_older_same_generation_capture_cannot_replace_newer_evidence` | `tests/test_mesh_settlement_regressions.py::test_older_same_generation_capture_cannot_replace_newer_evidence` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 2 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_normal_success_preserves_native_receipt_and_acceptance_fence` | `tests/test_mesh_settlement_regressions.py::test_normal_success_preserves_native_receipt_and_acceptance_fence` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_older_source_generation_capture_cannot_replace_newer_evidence` | `tests/test_mesh_settlement_regressions.py::test_older_source_generation_capture_cannot_replace_newer_evidence` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_complete_capture_missing_on_at_expiry_cannot_echo_old_match` | `tests/test_mesh_settlement_regressions.py::test_complete_capture_missing_on_at_expiry_cannot_echo_old_match` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_old_source_capture_cannot_contradict_new_acceptance` | `tests/test_mesh_settlement_regressions.py::test_old_source_capture_cannot_contradict_new_acceptance` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_write_admission.py::test_mark_settled_keeps_only_the_first_outcome` | `tests/test_write_admission.py::test_mark_settled_keeps_only_the_first_outcome` | Existing assertions retained; ticket-only contracts use the declared future API without supplying behavior. | 1 API GAP |
| `tests/test_write_admission.py::test_settle_callback_replays_immediately_on_settled_ticket` | `tests/test_write_admission.py::test_settle_callback_replays_immediately_on_settled_ticket` | Existing assertions retained; ticket-only contracts use the declared future API without supplying behavior. | 1 API GAP |
| `tests/test_write_admission.py::test_settle_callback_replay_failure_is_contained` | `tests/test_write_admission.py::test_settle_callback_replay_failure_is_contained` | Existing assertions retained; ticket-only contracts use the declared future API without supplying behavior. | 1 API GAP |
| `tests/test_write_admission.py::test_clear_settle_callback_stops_delivery` | `tests/test_write_admission.py::test_clear_settle_callback_stops_delivery` | Existing assertions retained; ticket-only contracts use the declared future API without supplying behavior. | 1 API GAP |
| `tests/test_write_admission.py::test_cancel_waiting_before_issue_settles_write_cancelled` | `tests/test_write_admission.py::test_cancel_waiting_before_issue_settles_write_cancelled` | Existing assertions retained; ticket-only contracts use the declared future API without supplying behavior. | 1 API GAP |
| `tests/test_write_admission.py::test_fast_success_settles_before_set_variables_returns` | `tests/test_write_admission.py::test_fast_success_settles_before_set_variables_returns` | Existing assertions retained; ticket-only contracts use the declared future API without supplying behavior. | 1 API GAP |
| `tests/test_write_admission.py::test_superseded_shared_ticket_settles_once_with_replacement_receipt` | `tests/test_write_admission.py::test_superseded_shared_ticket_settles_once_with_replacement_receipt` | Existing assertions retained; ticket-only contracts use the declared future API without supplying behavior. | 1 API GAP |
| `S4/test_review232_order.py::test_snapshot_captured_before_acceptance_is_not_contradiction` | `tests/test_mesh_settlement_regressions.py::test_pre_acceptance_snapshot_is_not_contradiction` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `S4/test_review232_order.py::test_fresh_contradiction_just_after_admission_settled_is_not_lost` | `tests/test_mesh_settlement_regressions.py::test_fresh_contradiction_after_native_return_is_not_lost` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 PASS |
| `S4/test_review232_order.py::test_resolver_expires_on_advertised_deadline_even_if_rearm_publish_slow` | `tests/test_mesh_settlement_regressions.py::test_expiry_does_not_wait_for_rearm_publish` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `S4/test_review232_r2.py::test_reconnect_does_not_confirm_against_new_native_state` | `tests/test_mesh_settlement_regressions.py::test_reconnect_new_generation_cannot_echo_old_matching_off` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 PASS |
| `S4/test_review232_r2.py::test_deadline_push_does_not_get_overwritten_by_old_match` | `tests/test_mesh_settlement_regressions.py::test_fresh_contradiction_at_expiry_cannot_be_overwritten[after_observation]` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 PASS |
| `S4/test_review232_r2.py::test_old_post_acceptance_capture_cannot_refresh_at_late_delivery` | `tests/test_mesh_settlement_regressions.py::test_early_post_acceptance_capture_delivered_at_79_is_stale_at_80` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `S4/test_review232_r2.py::test_known_acceptance_at_caller_timeout_does_not_lose_fence` | `tests/test_mesh_settlement_regressions.py::test_native_acceptance_before_caller_timeout_preserves_receipt_and_fence` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `S4/test_review232_r3.py::test_acquisition_push_cannot_skip_discovery_and_command_route` | `tests/test_mesh_settlement_regressions.py::test_acquisition_push_cannot_skip_discovery_and_command_route` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 PASS |
| `S4/test_review232_r3.py::test_reconcile_must_not_republish_old_snapshot_after_newer_contradiction` | `tests/test_mesh_settlement_regressions.py::test_reconcile_must_not_republish_old_snapshot_after_newer_contradiction` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `S4/test_review232_r3.py::test_reconnect_cannot_confirm_while_reconciliation_is_blocked` | `tests/test_mesh_settlement_regressions.py::test_reconnect_cannot_confirm_while_reconciliation_is_blocked` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `S4/test_review232_r3.py::test_native_success_cannot_lose_post_return_push_before_waiter_arms` | `tests/test_mesh_settlement_regressions.py::test_native_success_cannot_lose_post_return_push_before_waiter_arms` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `S4/test_review232_runtime.py::test_pre_acceptance_snapshot_is_not_contradiction` | `tests/test_mesh_settlement_regressions.py::test_pre_acceptance_snapshot_is_not_contradiction` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `S4/test_review232_runtime.py::test_fresh_contradiction_dispatched_after_native_return_is_not_lost` | `tests/test_mesh_settlement_regressions.py::test_fresh_contradiction_after_native_return_is_not_lost` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 PASS |
| `S4/test_review232_runtime.py::test_expiry_does_not_wait_for_rearm_publish` | `tests/test_mesh_settlement_regressions.py::test_expiry_does_not_wait_for_rearm_publish` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |

## Recorded Baseline

Python 3.10.20, `uv`, and `umask 022`; no full local suite, panels, live
services, private configuration, production implementation or payload edits.
Focused run: **148 cases: 100 PASS, 31 BEHAVIOR RED, 5 CLEANUP RED,
12 API GAP**. New files/admission additions contribute 67 parameter cases;
the run also includes 81 existing focused cases, with only the approved
shutdown cleanup expectation becoming red among those existing cases.
Collection succeeds, with zero skips/xfails and no import/collection failures.
The focused files `test_mesh_feedback.py` and `test_write_admission.py` had
73 passing tests before the port (coordinator's baseline); their final run has
72 existing passes, one intentional shutdown cleanup failure and seven new
API-gap failures.

Exact logs are under
`/data/projects/joyfulhouse/homeassistant-dev/brilliant-triage-2026-11-23/S9/`:
`phase-b-baseline-focused.log`, `phase-b-baseline.xml`, `phase-b-ruff.log`,
`phase-b-format.log`, `phase-b-mypy.log`. Initial exploratory runs are preserved
as `phase-b-baseline-runtime-initial.log` and `phase-b-baseline-ownership.log`;
only the final focused log/XML represents the completed port.

Commands (all run after `umask 022`):

```sh
uv run ruff check --fix tests/mesh_regression_support.py tests/test_mesh_settlement_regressions.py tests/test_mesh_ticket_regressions.py tests/test_write_admission.py tests/test_bridge.py tests/test_bus_adapter.py tests/test_mesh_feedback.py
uv run ruff format --check tests/mesh_regression_support.py tests/test_mesh_settlement_regressions.py tests/test_mesh_ticket_regressions.py tests/test_write_admission.py tests/test_bridge.py tests/test_bus_adapter.py tests/test_mesh_feedback.py
uv run mypy --strict src tests/mesh_regression_support.py tests/test_mesh_settlement_regressions.py tests/test_mesh_ticket_regressions.py tests/test_write_admission.py tests/test_bridge.py tests/test_bus_adapter.py tests/test_mesh_feedback.py
uv run pytest -q tests/test_mesh_settlement_regressions.py tests/test_mesh_ticket_regressions.py tests/test_mesh_feedback.py tests/test_write_admission.py tests/test_bridge.py::TestMeshConfirmedWrites::test_stale_write_completion_does_not_resurrect_pending tests/test_bridge.py::TestMeshConfirmedWrites::test_completed_resolver_clears_its_registry_entry tests/test_bus_adapter.py::TestInteractiveScheduling::test_cancelled_lane_revokes_folded_waiter_but_retains_issued_rpc tests/test_bus_adapter.py::TestInteractiveScheduling::test_ingress_only_newest_payload_issues_after_gate_opens --tb=short --junitxml=/data/projects/joyfulhouse/homeassistant-dev/brilliant-triage-2026-11-23/S9/phase-b-baseline.xml
```

Ruff/format pass. Strict mypy passes for 76 source files including dependency
analysis. Including `src` gives mypy the repository's typed module sources;
no missing future API is hidden by a suppression, fake method or linter bypass.
Pytest exits 1 deliberately: this is a red test-only TDD port, not green gates.

### Exact Failing Nodes

The final XML/log records all passing nodes too. These are the actual failures:

| Node ID | Baseline category |
| --- | --- |
| `tests/test_mesh_settlement_regressions.py::test_pre_acceptance_contradiction_is_ignored` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_pre_acceptance_snapshot_is_not_contradiction` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_expiry_does_not_wait_for_rearm_publish` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_late_acceptance_rearms_then_is_contradicted` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_late_acceptance_then_matching_value_confirms` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_late_failure_publishes_failed_with_observed_state` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_matching_observation_holds_null_until_confirm_window[caller_timeout]` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_detached_rpc_late_outcome_settles_only_its_own_pending[pending-receipt]` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_detached_rpc_late_outcome_settles_only_its_own_pending[pending-error]` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_adapter_rearm_publish_stays_cancellable[supersede]` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_adapter_rearm_publish_stays_cancellable[withdraw]` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_pre_acceptance_match_delivered_near_expiry_cannot_confirm` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_pre_acceptance_push_delayed_until_after_acceptance` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_matching_push_at_expiry_cannot_confirm` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_early_post_acceptance_capture_delivered_at_79_is_stale_at_80` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_older_same_generation_capture_cannot_replace_newer_evidence[0-1]` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_older_same_generation_capture_cannot_replace_newer_evidence[1-0]` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_older_source_generation_capture_cannot_replace_newer_evidence` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_complete_capture_missing_on_at_expiry_cannot_echo_old_match` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_old_source_capture_cannot_contradict_new_acceptance` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_normal_success_preserves_native_receipt_and_acceptance_fence` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_native_acceptance_before_caller_timeout_preserves_receipt_and_fence` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_shutdown_fences_obsolete_expiry_and_cleans_workers[sleeping]` | CLEANUP RED |
| `tests/test_mesh_settlement_regressions.py::test_shutdown_fences_obsolete_expiry_and_cleans_workers[expiry_already_ready]` | CLEANUP RED |
| `tests/test_mesh_settlement_regressions.py::test_resolver_losing_ownership_releases_its_entry` | CLEANUP RED |
| `tests/test_mesh_settlement_regressions.py::test_settle_task_losing_ownership_clears_its_registry_entry` | CLEANUP RED |
| `tests/test_mesh_settlement_regressions.py::test_views_are_read_only` | API GAP |
| `tests/test_mesh_settlement_regressions.py::test_reconcile_must_not_republish_old_snapshot_after_newer_contradiction` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_reconnect_cannot_confirm_while_reconciliation_is_blocked` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_native_success_cannot_lose_post_return_push_before_waiter_arms` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_abandoned_reused_ticket_never_settles_the_reissue[before]` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_abandoned_reused_ticket_never_settles_the_reissue[after]` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_inconclusive_outcome_uses_return_fence_and_original_expiry[pre_return_match-asyncio_timeout]` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_inconclusive_outcome_uses_return_fence_and_original_expiry[pre_return_match-native_timeout]` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_inconclusive_outcome_uses_return_fence_and_original_expiry[pre_return_match-cancelled]` | BEHAVIOR RED |
| `tests/test_mesh_settlement_regressions.py::test_inconclusive_outcome_uses_return_fence_and_original_expiry[pre_return_match-write_cancelled]` | BEHAVIOR RED |
| `tests/test_mesh_ticket_regressions.py::test_late_completion_reaches_the_settle_callback` | API GAP |
| `tests/test_mesh_ticket_regressions.py::test_late_failure_reaches_the_settle_callback` | API GAP |
| `tests/test_mesh_ticket_regressions.py::test_shutdown_of_detached_write_settles_write_cancelled` | API GAP |
| `tests/test_mesh_ticket_regressions.py::test_failing_settle_callback_is_contained` | API GAP |
| `tests/test_mesh_feedback.py::test_shutdown_fences_sleeping_resolver_without_sweeping_it` | CLEANUP RED |
| `tests/test_write_admission.py::test_mark_settled_keeps_only_the_first_outcome` | API GAP |
| `tests/test_write_admission.py::test_settle_callback_replays_immediately_on_settled_ticket` | API GAP |
| `tests/test_write_admission.py::test_settle_callback_replay_failure_is_contained` | API GAP |
| `tests/test_write_admission.py::test_clear_settle_callback_stops_delivery` | API GAP |
| `tests/test_write_admission.py::test_cancel_waiting_before_issue_settles_write_cancelled` | API GAP |
| `tests/test_write_admission.py::test_fast_success_settles_before_set_variables_returns` | API GAP |
| `tests/test_write_admission.py::test_superseded_shared_ticket_settles_once_with_replacement_receipt` | API GAP |

## Integration Review Notes

- Some unchanged main tests encode the old no-late-settlement contract, including
  `test_detached_rpc_outcome_cannot_settle_or_rearm_feedback`, and old FakeBus
  timeout tests conflate caller timeout with settled native timeout. They remain
  present and their historical assertions have not been silently weakened.
  Runtime integration must explicitly reconcile those obsolete fixture/contract
  expectations with the approved design and this manifest, preserving native
  timeout confirmation and unresolved caller-timeout coverage separately.
- Existing FakeBus mesh observations intentionally have no mesh provenance.
  Integration must stamp transport captures in that test fake if those legacy
  tests require trusted mesh evidence; production must reject missing provenance.
- Several red schedules first stop at the absent late-acceptance rearm
  publication. Their downstream gate cancellation and zero-old-request wire
  assertions are committed and must also execute once rearm exists. A main
  failure before that gate proves missing rearm, not cancellation correctness.
- Source-specific mutable-record/private-generation assertions map to identity
  guarded ownership, one successor timer, explicit target, and zero obsolete
  wire effects. No test requires retaining the obsolete architecture. The
  unchanged diagnostic accessors are temporary read-only integration seams.
- Additional final design schedules remain later tests, including Astra's R5
  scale-rebinding/read-across-return scenario: start a complete read changing a
  dimmer scale before native return, deliver it after the return floor advances,
  allow admitted discovery metadata but keep primary unknown, and release unknown
  only on a genuinely later trusted primary capture. This note does not claim
  that proposed schedule has run or passed.
