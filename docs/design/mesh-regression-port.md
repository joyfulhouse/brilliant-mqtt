# PR #232 Regression Port

This records the historical test-only Phase B baseline on production main
`376f745ae1273bd4b1312a01eeba41066470285b`. At that stage no production file or
mirrored payload was changed. The baseline outcomes, mapping and Task 4B notes
below remain historical evidence; final implementation results are appended
separately. Historical committed sources are test files at
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
| `tests/test_bridge.py::TestMeshWriteTimeout::test_superseded_write_late_outcome_is_ignored` | `tests/test_mesh_settlement_regressions.py::test_superseded_write_late_outcome_is_ignored` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. Independent old/replacement native gates. Record successor owner/generation, exact admission ticket and target before old completion; retain old device lock. Through old completion and blocked replacement issue, require zero state publications, identical successor association and no new timer. Only replacement return starts its window. | 2 PASS |
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
| `tests/test_mesh_pending.py::test_begin_supersedes_and_stale_generation_cannot_arm` | `tests/test_mesh_settlement_regressions.py::test_superseded_write_late_outcome_is_ignored`; `tests/test_mesh_settlement_regressions.py::test_adapter_rearm_publish_stays_cancellable[supersede]` | Old completion cannot arm a successor; one live successor timer/target; old timer retires before gate release. Independent old/replacement native gates. Record successor owner/generation, exact admission ticket and target before old completion; retain old device lock. Through old completion and blocked replacement issue, require zero state publications, identical successor association and no new timer. Only replacement return starts its window. | 1 BEHAVIOR RED, 2 PASS |
| `tests/test_mesh_pending.py::test_views_are_read_only` | `tests/test_mesh_settlement_regressions.py::test_views_are_read_only` | Actual future owner diagnostic views must reject mutation; no test-owned immutable copy. API-only gap on main. | 1 API GAP |
| `tests/test_mesh_pending.py::test_unresolved_rpc_ignores_a_differing_observation` | `tests/test_mesh_settlement_regressions.py::test_pre_acceptance_contradiction_is_ignored` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_pending.py::test_late_acceptance_rearms_a_full_window` | `tests/test_mesh_settlement_regressions.py::test_late_acceptance_rearms_then_is_contradicted` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_pending.py::test_late_failure_publishes_failed_forced` | `tests/test_mesh_settlement_regressions.py::test_late_failure_publishes_failed_with_observed_state` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_pending.py::test_inconclusive_outcome_only_clears_the_flag` | `tests/test_mesh_settlement_regressions.py::test_late_timeout_or_cancellation_resumes_contradiction`; `tests/test_mesh_settlement_regressions.py::test_inconclusive_outcome_uses_return_fence_and_original_expiry` | Four actual native outcomes keep original timer/deadline, use native-return fence, permit later contradiction/fresh expiry confirmation; no acceptance rearm. | 4 BEHAVIOR RED, 8 PASS |
| `tests/test_mesh_pending.py::test_outcome_for_a_resolved_or_foreign_record_is_ignored` | `tests/test_write_admission.py::test_mark_settled_keeps_only_the_first_outcome`; `tests/test_mesh_settlement_regressions.py::test_superseded_write_late_outcome_is_ignored`; `tests/test_mesh_settlement_regressions.py::test_abandoned_reused_ticket_never_settles_the_reissue` | First ticket outcome wins; resolved duplicate cannot redeliver; foreign/superseded/rejected-reuse tickets cannot settle the successor. No invented old PendingMeshWrite. Independent old/replacement native gates. Record successor owner/generation, exact admission ticket and target before old completion; retain old device lock. Through old completion and blocked replacement issue, require zero state publications, identical successor association and no new timer. Only replacement return starts its window. | 1 API GAP, 2 BEHAVIOR RED, 2 PASS |
| `tests/test_mesh_pending.py::test_resolver_confirms_with_a_fresh_match` | `tests/test_mesh_settlement_regressions.py::test_late_acceptance_then_matching_value_confirms` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_pending.py::test_resolver_without_fresh_match_closes_unconfirmed` | `tests/test_mesh_settlement_regressions.py::test_unresolved_write_without_observations_closes_unconfirmed` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 PASS |
| `tests/test_mesh_pending.py::test_revoke_feedback_fences_and_resolver_releases_its_entry` | `tests/test_mesh_settlement_regressions.py::test_shutdown_fences_obsolete_expiry_and_cleans_workers`; `tests/test_mesh_settlement_regressions.py::test_acquisition_push_cannot_skip_discovery_and_command_route` | Revocation rejects sleeping/already-ready expiry, cleans worker/timer registries and has zero wire effects; reacquisition restores routing. | 2 CLEANUP RED, 1 PASS |
| `tests/test_mesh_settlement_runtime.py::test_pre_acceptance_snapshot_is_not_contradiction` | `tests/test_mesh_settlement_regressions.py::test_pre_acceptance_snapshot_is_not_contradiction` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_fresh_contradiction_after_native_return_is_not_lost` | `tests/test_mesh_settlement_regressions.py::test_fresh_contradiction_after_native_return_is_not_lost` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. Native returned and notification captured before publication starts; publication blocks, internal contradiction must already own state before gate release, then terminal ON/contradicted reaches wire. | 1 PASS |
| `tests/test_mesh_settlement_runtime.py::test_expiry_does_not_wait_for_rearm_publish` | `tests/test_mesh_settlement_regressions.py::test_expiry_does_not_wait_for_rearm_publish` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_pre_acceptance_match_delivered_near_expiry_cannot_confirm` | `tests/test_mesh_settlement_regressions.py::test_pre_acceptance_match_delivered_near_expiry_cannot_confirm` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_adapter_rearm_publish_stays_cancellable` | `tests/test_mesh_settlement_regressions.py::test_adapter_rearm_publish_stays_cancellable` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 2 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_pre_acceptance_push_delayed_until_after_acceptance` | `tests/test_mesh_settlement_regressions.py::test_pre_acceptance_push_delayed_until_after_acceptance` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_matching_push_at_expiry_cannot_confirm` | `tests/test_mesh_settlement_regressions.py::test_matching_push_at_expiry_cannot_confirm` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_reconnect_new_generation_cannot_echo_old_matching_off` | `tests/test_mesh_settlement_regressions.py::test_reconnect_new_generation_cannot_echo_old_matching_off` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 PASS |
| `tests/test_mesh_settlement_runtime.py::test_fresh_contradiction_at_expiry_cannot_be_overwritten` | `tests/test_mesh_settlement_regressions.py::test_fresh_contradiction_at_expiry_cannot_be_overwritten` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 2 PASS |
| `tests/test_mesh_settlement_runtime.py::test_early_post_acceptance_capture_delivered_at_79_is_stale_at_80` | `tests/test_mesh_settlement_regressions.py::test_early_post_acceptance_capture_delivered_at_79_is_stale_at_80` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_native_acceptance_before_caller_timeout_preserves_receipt_and_fence` | `tests/test_mesh_settlement_regressions.py::test_native_acceptance_before_caller_timeout_preserves_receipt_and_fence` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. Snapshot ticket settled/accepted fence inside held native result-delivery callback, before caller resumption. Assert stored boundary and exact ticket/current-request fence equality after receipt/wire behavior, preserving semantic RED. | 1 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_older_same_generation_capture_cannot_replace_newer_evidence` | `tests/test_mesh_settlement_regressions.py::test_older_same_generation_capture_cannot_replace_newer_evidence` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 2 BEHAVIOR RED |
| `tests/test_mesh_settlement_runtime.py::test_normal_success_preserves_native_receipt_and_acceptance_fence` | `tests/test_mesh_settlement_regressions.py::test_normal_success_preserves_native_receipt_and_acceptance_fence` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. Positive caller deadline; exact successful ticket and owner must be resolved, with the same non-null acceptance fence; pre-write capture is same generation and older sequence. Assert after stale-capture behavior. | 1 BEHAVIOR RED |
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
| `S4/test_review232_order.py::test_fresh_contradiction_just_after_admission_settled_is_not_lost` | `tests/test_mesh_settlement_regressions.py::test_fresh_contradiction_after_native_return_is_not_lost` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. Native returned and notification captured before publication starts; publication blocks, internal contradiction must already own state before gate release, then terminal ON/contradicted reaches wire. | 1 PASS |
| `S4/test_review232_order.py::test_resolver_expires_on_advertised_deadline_even_if_rearm_publish_slow` | `tests/test_mesh_settlement_regressions.py::test_expiry_does_not_wait_for_rearm_publish` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `S4/test_review232_r2.py::test_reconnect_does_not_confirm_against_new_native_state` | `tests/test_mesh_settlement_regressions.py::test_reconnect_new_generation_cannot_echo_old_matching_off` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 PASS |
| `S4/test_review232_r2.py::test_deadline_push_does_not_get_overwritten_by_old_match` | `tests/test_mesh_settlement_regressions.py::test_fresh_contradiction_at_expiry_cannot_be_overwritten[after_observation]` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 PASS |
| `S4/test_review232_r2.py::test_old_post_acceptance_capture_cannot_refresh_at_late_delivery` | `tests/test_mesh_settlement_regressions.py::test_early_post_acceptance_capture_delivered_at_79_is_stale_at_80` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `S4/test_review232_r2.py::test_known_acceptance_at_caller_timeout_does_not_lose_fence` | `tests/test_mesh_settlement_regressions.py::test_native_acceptance_before_caller_timeout_preserves_receipt_and_fence` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. Snapshot ticket settled/accepted fence inside held native result-delivery callback, before caller resumption. Assert stored boundary and exact ticket/current-request fence equality after receipt/wire behavior, preserving semantic RED. | 1 BEHAVIOR RED |
| `S4/test_review232_r3.py::test_acquisition_push_cannot_skip_discovery_and_command_route` | `tests/test_mesh_settlement_regressions.py::test_acquisition_push_cannot_skip_discovery_and_command_route` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 PASS |
| `S4/test_review232_r3.py::test_reconcile_must_not_republish_old_snapshot_after_newer_contradiction` | `tests/test_mesh_settlement_regressions.py::test_reconcile_must_not_republish_old_snapshot_after_newer_contradiction` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `S4/test_review232_r3.py::test_reconnect_cannot_confirm_while_reconciliation_is_blocked` | `tests/test_mesh_settlement_regressions.py::test_reconnect_cannot_confirm_while_reconciliation_is_blocked` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `S4/test_review232_r3.py::test_native_success_cannot_lose_post_return_push_before_waiter_arms` | `tests/test_mesh_settlement_regressions.py::test_native_success_cannot_lose_post_return_push_before_waiter_arms` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `S4/test_review232_runtime.py::test_pre_acceptance_snapshot_is_not_contradiction` | `tests/test_mesh_settlement_regressions.py::test_pre_acceptance_snapshot_is_not_contradiction` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |
| `S4/test_review232_runtime.py::test_fresh_contradiction_dispatched_after_native_return_is_not_lost` | `tests/test_mesh_settlement_regressions.py::test_fresh_contradiction_after_native_return_is_not_lost` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. Native returned and notification captured before publication starts; publication blocks, internal contradiction must already own state before gate release, then terminal ON/contradicted reaches wire. | 1 PASS |
| `S4/test_review232_runtime.py::test_expiry_does_not_wait_for_rearm_publish` | `tests/test_mesh_settlement_regressions.py::test_expiry_does_not_wait_for_rearm_publish` | Production adapter/native and MQTT gates; wire status/primary/targets/deadline plus existing identity/cleanup assertions. | 1 BEHAVIOR RED |

## Spec Review Corrections

Independent review of `7ddf5749ed12ee2dbd373517f1476bc4bcc1d6d7` found
four substantive assertion losses. The corrected schedules retain all existing
cases and add no fake settlement behavior:

1. The post-return notification captures `(native returned, publish not started)`,
   blocks the next publication, and requires internal `contradicted` while MQTT
   is still blocked, followed by terminal ON/contradicted after release. This
   stronger historical schedule passes unchanged production main.
2. The held `_finish_admission` callback records ticket settlement and acceptance
   provenance at callback entry, while result delivery is unfinished. Missing
   fields are recorded as missing facts. The test first checks the actual receipt
   and stale-capture behavior, then asserts the saved `(True, "'ok'", False)`
   boundary and exact ticket/current-request acceptance-fence equality. Main
   still fails semantically on its placeholder receipt; no API gap replaces it.
3. Superseded accepted/rejected native results use separate old and successor
   gates. Before releasing the old operation, capture successor feedback-owner
   identity, command generation, actual admission/ticket and ON target. The
   successor is still waiting for the physical device lock, so it has no new
   confirmation window yet. Release only the old RPC; allow the successor to
   enter its native gate while keeping its result blocked and caller pending.
   Require zero state publications over that entire isolated interval, the same
   successor owner/generation/admission/ticket/target, old timer canceled, no
   successor timer, and exactly the original one requested timer. Then allow the
   successor's own return and require exactly one ON window and two total timer
   requests. Both old-outcome orderings pass unchanged main. The positive caller
   deadline is 60 seconds to prevent caller timeout contaminating the isolated
   stale-outcome interval; the test releases native gates explicitly, without
   sleeping for that duration. Native requests remain serialized throughout.
4. Normal success with a positive caller deadline now requires the exact ticket
   and current request to be resolved and share non-null acceptance provenance,
   with same-generation/lower-sequence pre-write capture. Those assertions follow
   the stale-capture wire behavior. Main still fails on the stale contradiction,
   preserving a semantic rather than API-only failure.

The successor diagnostic seam deliberately reads the current feedback owner and
actual adapter admission while waiting; it does not fabricate a pending window
before physical native issue. Runtime integration may point these reads at the
new owner/command diagnostics, preserving identity and the full zero-wire interval.

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
`phase-b-spec-r1-baseline-focused.log`, `phase-b-spec-r1-baseline.xml`,
`phase-b-spec-r1-ruff.log`, `phase-b-spec-r1-format.log`, and
`phase-b-spec-r1-mypy.log`. `phase-b-spec-fixes-targeted.log` records the five
corrected parameter cases: three passes and two semantic failures. The original
`phase-b-baseline-focused.log`/`phase-b-baseline.xml`, original static logs,
`phase-b-baseline-runtime-initial.log`, and `phase-b-baseline-ownership.log`
remain preserved. The spec-r1 focused log/XML is the current authoritative replay.

Commands (all run after `umask 022`):

```sh
uv run ruff check
uv run ruff format --check
uv run mypy --strict src tests scripts/brilliant-panel/bundle_manifest.py
uv run pytest -q tests/test_mesh_settlement_regressions.py tests/test_mesh_ticket_regressions.py tests/test_mesh_feedback.py tests/test_write_admission.py tests/test_bridge.py::TestMeshConfirmedWrites::test_stale_write_completion_does_not_resurrect_pending tests/test_bridge.py::TestMeshConfirmedWrites::test_completed_resolver_clears_its_registry_entry tests/test_bus_adapter.py::TestInteractiveScheduling::test_cancelled_lane_revokes_folded_waiter_but_retains_issued_rpc tests/test_bus_adapter.py::TestInteractiveScheduling::test_ingress_only_newest_payload_issues_after_gate_opens --tb=short --junitxml=/data/projects/joyfulhouse/homeassistant-dev/brilliant-triage-2026-11-23/S9/phase-b-spec-r1-baseline.xml
```

Root Ruff passes and root format checks 218 files. Root strict mypy passes for
193 source files. Including `src` gives mypy the repository's typed module sources;
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

## Task 4B Connected Adaptations

The original 75-ID mapping and 148-case baseline above remain historical evidence.
Task 4B connects the reviewed owner/registry/driver to Bridge and runs all 148
cases successfully. Baseline logs/XML are unchanged; new evidence uses
`S9/task4b-*` names. No case is skipped, marked xfail or removed.

- Diagnostics now read the actual `Bridge.mesh_registry`, immutable owner
  `view`/`render`, and `Bridge.mesh_driver` timer/child/diff-cache views. The old
  mutable pending/feedback/counter dictionaries and resolver functions are gone.
  Lifetime identity compares the same owner; late acceptance compares a fresh
  exact window token, and supersession compares captured command IDs/window
  tokens. Targets, receipt, acceptance/return envelope, deadline, physical lock,
  cancel-before-gate-release, immutable-view and cleanup assertions remain.
- `test_older_same_generation_capture_cannot_replace_newer_evidence` now actually
  holds the older native read after its read-start reservation, delivers a newer
  capture, then returns the frozen older native snapshot. Its original newer
  evidence, matching/contradiction, deadline and terminal wire assertions remain.
  Sequential successful `get_all()` calls already reduce synchronously and cannot
  defer an admitted earlier contradiction until an asynchronous poll callback.
- The approved active branch of
  `test_detached_rpc_outcome_cannot_settle_or_rearm_feedback` now expects actual
  late acceptance to start its fresh 80-second window, or actual rejection to
  publish failed. Its withdrawn branches still require exactly zero wire effects,
  no rearm and no timer. The former hybrid FakeBus/real-write-only adapter fixture
  is replaced with one production adapter for capture, issue and actual return.
  Separate worker-abandonment schedules keep state unknown through real return.
- The two production scheduling fixtures now establish real owners using native
  `get_peripheral` snapshots, stamp original `_InboundMessage.mesh_owner`, and
  owner-bind the physical blocker/successor tickets. The mesh numeric aux case
  uses the real LIGHT `motion_high_threshold` route (0..255) instead of a synthetic
  LIGHT `screen_brightness` descriptor. The exact final value 120, one native
  replacement, ordering, nonpreemption, peak concurrency and timer assertions
  remain. Nonmesh `screen_brightness` cases retain their original payloads.
- FakeBus supplies primitive source/generation/sequence/capture time, inventory
  before captures, and first actual native return; FakeMqtt stamps original owner
  at injection. They contain no settlement policy. Fixtures explicitly share the
  adapter/Bridge/deriver monotonic clock. Metadata-only mesh carriers remain empty;
  compatibility assertions materialize the current owner on demand.
- The old superseding-command test drains and asserts the current Begin's
  legitimate superseded document while the new native call stays gated. It then
  clears the wire log, snapshots the actual successor command/window/targets,
  releases the old deadline and still requires zero additional publications or
  warnings, unchanged successor identity, and no old request escape.
- Initial or terminal MQTT failures are contained by the final driver lane and
  retain reducer debt. The former caller-exception assertion is replaced with
  zero successful wire publications plus actual outstanding owner debt; the next
  existing observation must repair the same pending/null document. Warning
  assertions follow the actual mesh driver. A settled native timeout retains
  its actual TIMEOUT/return fence and no invented pending-RPC receipt.
- The broker-recorded pending test checks the actual driver diff cache and
  admitted owner evidence while the single MQTT child is blocked. Expiry must
  cancel/join that child and repair the terminal wire document; a duplicate
  pending child is no longer required merely to acknowledge a matching capture.
- Private desired-enforcement fixtures acquire the registry and deliver their
  inventory through the native fake before enforcement. Direct stale/unscoped
  snapshots cannot create owners. Batching, caps, retry/backoff, spacing, scope,
  missing-variable and persistence assertions remain. The per-variable backoff
  case uses LIGHT `motion_low_threshold` instead of MOTION_CONFIG-only
  `enable_pir_motion_score`, preserving all attempt counts and timing assertions.
- The deferred-intent snapshot-loss schedule uses a wired device, whose snapshot
  remains the translation source. Separate mesh schedules prove metadata-cache
  loss does not block a valid owner-stamped command or projection, while actual
  owner loss rejects stale aux commands before recording intent. Heartbeat/read
  exception cases use wired devices, with a HARDWARE aux payload for the existing
  MQTT exception-propagation assertion; primary final lanes contain failures.
  An acquired-mesh poll separately requires heartbeat and owner-state publication.
- The standby normalization check still requires zero work and publications. Its
  first acquired mesh push now requires zero legacy normalization/Variable
  allocation and all 40 state publications through primitive owner ingress.
  Nonmesh deferred-normalization assertions remain unchanged.
- Nonmesh diagnostic clock-read and measurement assertions remain unchanged.
  Only mesh native returns allocate return provenance and advance creation floor;
  wired issue retains field eras and current-generation provenance, reusing the
  lock-acquisition timestamp without an intervening await or another clock read.
- Direct auxiliary translation validates the registered route/descriptor and
  original live mesh owner before persisting reconciled intent. Production
  adapter cases cover missing/revoked context, missing routes and descriptors
  invalidated by a native capture; existing desired contents and file bytes
  remain unchanged, with zero native calls through later ordinary enforcement.
  Registered wired routes still record intent before the snapshot guard.
- A reconnect permanently revokes retired state/admission but retains the exact
  unfinished routing-cleanup entry. Existing repair triggers join obsolete
  effects with the current bound and run cleanup only while acquisition and
  adapter admission remain open. Actual loop/foreign hook schedules preserve
  ordinary-failure debt, require unsubscribe/config tombstones and release old
  maps through same/distinct-ID churn; reappearance prevents obsolete cleanup
  from changing successor ownership or emitting old state.

Additional connected production schedules cover synchronous native return before
caller resumption; merged MQTT fold ownership before the replacement handler;
issue preparation followed by actual commit refusal; worker cancellation during
native wait and after stored return; original queued owner across reappearance;
auxiliary return/native contradiction/delayed caller; ordinary cleanup failure
repaired only by existing reconcile; dynamic standby/shared panel scoping; twelve
distinct peripheral IDs with bounded live maps; unsupported infrastructure; and
exact inherited completion listeners across registered loop/foreign reconnect
hooks and withdrawal/reacquisition.

## Final Integration Validation

The completed implementation reruns the **exact historical 148-case selection:
148 PASS**, with zero skips/xfails, errors or failures. The collected
`(classname, name)` set is identical to the authoritative Phase B XML's 148
nodes. All 75 original mapping rows and the historical baseline outcomes above
are preserved; the baseline logs/XML were not overwritten. Task 4B's explicit
fixture/diagnostic adaptations remain documented above. Final slider operations
retain the exact preparation ticket, per-slider mutex, backup/write/readback/
commit ordering and capacity/fatal/generation revocation assertions.

Final root static checks pass, and the affected focused selection passes
**1,871 cases across 62 files**. This includes every new runtime test and the
affected bus, admission, Bridge, MQTT, model, motion, commands, desired-state,
wired feedback, camera/privacy, coordinator and virtual-slider consumers. These
counts overlap the historical selection and the schedule/resource proofs;
they must not be added together. Full agent and HA suites remain CI gates.

Representative executed schedule anchors (parameter variants run together):

| Schedule | Committed test evidence |
| --- | --- |
| Disjoint/overlapping native read and push | `tests/test_mesh_bus_ingress.py::test_actual_read_push_overlap_merges_owner_fields_independently` |
| Actual registered reconnect hooks, ready expiry and waiting issue | `tests/test_mesh_bus_reconnect.py::test_actual_hook_fences_an_already_ready_real_owner_expiry`; `tests/test_mesh_bus_issue.py::test_actual_foreign_hook_fences_write_already_waiting_for_device_lock` |
| Acquisition, retirement, motion and negative-read fences | `tests/test_mesh_final_schedules.py::test_native_read_from_old_acquisition_cannot_seed_reacquired_owner_or_routes`; `test_pre_expiry_native_read_and_aux_only_reconnect_cannot_release_unknown`; `test_stale_high_motion_native_read_between_low_pushes_cannot_refresh_history`; `test_native_negative_read_shapes_respect_per_field_evidence_and_topology` |
| Identical mirror with target/power/deletion/unreliable masks | `tests/test_mesh_final_schedules.py::test_identical_native_mirror_refreshes_only_explicit_reliable_target_evidence` |
| Production MQTT fold and original queued owner | `tests/test_mesh_bridge_integration.py::test_production_lane_fold_owns_merged_targets_before_rehandler`; `test_original_queued_owner_cannot_write_after_same_topic_replacement` |
| Blocked old-window pending/terminal, late acceptance and successful idle expiry | `tests/test_mesh_final_schedules.py::test_late_native_acceptance_revokes_blocked_old_window_before_handoff`; `test_successful_native_idle_expiry_revokes_blocked_pending_before_handoff`; `test_blocked_terminal_native_contradiction_precedes_ordinary_idle_debt` |
| Abandonment, successor completion and absent-owner creation floor | `tests/test_mesh_bridge_integration.py::test_issued_worker_abandonment_remains_unknown_through_real_return`; `tests/test_mesh_final_schedules.py::test_abandoned_native_work_keeps_successor_unknown_until_post_return_capture`; `test_native_return_without_owner_fences_already_started_reappearance_read` |
| Astra R5 scale/read across native return | `tests/test_mesh_registry.py::test_pre_return_binding_repair_seeds_no_state_and_older_reads_cannot_rebind_or_remove` |
| Routing, availability, capacity and full ownership-map churn | `tests/test_mesh_capacity_routing.py`; `tests/test_mesh_availability.py`; `tests/test_mesh_discovery_metadata.py`; `tests/test_mesh_session_recovery.py`; `tests/test_mesh_bridge_integration.py::test_repeated_mesh_create_write_remove_reacquire_bounds_all_live_maps` |
| Resident slider preparation/issue and deferred recovery | `tests/test_mesh_slider_admission.py`; `tests/test_mesh_slider_operations.py` |
| Standby/shared-panel native read budget and blocked-broker work | `tests/test_mesh_bridge_integration.py::test_dynamic_standby_and_shared_panel_bridge_keep_one_mesh_guard`; `tests/test_mesh_resource_bounds.py::test_forty_load_storm_has_bounded_work_and_no_extra_reads` |

The connected resource proof uses the actual production adapter, native observer
and Bridge with **40 loads, 30 raw variables each and 101 frames**. It records
4,040 captures and 4,040 scoped fields, zero bus normalizations/`Variable`
allocations, **80 renders while the state broker children are blocked**, 40
initial blocked state attempts and 80 total attempts. Tasks/owners/lanes remain
bounded, mesh never allocates wired field eras, and native reads remain exactly
the initial `own-device` and `ble_mesh` reads. All final state is ON/idle. The
explicit post-unblock flush produces **267 total renders**; the blocked-work
bound is not a total-render claim or a live CPU measurement.

TDD chronology is preserved: Phase B ran before implementation and recorded
100 PASS, 31 BEHAVIOR RED, 5 CLEANUP RED and 12 API GAP. Runtime tasks then added
their focused regressions and corrections. The resource proof and
`tests/test_mesh_final_schedules.py` are **postimplementation supporting proof**;
no preimplementation RED result is claimed for those additions. No test-only
owner policy substitutes for production settlement.

Detailed final schedule-to-node/result audit and touched-surface coverage live
in the persistent S9 artifacts `task6-final-schedule-audit.md`,
`task6-final-schedule-audit.json` and `task6-affected-files.txt`. Execution logs
and XML are `task6-final-affected.*`, `task6-final-historical148.*`,
`task6-final-schedules.*` and `task6-final-resource.*`; original Phase B evidence
has a separately recorded SHA-256 inventory. These are internal local gates,
not final prescribed PR reviews, CI or deployment. Agent 0.12.0, integration
0.13.0 and release ordinal 7 remain unchanged. HA static and focused payload
consumer gates do not establish HA-core template rendering: the stock-Jinja
limitation in [mesh-write-feedback.md](../reference/mesh-write-feedback.md)
still applies. Live CPU delta (agent <=2 percentage points, bus <=0.5) and
physical responsiveness remain outstanding under #225.

## PR #297 Review Corrections, Round 1

The earlier final-integration results above describe the initial implementation
head, `ace2edbfa6b53016f2dcbb892af5de56a1e3f119`. Round-one adversarial review
found that the mapped late-acceptance/matching-value test checked only the last
wire state, losing the original zero-publication-before-expiry and exactly-one
OFF-at-expiry assertions. That port now records the target-state-topic offset
after acceptance, requires an empty complete slice through the matching
observation, then compares the complete expiry slice to the one expected
idle/OFF document. Existing deadline, owner and timer assertions remain. Two
permanent injection schedules reject both an early OFF followed by pending and
a duplicate terminal OFF. They live in `tests/test_mesh_review_corrections.py`,
so the exact historical selection remains 148 cases and all 75 mapping rows
and baseline evidence remain intact.

The same correction module covers ordinary and capacity retirement of an
already-unknown load; delayed capability replacement in both primary
polarities and with newer auxiliary evidence; authoritative unknown-ID absence
at deletion, scoped-None and complete-inventory boundaries; failed ownerless
withdrawal cleanup, repeated failure and reappearance; and healthy-broker
routing work at 10 and 40 loads. A shadowed capability replacement accepts its
metadata but starts unknown behind a fresh existing binding reservation.
A second already-started read cannot rewind that successor's binding or fields,
including after newer explicit field removal. Post-replacement trusted capture
restores state. No command, pending request or old evidence is migrated.

Correction TDD evidence is recorded separately under S9 as
`pr297-r1-red-final.log` (13 semantic failures, one passing control, before
production edits), `pr297-r1-successor-red.log` (two additional delayed-read
failures against the initial correction), and `pr297-r1-orphan-race-red.log`
(one synchronous-reappearance failure against the initial correction).
Earlier fixture-signature and typing corrections are not semantic RED claims.
The additional routing-index churn assertions are postimplementation supporting
proof. No baseline XML/log or original reviewer probe was overwritten.

The consolidated correction focused selection passes **1,903 cases across 64 files**,
including **32 correction cases**, and the exact historical selection again
passes **148 cases**. The seven unchanged reviewer runtime probes pass; an
external wrapper around the two unchanged sensitivity probes requires their
invalid sequences to raise an assertion. Healthy stable pushes perform zero
routing-record enumeration and create zero topic-repair workers. Three
unchanged polls visit 99 routing records at 10 loads and 369 at 40 loads, one
shared readiness pass per poll, with zero new topic-repair workers. Bounded
routing work indexes disappear with their corresponding records through
retirement, withdrawal, reappearance and distinct-ID churn. These are local
operation counts, not on-panel CPU acceptance. Final correction commands,
results and payload proof are recorded in `S9/pr297-r1-implementer-handoff.md`.

The controller-confirmed scope-unknown extension adds cold/warm schedules for
successful missing and incomplete parent reads, with native-exception controls.
Before the extension's source fix, four semantic failures and two passing
controls are recorded in `pr297-r1-scope-unknown-red-corrected-fixture.log`.
These boundaries now advance the existing creation/inventory ordering and hold
readiness offline until a later complete inventory. Known topology/routes and
unshadowed auxiliary merges survive; transport exceptions contribute no floor.
Both unchanged controller runtime probes pass alongside the original seven.

Kimi's final round-one review independently confirmed the unknown-ID absence
finding at HIGH severity: an owner resurrected from a pre-deletion read admitted
an actual native OFF write from MQTT during cold acquisition. Six additional
permanent schedules begin with no owners and no completed initial inventory,
then interleave deletion, scoped None or complete empty inventory before the
first read returns. Each sends MQTT OFF and requires zero owners, command
routes, subscriptions and native writes. Later inventory and explicit creation
both restore actual native admission. Each schedule also completes removal,
rejects another absent-owner command and proves a new successor can issue a
native command after reappearance. Existing hot known-owner auxiliary merges
remain covered. Read counters require zero native reads for explicit creation
and only the ordinary own-device/mesh pair for inventory recovery. These
schedules require no further production change.

The six schedules all fail semantically against a separate `git archive`
export of `ace2edb` source, recording the resurrected owner/route and actual
native `on=0` issue in `pr297-r1-cold-native-red.log`. They pass against the
corrected source in `pr297-r1-cold-native-green.log`; the imported frozen-source
path is recorded in `pr297-r1-cold-source-proof.log`. The immutable review
checkout is untouched. Kimi's unchanged original native reproduction also
passes with zero owner/route/native write. At Kimi consolidation head `24f5f99`,
root source and payload remain byte-identical to the first correction commit;
that follow-up adds only test and documentation coverage. Its gate and
payload-index evidence is recorded under the `pr297-r1-kimi-*` prefix.

Fresh spec review of the complete correction then found a remaining MED in
withdrawal repair: an ID checked absent could reappear while another ID's
repair awaited MQTT, after which its unconditional withdrawal desire still
issued unsubscribe. Two permanent actual-adapter schedules in
`tests/test_mesh_routing_issue_guard.py` reproduce this cross-ID schedule and
the worker-to-membership-child scheduling gap. Both fail semantically before
the spec correction in `specfix-red-final.log`. The correction retains live
owner-absence validity in the existing desire, rechecks exact desire/validity
inside the membership child, and requires still-valid actual membership before
marking work applied or forgetting it. A skipped operation retains the same
unapplied record and bounded indexes. Existing poll recovery restores current
routes, online readiness and actual native command admission. No read, state
map, permit, timer or retry loop is added. Evidence for this spec follow-up is
separate under `specfix-*`; earlier reviewer and correction artifacts remain.

The correction connected-schedule run passes 150 cases. Its 40-load resource
proof preserves 4,040 captures, zero native normalization/Variable allocation,
80 blocked renders and exactly the initial own-device/mesh reads; it records
279 total renders after the explicit flush. The earlier 267 total remains a
historical result, not a bound or a claim repeated by this run. HA static checks
and all 798 affected payload-consumer cases pass before the deterministic build
pair; the default-temp compatibility selection passes 293 cases.

### Round-Two Inventory, Topic Issue And Publication Corrections

The second independent review confirmed six further ordering/work-bound gaps.
Permanent typed production-adapter schedules in
`tests/test_mesh_round2_corrections.py` first produced eight semantic failures
on unchanged `a11a8f7` source (`pr297-r2-permanent-red.log` in S9), before source
edits. The expanded module has 19 passing cases, including true current 257th
capacity disable and known-owner field eligibility controls.

First sightings below the topology creation floor now return before capacity
reservation or disable. Inventory below that floor cannot complete acquisition
readiness or recover capacity; an existing trusted poll restores online
readiness without another read. Rejected readiness/topology inventory still
merges known-owner omissions at their original per-field sequences, preserving
newer independent existence and unknown-primary barriers. The existing
poll/reconcile availability sweep repairs changed previously requested
owner descriptors even when the negative batch omitted their metadata carrier.
The auxiliary MQTT schedule checks actual native write rejection.

Current ownerless withdrawal retries wait for ordinary completion and retain
errors as bounded unapplied debt; the one-second join remains for obsolete
cancellation and teardown. Config payloads, present memberships and absent
topic desires validate the individual owner's current descriptors inside the
existing issue child. A skipped issue neither acknowledges nor forgets debt.
Already-issued same-owner config and membership operations retain broker order
before repair. State publication consults a derived index containing only real
revoked children; actual child completion removes each entry. Healthy changed
notifications at 10/40 owners enumerate no unrelated state lane and create
exactly one publication child per change, with no topic-repair task. A blocked
old-generation child still precedes its successor's publication.

The focused affected selection passes 1,922 cases. The exact historical
selection remains 148 passing nodes; the audit retains all 75 mapping rows,
16 baseline digests and 33 prior schedule groups. New correction evidence is
separate under `pr297-r2-*` in S9, and original reviewer probes and baseline
artifacts remain unchanged. These off-device schedules establish ordering and
bounded work, not physical responsiveness or the live i.MX6 CPU budget.

## PR297 Round-Three Continuation Corrections

The unchanged external ten-case probe and its typed permanent port in
`tests/test_mesh_round3_corrections.py` each reproduce seven semantic failures
and three passing controls on both baseline source and the unchanged committed
payload at `a36a599fd5eb96d2397facce7fdad4c73fa7cab9`. Separate logs/XML under
`pr297-r3fix-red-*` in S9 preserve those four runs before runtime edits.

Winning inventory counts use existing inventory ordering independently of the
creation floor advanced by native return. A winning oversize disables service;
an older oversize cannot override a newer fitting inventory. Fitting readiness,
new slot creation and the combined 256-record bound keep their existing fences.
Shadowed capability replacement checks metadata and existence sequences as well
as field watermarks, then uses its existing fresh birth boundary. No old intent,
window or evidence transfers; only the exact outstanding physical completion
condition can survive.

Existing poll/reconcile repairs known owner routes in both default and shared
mesh scopes, including unapplied config/membership/forget debt and replacement
owners without metadata carriers. Shared availability publication keeps its
separate scope guard. Repair uses the existing per-peripheral topic index and
does not force healthy unrelated topics or add reads. Ownerless withdrawal debt
retains its existing retry boundary and issue-time validity checks. Synchronous
owner revocation uses the same topic index with exact desired-owner identity
and actual child checks: the permanent counter records 32 topic visits for
10 owners/33 topics and 122 visits for 40 owners/123 topics, replacing the
baseline's 330 and 4,920 visits.

The permanent module adds controls for accepted/rejected primary and auxiliary
returns, later fitting recovery, poll and reconcile repair in both scopes,
healthy topic deduplication, unchanged read counts, metadata-only and
existence-only presence with an outstanding ticket, and actual child
cancellation with old/current/unrelated owner identities. The prior ownerless
issue-guard schedules remain unchanged. Source completion is separate from
shipping readiness: generated payload rebuilding, payload GREEN/parity and
spec then quality review remain controller-owned.

Final source validation passes 1,949 affected cases across 66 files, including
all 27 permanent round-three cases. The focused adjacent run passes 133 cases;
the exact historical selection remains 148 passing nodes. Root Ruff/check,
format check and strict mypy pass. The preservation audit confirms all 75
mapping rows, 16 baseline digests and 33 prior schedule groups. Logs/XML and
exact command JSON remain separate under `pr297-r3fix-*` in S9, including
intermediate failures and the interrupted initial affected run. Generated
payload and versions remain unchanged in this source-only continuation.
