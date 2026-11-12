//! Pure preflight for the extra Identifies used by a Gateway parallel update.
//!
//! An observation is neither a reservation nor ownership authorization. An
//! `Execute` decision permits submitting this plan to the Bot's shared Identify
//! queue; it never permits bypassing that queue or starting all sessions at once.
//! Callers must account for other consumers and serialize budget reservation at
//! the execution boundary. Separate processes/hosts need a shared coordinator.
//! This module performs no HTTP, RPC, sleeping, Identify, or consensus.
use std::{num::NonZeroU16, time::Duration};
use twilight_model::gateway::SessionStartLimit;

/// One Bot's update, including every new session needed for parallel Gateways.
/// Count any planned retry attempts too; Resume does not consume an Identify.
/// A Core-only update normally needs zero. The complete plan must fit in one
/// session budget; split larger updates explicitly rather than partly starting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UpdatePlan {
    pub additional_identifies: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeferralReason {
    InsufficientBudget {
        required: u32,
        remaining: u32,
    },
    /// A reset prediction must be confirmed with a fresh Discord observation.
    MetadataExpired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RejectionReason {
    InvalidSessionLimits,
    ObservationInFuture,
    TimeOverflow,
    ExceedsSessionTotal { required: u32, total: u32 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateDecision {
    Execute {
        additional_identifies: u32,
        /// `None` means the plan does not need an Identify at all.
        /// Otherwise the existing shared queue must enforce the concurrency
        /// buckets and Identify interval for each shard, including other plans.
        max_concurrency: Option<NonZeroU16>,
    },
    Defer {
        /// Earliest time to obtain fresh metadata and reevaluate, in the same
        /// local clock domain as `observed_at` and `now`. This is not a permit.
        retry_at: Duration,
        reason: DeferralReason,
    },
    Reject {
        reason: RejectionReason,
    },
}

impl UpdateDecision {
    /// Diagnostics contain only fixed text, counts and local deadlines. No Bot
    /// token, endpoint, response body, or other untrusted string is accepted.
    pub fn diagnostic(&self) -> String {
        match self {
            Self::Execute {
                additional_identifies: 0,
                ..
            } => "update can proceed: no additional Identify is required".into(),
            Self::Execute {
                additional_identifies,
                max_concurrency,
            } => format!(
                "update budget fits: {additional_identifies} additional Identifies; submit through the Bot's shared Identify queue (max_concurrency={}); this is not a reservation or an immediate-start permit",
                max_concurrency.map_or(0, NonZeroU16::get)
            ),
            Self::Defer {
                retry_at,
                reason: DeferralReason::InsufficientBudget { required, remaining },
            } => format!(
                "update deferred: {required} additional Identifies required, {remaining} remaining; refresh session_start_limit and reevaluate at {retry_at:?}; keep the old Gateway active"
            ),
            Self::Defer {
                retry_at,
                reason: DeferralReason::MetadataExpired,
            } => format!(
                "update deferred: session_start_limit reached its predicted reset; obtain fresh metadata and reevaluate at {retry_at:?}; do not assume the budget refilled"
            ),
            Self::Reject { reason } => match reason {
                RejectionReason::InvalidSessionLimits =>
                    "update rejected: invalid session_start_limit (total, remaining, or max_concurrency)".into(),
                RejectionReason::ObservationInFuture =>
                    "update rejected: observation time is in the future or the clock regressed".into(),
                RejectionReason::TimeOverflow =>
                    "update rejected: session reset deadline is outside the clock range".into(),
                RejectionReason::ExceedsSessionTotal { required, total } => format!(
                    "update rejected: {required} additional Identifies exceed the session total {total}; split or revise the update plan"
                ),
            },
        }
    }
}

/// Evaluate a plan without changing state. `reset_after` is Discord's relative
/// duration in milliseconds measured from `observed_at`, never from `now`.
/// Both times must belong to the same monotonic clock domain.
///
/// An insufficient budget defers until the predicted reset; at or after that
/// instant the old observation is expired, even when it had remaining sessions.
/// The caller must refresh metadata before reevaluation. Elapsed time alone
/// cannot establish that another client has not already used the reset budget.
///
/// `max_concurrency` is not a cap on this plan's total number of Identifies:
/// larger plans may proceed through the shared queue in multiple intervals.
/// Actual per-shard bucket timing remains the queue's responsibility.
pub fn evaluate_update(
    limits: &SessionStartLimit,
    observed_at: Duration,
    now: Duration,
    plan: UpdatePlan,
) -> UpdateDecision {
    let required = plan.additional_identifies;
    // Core-only and already-connected Gateway updates have no dependency on
    // session metadata. In particular exhaustion must not block Core updates.
    if required == 0 {
        return UpdateDecision::Execute {
            additional_identifies: 0,
            max_concurrency: None,
        };
    }

    let Some(max_concurrency) = NonZeroU16::new(limits.max_concurrency) else {
        return UpdateDecision::Reject {
            reason: RejectionReason::InvalidSessionLimits,
        };
    };
    if limits.total == 0 || limits.remaining > limits.total {
        return UpdateDecision::Reject {
            reason: RejectionReason::InvalidSessionLimits,
        };
    }
    if now < observed_at {
        return UpdateDecision::Reject {
            reason: RejectionReason::ObservationInFuture,
        };
    }
    if required > limits.total {
        return UpdateDecision::Reject {
            reason: RejectionReason::ExceedsSessionTotal {
                required,
                total: limits.total,
            },
        };
    }
    let Some(reset_at) = observed_at.checked_add(Duration::from_millis(limits.reset_after)) else {
        return UpdateDecision::Reject {
            reason: RejectionReason::TimeOverflow,
        };
    };
    if now >= reset_at {
        return UpdateDecision::Defer {
            retry_at: now,
            reason: DeferralReason::MetadataExpired,
        };
    }
    if required > limits.remaining {
        return UpdateDecision::Defer {
            retry_at: reset_at,
            reason: DeferralReason::InsufficientBudget {
                required,
                remaining: limits.remaining,
            },
        };
    }
    UpdateDecision::Execute {
        additional_identifies: required,
        max_concurrency: Some(max_concurrency),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> SessionStartLimit {
        SessionStartLimit {
            max_concurrency: 2,
            remaining: 4,
            reset_after: 1_500,
            total: 8,
        }
    }

    fn evaluate(limits: &SessionStartLimit, now_ms: u64, required: u32) -> UpdateDecision {
        evaluate_update(
            limits,
            Duration::from_secs(10),
            Duration::from_millis(now_ms),
            UpdatePlan {
                additional_identifies: required,
            },
        )
    }

    #[test]
    fn exact_remaining_can_proceed_through_shared_queue() {
        assert_eq!(
            evaluate(&limits(), 10_000, 4),
            UpdateDecision::Execute {
                additional_identifies: 4,
                max_concurrency: NonZeroU16::new(2),
            }
        );
    }

    #[test]
    fn plan_larger_than_concurrency_is_not_rejected_or_granted_immediate_starts() {
        let mut limits = limits();
        limits.max_concurrency = 1;
        let decision = evaluate(&limits, 10_000, 4);
        assert_eq!(
            decision,
            UpdateDecision::Execute {
                additional_identifies: 4,
                max_concurrency: NonZeroU16::new(1),
            }
        );
        assert!(decision.diagnostic().contains("shared Identify queue"));
        assert!(decision.diagnostic().contains("not a reservation"));
        assert!(decision.diagnostic().contains("max_concurrency=1"));
    }

    #[test]
    fn remaining_boundaries_for_every_plan_in_small_budget() {
        for remaining in 0..=8 {
            let mut limits = limits();
            limits.remaining = remaining;
            for required in 1..=8 {
                let decision = evaluate(&limits, 10_000, required);
                if required <= remaining {
                    assert!(matches!(decision, UpdateDecision::Execute { .. }));
                } else {
                    assert_eq!(
                        decision,
                        UpdateDecision::Defer {
                            retry_at: Duration::from_millis(11_500),
                            reason: DeferralReason::InsufficientBudget {
                                required,
                                remaining,
                            },
                        }
                    );
                }
            }
        }
    }

    #[test]
    fn exhausted_budget_defers_and_diagnoses_old_gateway_retention() {
        let mut limits = limits();
        limits.remaining = 0;
        let decision = evaluate(&limits, 10_500, 1);
        assert_eq!(
            decision,
            UpdateDecision::Defer {
                retry_at: Duration::from_millis(11_500),
                reason: DeferralReason::InsufficientBudget {
                    required: 1,
                    remaining: 0,
                },
            }
        );
        assert!(decision.diagnostic().contains("0 remaining"));
        assert!(
            decision
                .diagnostic()
                .contains("keep the old Gateway active")
        );
    }

    #[test]
    fn partially_consumed_budget_also_defers_without_partial_execution() {
        assert_eq!(
            evaluate(&limits(), 10_000, 5),
            UpdateDecision::Defer {
                retry_at: Duration::from_millis(11_500),
                reason: DeferralReason::InsufficientBudget {
                    required: 5,
                    remaining: 4,
                },
            }
        );
    }

    #[test]
    fn reset_deadline_is_anchored_to_observation_not_evaluation() {
        let expected = evaluate(&limits(), 10_001, 5);
        assert_eq!(evaluate(&limits(), 11_499, 5), expected);
    }

    #[test]
    fn one_nanosecond_before_reset_is_still_current() {
        assert!(matches!(
            evaluate_update(
                &limits(),
                Duration::from_secs(10),
                Duration::from_millis(11_500) - Duration::from_nanos(1),
                UpdatePlan {
                    additional_identifies: 4,
                },
            ),
            UpdateDecision::Execute { .. }
        ));
    }

    #[test]
    fn exact_reset_and_later_require_refresh_even_with_remaining() {
        for remaining in [0, 4, 8] {
            let mut limits = limits();
            limits.remaining = remaining;
            for now_ms in [11_500, 11_501, 100_000] {
                let decision = evaluate(&limits, now_ms, 1);
                assert_eq!(
                    decision,
                    UpdateDecision::Defer {
                        retry_at: Duration::from_millis(now_ms),
                        reason: DeferralReason::MetadataExpired,
                    }
                );
                assert!(decision.diagnostic().contains("fresh metadata"));
            }
        }
    }

    #[test]
    fn fresh_observation_after_reset_can_proceed() {
        let mut fresh = limits();
        fresh.remaining = fresh.total;
        assert_eq!(
            evaluate_update(
                &fresh,
                Duration::from_millis(11_600),
                Duration::from_millis(11_600),
                UpdatePlan {
                    additional_identifies: 8,
                },
            ),
            UpdateDecision::Execute {
                additional_identifies: 8,
                max_concurrency: NonZeroU16::new(2),
            }
        );
    }

    #[test]
    fn fresh_observation_can_show_reset_budget_already_consumed() {
        let mut fresh = limits();
        fresh.remaining = 0;
        assert_eq!(
            evaluate_update(
                &fresh,
                Duration::from_millis(11_600),
                Duration::from_millis(11_600),
                UpdatePlan {
                    additional_identifies: 1,
                },
            ),
            UpdateDecision::Defer {
                retry_at: Duration::from_millis(13_100),
                reason: DeferralReason::InsufficientBudget {
                    required: 1,
                    remaining: 0,
                },
            }
        );
    }

    #[test]
    fn zero_reset_after_never_grants_from_old_observation() {
        let mut limits = limits();
        limits.reset_after = 0;
        assert_eq!(
            evaluate(&limits, 10_000, 1),
            UpdateDecision::Defer {
                retry_at: Duration::from_secs(10),
                reason: DeferralReason::MetadataExpired,
            }
        );
    }

    #[test]
    fn core_only_update_works_despite_exhausted_budget() {
        let mut limits = limits();
        limits.remaining = 0;
        let decision = evaluate(&limits, 10_000, 0);
        assert_eq!(
            decision,
            UpdateDecision::Execute {
                additional_identifies: 0,
                max_concurrency: None,
            }
        );
        assert!(decision.diagnostic().contains("no additional Identify"));
    }

    #[test]
    fn zero_identify_plan_does_not_depend_on_metadata_or_clock() {
        let irrelevant = SessionStartLimit {
            max_concurrency: 0,
            remaining: u32::MAX,
            reset_after: u64::MAX,
            total: 0,
        };
        for now in [Duration::ZERO, Duration::MAX] {
            assert!(matches!(
                evaluate_update(
                    &irrelevant,
                    Duration::MAX,
                    now,
                    UpdatePlan {
                        additional_identifies: 0,
                    },
                ),
                UpdateDecision::Execute {
                    max_concurrency: None,
                    ..
                }
            ));
        }
    }

    #[test]
    fn plans_exceeding_total_are_rejected_before_waiting_for_reset() {
        for now_ms in [10_000, 11_500, 1_000_000] {
            for required in [9, u32::MAX] {
                let decision = evaluate(&limits(), now_ms, required);
                assert_eq!(
                    decision,
                    UpdateDecision::Reject {
                        reason: RejectionReason::ExceedsSessionTotal { required, total: 8 },
                    }
                );
                assert!(decision.diagnostic().contains("split or revise"));
            }
        }
    }

    #[test]
    fn invalid_limits_are_rejected() {
        let base = limits();
        let invalid = [
            SessionStartLimit {
                max_concurrency: 0,
                ..base.clone()
            },
            SessionStartLimit {
                total: 0,
                remaining: 0,
                ..base.clone()
            },
            SessionStartLimit {
                remaining: 9,
                ..base
            },
        ];
        for limits in invalid {
            assert_eq!(
                evaluate(&limits, 10_000, 1),
                UpdateDecision::Reject {
                    reason: RejectionReason::InvalidSessionLimits,
                }
            );
        }
    }

    #[test]
    fn future_observation_or_regressed_clock_is_rejected() {
        assert_eq!(
            evaluate(&limits(), 9_999, 1),
            UpdateDecision::Reject {
                reason: RejectionReason::ObservationInFuture,
            }
        );
    }

    #[test]
    fn reset_time_overflow_is_rejected_without_panicking() {
        assert_eq!(
            evaluate_update(
                &limits(),
                Duration::MAX,
                Duration::MAX,
                UpdatePlan {
                    additional_identifies: 1,
                },
            ),
            UpdateDecision::Reject {
                reason: RejectionReason::TimeOverflow,
            }
        );
    }

    #[test]
    fn maximum_reset_milliseconds_are_supported_without_truncation() {
        let mut limits = limits();
        limits.reset_after = u64::MAX;
        let decision = evaluate(&limits, 10_000, 5);
        assert_eq!(
            decision,
            UpdateDecision::Defer {
                retry_at: Duration::from_secs(10) + Duration::from_millis(u64::MAX),
                reason: DeferralReason::InsufficientBudget {
                    required: 5,
                    remaining: 4,
                },
            }
        );
    }

    #[test]
    fn maximum_counts_and_concurrency_do_not_overflow() {
        let limits = SessionStartLimit {
            max_concurrency: u16::MAX,
            remaining: u32::MAX,
            reset_after: 1,
            total: u32::MAX,
        };
        assert_eq!(
            evaluate(&limits, 10_000, u32::MAX),
            UpdateDecision::Execute {
                additional_identifies: u32::MAX,
                max_concurrency: NonZeroU16::new(u16::MAX),
            }
        );
    }

    #[test]
    fn function_does_not_consume_or_mutate_an_observation() {
        let limits = limits();
        let before = limits.clone();
        assert_eq!(evaluate(&limits, 10_000, 4), evaluate(&limits, 10_000, 4));
        assert_eq!(limits, before);
    }

    #[test]
    fn every_rejection_has_actionable_diagnostics() {
        for reason in [
            RejectionReason::InvalidSessionLimits,
            RejectionReason::ObservationInFuture,
            RejectionReason::TimeOverflow,
            RejectionReason::ExceedsSessionTotal {
                required: 9,
                total: 8,
            },
        ] {
            assert!(
                UpdateDecision::Reject { reason }
                    .diagnostic()
                    .starts_with("update rejected:")
            );
        }
    }
}
