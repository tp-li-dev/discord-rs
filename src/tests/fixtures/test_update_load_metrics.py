"""Verify the acceptance gate using independent synthetic evidence."""
import unittest
from update_load import metrics, violations, live
import os


class MeasurementTests(unittest.TestCase):
    def setUp(self):
        self.sent = {key: {"slash": key % 2 == 0, "ns": 1_000_000_000} for key in range(1, 21)}
        self.http = [{"kind": "effect", "id": key, "ns": 1_100_000_000 + key} for key in self.sent]
        self.http += [{"kind": "ack", "id": key, "ns": 1_050_000_000} for key in self.sent if key % 2 == 0]
        self.replies = [{"type": "result", "id": key, "outcome": "completed"} for key in self.sent]

    def score(self):
        value = metrics(self.sent, self.http, self.replies)
        value["active_samples"] = 1
        return value

    def test_clean_evidence(self):
        self.assertEqual(violations(self.score()), [])
        self.assertEqual(self.score()["interaction_ack_max_ms"], 50)

    def test_missing_duplicate_reorder_and_failure_are_detected(self):
        self.http = [row for row in self.http if row["id"] != 1]
        self.http.append({"kind": "effect", "id": 2, "ns": 2_000_000_000})
        self.http.append({"kind": "effect", "id": 99, "ns": 500_000_000})
        self.replies[0]["outcome"] = "dispatch_failed"
        result = self.score()
        for key in ("missing_effects", "duplicate_effects", "unexpected_effects", "order_inversions", "failed_results"):
            self.assertIn(key, violations(result))

    def test_ack_deadline_includes_time_in_input_queue(self):
        for row in self.http:
            if row["kind"] == "ack" and row["id"] == 2:
                row["ns"] = 4_000_000_001
        self.assertEqual(self.score()["interaction_over_3s"], 1)
        self.assertIn("interaction_over_3s", violations(self.score()))

    def test_no_ack_is_failure_even_without_latency_sample(self):
        self.http = [row for row in self.http if row["kind"] != "ack"]
        self.assertEqual(self.score()["interaction_missing_ack"], 10)
        self.assertIn("interaction_missing_ack", violations(self.score()))

    def test_orphan_double_active_send_and_probe_fail_closed(self):
        self.assertTrue(live(os.getpid()))
        for key in ("orphans", "double_active_samples", "harness_errors"):
            value = self.score()
            value[key] = 1
            self.assertIn(key, violations(value))
        value = self.score()
        value["active_samples"] = 0
        self.assertIn("missing_active_observation", violations(value))

    def test_duplicate_interaction_ack_is_not_hidden_by_first_timestamp(self):
        self.http.append({"kind": "ack", "id": 2, "ns": 2_000_000_000})
        self.assertEqual(self.score()["interaction_duplicate_ack"], 1)
        self.assertIn("interaction_duplicate_ack", violations(self.score()))

    def test_error_message_does_not_count_as_successful_event(self):
        self.http[0]["kind"] = "failed_effect"
        value = self.score()
        self.assertEqual(value["missing_effects"], 1)
        self.assertEqual(value["failed_effects"], 1)
        self.assertIn("missing_effects", violations(value))

    def test_unknown_orphan_count_cannot_pass_or_be_reported_as_zero(self):
        value = self.score()
        value["orphans"] = None
        self.assertIn("orphans", violations(value))


if __name__ == "__main__":
    unittest.main()
