package service

import (
	"testing"
	"time"
)

func TestPlanProcessingFailureOwnsRetryTransition(t *testing.T) {
	now := time.Date(2026, time.August, 31, 1, 2, 3, 0, time.UTC)
	first := PlanProcessingFailure(0, "processing_failed", now)
	if first.Status != "queued" || first.Attempts != 1 || !first.NextAttemptAt.Equal(now.Add(time.Second)) {
		t.Fatalf("first=%+v", first)
	}
	second := PlanProcessingFailure(1, "processing_failed", now)
	if second.Status != "queued" || second.Attempts != 2 || !second.NextAttemptAt.Equal(now.Add(2*time.Second)) {
		t.Fatalf("second=%+v", second)
	}
	terminal := PlanProcessingFailure(2, "processing_failed", now)
	if terminal.Status != "failed" || terminal.Attempts != 3 || !terminal.NextAttemptAt.IsZero() {
		t.Fatalf("terminal=%+v", terminal)
	}
}
