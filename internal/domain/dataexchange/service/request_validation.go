// Package service contains deployment-neutral Data Exchange domain policies.
package service

import (
	"fmt"
	"strings"
	"time"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange/internal/domain/dataexchange/model"
)

const maxProcessingAttempts = 3
const retryInitialDelay = time.Second

func ValidateImportRequest(request dataexchange.ImportRequest) error {
	if err := request.Scope.Validate(); err != nil {
		return err
	}
	if request.Source == nil || strings.TrimSpace(request.Provider) == "" || strings.TrimSpace(request.ObjectKey) == "" || strings.TrimSpace(request.IdempotencyKey) == "" {
		return fmt.Errorf("Data Exchange import request is incomplete")
	}
	return nil
}

func ValidateExportRequest(request dataexchange.ExportRequest) error {
	if err := request.Scope.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(request.Provider) == "" || strings.TrimSpace(request.ObjectKey) == "" || strings.TrimSpace(request.IdempotencyKey) == "" {
		return fmt.Errorf("Data Exchange export request is incomplete")
	}
	return nil
}

// PlanProcessingFailure owns the deployment-neutral retry transition. Stores
// persist this decision but do not choose retry counts or backoff policy.
func PlanProcessingFailure(previousAttempts int, code string, now time.Time) model.FailurePlan {
	if previousAttempts < 0 {
		previousAttempts = 0
	}
	plan := model.FailurePlan{Status: "failed", Code: strings.TrimSpace(code), Attempts: previousAttempts + 1}
	if plan.Code == "" {
		plan.Code = "processing_failed"
	}
	if plan.Attempts < maxProcessingAttempts {
		plan.Status = "queued"
		plan.NextAttemptAt = now.UTC().Add(retryInitialDelay * time.Duration(1<<previousAttempts))
	}
	return plan
}
