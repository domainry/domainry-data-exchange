// Package service contains deployment-neutral Data Exchange domain policies.
package service

import (
	"fmt"
	"strings"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
)

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
