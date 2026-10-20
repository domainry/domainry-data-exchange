package module

import (
	"net/http/httptest"
	"testing"

	"github.com/domainry/domainry-foundation/apperror"
)

type classificationCapture struct {
	*httptest.ResponseRecorder
	kind, code string
}

func (capture *classificationCapture) RecordError(kind, code string) {
	capture.kind, capture.code = kind, code
}

func TestDataExchangeErrorClassificationMatrix(t *testing.T) {
	tests := []struct {
		kind apperror.ErrorKind
		code string
	}{
		{apperror.KindUnavailable, "backend.data_exchange.database_unavailable"},
		{apperror.KindUnavailable, "backend.data_exchange.provider_unavailable"},
		{apperror.KindConflict, "backend.data_exchange.lease_rejected"},
		{apperror.KindBadRequest, "backend.data_exchange.request_invalid"},
		{apperror.KindForbidden, "backend.data_exchange.permission_denied"},
	}
	for _, test := range tests {
		capture := &classificationCapture{ResponseRecorder: httptest.NewRecorder()}
		writeError(capture, apperror.New(test.kind, test.code, nil, nil))
		if capture.kind != string(test.kind) || capture.code != test.code {
			t.Fatalf("want=%q/%q got=%q/%q", test.kind, test.code, capture.kind, capture.code)
		}
	}
}
