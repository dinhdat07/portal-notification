package observability

import (
	"context"
	"net/http"
)

const (
	StatusOK       = "ok"
	StatusError    = "error"
	StatusDisabled = "disabled"

	OverallStatusOK       = "ok"
	OverallStatusNotReady = "not_ready"
)

type Checker func(context.Context) Report

type Report struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

func NewReport(checks map[string]string) Report {
	status := OverallStatusOK

	for _, checkStatus := range checks {
		if checkStatus != StatusOK && checkStatus != StatusDisabled {
			status = OverallStatusNotReady
			break
		}
	}

	return Report{
		Status: status,
		Checks: checks,
	}
}

func HTTPStatus(report Report) int {
	if report.Status == OverallStatusOK {
		return http.StatusOK
	}

	return http.StatusServiceUnavailable
}
