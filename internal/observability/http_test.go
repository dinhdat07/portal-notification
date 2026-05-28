package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReadinessReturnsServiceUnavailableWhenDependencyFails(t *testing.T) {
	t.Parallel()

	handler := NewMux(func(context.Context) Report {
		return NewReport(map[string]string{
			"db":    StatusOK,
			"kafka": StatusOK,
			"smtp":  StatusError,
		})
	})

	req := httptest.NewRequest(http.MethodGet, "/readiness", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, `"status":"not_ready"`) {
		t.Fatalf("expected not_ready body, got %s", body)
	}
}

func TestReadinessReturnsOKWhenDependenciesAreHealthy(t *testing.T) {
	t.Parallel()

	handler := NewMux(func(context.Context) Report {
		return NewReport(map[string]string{
			"db":    StatusOK,
			"kafka": StatusOK,
			"smtp":  StatusOK,
		})
	})

	req := httptest.NewRequest(http.MethodGet, "/readiness", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
}
