package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

type VMWriter struct {
	logger   *slog.Logger
	endpoint string
	client   *http.Client
	storage  *Storage
}

type WriteResult struct {
	Written []int64
	Skipped []int64
}

func NewVMWriter(logger *slog.Logger, endpoint string, storage *Storage) *VMWriter {
	return &VMWriter{
		logger:   logger,
		endpoint: endpoint,
		client:   &http.Client{Timeout: 30 * time.Second},
		storage:  storage,
	}
}

// DrainUnsubmitted processes all remaining unsubmitted records
func (w *VMWriter) DrainUnsubmitted(ctx context.Context) {
	const batchSize = 1000
	totalRecovered := 0

	for {
		readings, err := w.storage.GetUnsubmitted(batchSize)
		if err != nil {
			w.logger.Error("failed to get unsubmitted during drain", "error", err)
			return
		}

		if len(readings) == 0 {
			if totalRecovered > 0 {
				w.logger.Info("recovery complete", "total", totalRecovered)
			}
			return
		}

		result, err := w.WriteBatch(ctx, readings)
		if err != nil {
			w.logger.Warn("failed to write during drain", "error", err)
			return
		}

		if len(result.Written) > 0 {
			if err := w.storage.MarkSubmitted(result.Written); err != nil {
				w.logger.Error("failed to mark submitted during drain", "error", err)
				return
			}
			totalRecovered += len(result.Written)
		}

		// If we got less than batch size, we're done
		if len(readings) < int(batchSize) {
			if totalRecovered > 0 {
				w.logger.Info("recovery complete", "total", totalRecovered)
			}
			return
		}
	}
}

// WriteBatch writes readings to VM and returns which IDs were written vs skipped
func (w *VMWriter) WriteBatch(ctx context.Context, readings []StoredReading) (WriteResult, error) {
	var result WriteResult

	if len(readings) == 0 {
		return result, nil
	}

	// Safety check - all our call sites use <= 1000
	if len(readings) > 5000 {
		return result, fmt.Errorf("batch too large: %d (max 5000)", len(readings))
	}

	var buf bytes.Buffer

	// Build Prometheus exposition format with timestamps
	for _, r := range readings {
		if r.ParsedCO2 == nil || r.ParsedTS == nil {
			result.Skipped = append(result.Skipped, r.ID)
			continue
		}

		ts := int64(*r.ParsedTS) * 1000 // convert to milliseconds
		device := r.DeviceAddr
		if device == "" {
			device = "unknown"
		}

		// Format: metric{labels} value timestamp_ms
		fmt.Fprintf(&buf, "sensor_co2_ppm{device=\"%s\"} %d %d\n", device, *r.ParsedCO2, ts)

		if r.ParsedHumid != nil {
			fmt.Fprintf(&buf, "sensor_humidity_percent{device=\"%s\"} %.1f %d\n", device, *r.ParsedHumid, ts)
		}

		if r.ParsedPress != nil {
			fmt.Fprintf(&buf, "sensor_pressure_hpa{device=\"%s\"} %d %d\n", device, *r.ParsedPress, ts)
		}

		if r.ParsedTemp != nil {
			fmt.Fprintf(&buf, "sensor_temperature_celsius{device=\"%s\"} %.1f %d\n", device, *r.ParsedTemp, ts)
		}

		result.Written = append(result.Written, r.ID)
	}

	if buf.Len() == 0 {
		return result, nil
	}

	// POST to VictoriaMetrics
	req, err := http.NewRequestWithContext(ctx, "POST", w.endpoint, &buf)
	if err != nil {
		return WriteResult{}, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "text/plain")

	resp, err := w.client.Do(req)
	if err != nil {
		return WriteResult{}, fmt.Errorf("post to vm: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return WriteResult{}, fmt.Errorf("vm returned status %d: %s", resp.StatusCode, body)
	}

	if len(result.Written) > 0 {
		w.logger.Info("batch submitted to vm", "written", len(result.Written), "skipped", len(result.Skipped))
	}

	return result, nil
}

func (w *VMWriter) Close() error {
	w.client.CloseIdleConnections()
	return nil
}
