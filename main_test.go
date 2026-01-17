package main

import (
	"os"
	"testing"
)

func TestStorageSaveAndRetrieve(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	storage, err := NewStorage(dbPath)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer func() { _ = storage.Close() }()

	deviceAddr := "AA:BB:CC:DD:EE:FF"
	payload := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D}
	reading := Reading{
		Timestamp:   1234567890,
		CO2:         450,
		Humidity:    65.5,
		Pressure:    1013,
		Temperature: 22.5,
	}

	saved, err := storage.SaveReading(deviceAddr, payload, &reading)
	if err != nil {
		t.Fatalf("failed to save payload: %v", err)
	}

	payloads, err := storage.GetUnsubmitted(10)
	if err != nil {
		t.Fatalf("failed to get unsubmitted: %v", err)
	}

	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(payloads))
	}

	p := payloads[0]
	if p.ID != saved.ID {
		t.Errorf("wrong ID: got %d, want %d", p.ID, saved.ID)
	}
	if *p.ParsedCO2 != 450 {
		t.Errorf("wrong CO2: got %d, want 450", *p.ParsedCO2)
	}

	if err := storage.MarkSubmitted([]int64{p.ID}); err != nil {
		t.Fatalf("failed to mark submitted: %v", err)
	}

	payloads, err = storage.GetUnsubmitted(10)
	if err != nil {
		t.Fatalf("failed to get unsubmitted: %v", err)
	}
	if len(payloads) != 0 {
		t.Errorf("expected 0 payloads after marking submitted, got %d", len(payloads))
	}
}

func TestStorageLimit(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	storage, err := NewStorage(dbPath)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer func() { _ = storage.Close() }()

	for i := 0; i < 100; i++ {
		payload := []byte{byte(i), 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
		reading := Reading{
			Timestamp: uint32(1234567890 + i),
			CO2:       uint16(400 + i),
		}
		if _, err := storage.SaveReading("test", payload, &reading); err != nil {
			t.Fatalf("failed to save payload %d: %v", i, err)
		}
	}

	payloads, err := storage.GetUnsubmitted(10)
	if err != nil {
		t.Fatalf("failed to get unsubmitted: %v", err)
	}

	if len(payloads) != 10 {
		t.Errorf("expected 10 payloads with limit, got %d", len(payloads))
	}
}

func TestParseReading(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		wantOK  bool
	}{
		{
			name:    "valid payload",
			payload: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x16, 0x00, 0xC8, 0x01, 0xC2, 0x03, 0xF5},
			wantOK:  true,
		},
		{
			name:    "too short",
			payload: []byte{0x00, 0x00, 0x00},
			wantOK:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := parseReading(tt.payload)
			if ok != tt.wantOK {
				t.Errorf("parseReading() ok = %v, want %v", ok, tt.wantOK)
			}
		})
	}
}

func TestGetEnv(t *testing.T) {
	key := "TEST_VAR_UNIQUE"
	defaultVal := "default"

	if got := getEnv(key, defaultVal); got != defaultVal {
		t.Errorf("getEnv() = %v, want %v", got, defaultVal)
	}

	_ = os.Setenv(key, "custom")
	defer func() { _ = os.Unsetenv(key) }()

	if got := getEnv(key, defaultVal); got != "custom" {
		t.Errorf("getEnv() = %v, want %v", got, "custom")
	}
}

func TestMarkSubmittedIdempotent(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	storage, err := NewStorage(dbPath)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer func() { _ = storage.Close() }()

	payload := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D}
	reading := Reading{Timestamp: 1234567890, CO2: 450}

	saved, err := storage.SaveReading("test", payload, &reading)
	if err != nil {
		t.Fatalf("failed to save: %v", err)
	}

	// Mark as submitted once
	if err := storage.MarkSubmitted([]int64{saved.ID}); err != nil {
		t.Fatalf("first mark failed: %v", err)
	}

	// Mark same ID again (idempotent)
	if err := storage.MarkSubmitted([]int64{saved.ID}); err != nil {
		t.Errorf("second mark failed (should be idempotent): %v", err)
	}

	// Verify still submitted
	unsubmitted, err := storage.GetUnsubmitted(10)
	if err != nil {
		t.Fatalf("failed to get unsubmitted: %v", err)
	}
	if len(unsubmitted) != 0 {
		t.Errorf("expected 0 unsubmitted, got %d", len(unsubmitted))
	}
}
