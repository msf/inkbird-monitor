package main

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

type Storage struct {
	db *sql.DB
}

type StoredReading struct {
	ID          int64
	ReceivedAt  time.Time
	DeviceAddr  string
	Payload     []byte
	PayloadHex  string
	Length      int
	Submitted   bool
	Reading     *Reading // Reconstructed from parsed fields
	ParsedCO2   *uint16
	ParsedHumid *float32
	ParsedPress *uint16
	ParsedTemp  *float32
	ParsedTS    *uint32
}

func NewStorage(path string) (*Storage, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}

	s := &Storage{db: db}
	if err := s.initSchema(); err != nil {
		return nil, fmt.Errorf("init schema: %w", err)
	}

	return s, nil
}

func (s *Storage) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS payloads (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		received_at TEXT NOT NULL,
		device_addr TEXT NOT NULL,
		payload BLOB NOT NULL,
		payload_hex TEXT NOT NULL,
		length INTEGER NOT NULL,
		submitted INTEGER NOT NULL DEFAULT 0,
		parsed_timestamp INTEGER,
		parsed_co2 INTEGER,
		parsed_humidity REAL,
		parsed_pressure INTEGER,
		parsed_temperature REAL
	);

	CREATE INDEX IF NOT EXISTS idx_received_at ON payloads(received_at);
	CREATE INDEX IF NOT EXISTS idx_submitted ON payloads(submitted);
	CREATE INDEX IF NOT EXISTS idx_device ON payloads(device_addr);
	CREATE INDEX IF NOT EXISTS idx_length ON payloads(length);
	`

	_, err := s.db.Exec(schema)
	return err
}

func (s *Storage) SaveReading(deviceAddr string, payload []byte, reading *Reading) (StoredReading, error) {
	var co2, press *uint16
	var humid, temp *float32
	var timestamp *uint32

	if reading != nil {
		timestamp = &reading.Timestamp
		co2 = &reading.CO2
		humid = &reading.Humidity
		press = &reading.Pressure
		temp = &reading.Temperature
	}

	receivedAt := time.Now()
	query := `
	INSERT INTO payloads (received_at, device_addr, payload, payload_hex, length, parsed_timestamp, parsed_co2, parsed_humidity, parsed_pressure, parsed_temperature)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	result, err := s.db.Exec(query,
		receivedAt.Format(time.RFC3339Nano),
		deviceAddr,
		payload,
		hex.EncodeToString(payload),
		len(payload),
		timestamp,
		co2,
		humid,
		press,
		temp,
	)
	if err != nil {
		return StoredReading{}, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return StoredReading{}, err
	}

	// Return the stored reading for immediate use
	return StoredReading{
		ID:          id,
		ReceivedAt:  receivedAt,
		DeviceAddr:  deviceAddr,
		Payload:     payload,
		PayloadHex:  hex.EncodeToString(payload),
		Length:      len(payload),
		Submitted:   false,
		Reading:     reading,
		ParsedCO2:   co2,
		ParsedHumid: humid,
		ParsedPress: press,
		ParsedTemp:  temp,
		ParsedTS:    timestamp,
	}, nil
}

func (s *Storage) GetUnsubmitted(limit uint32) ([]StoredReading, error) {
	query := `
	SELECT id, received_at, device_addr, payload, payload_hex, length, submitted,
	       parsed_timestamp, parsed_co2, parsed_humidity, parsed_pressure, parsed_temperature
	FROM payloads
	WHERE submitted = 0 AND parsed_co2 IS NOT NULL
	ORDER BY received_at ASC
	LIMIT ?
	`

	rows, err := s.db.Query(query, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []StoredReading
	for rows.Next() {
		var r StoredReading
		var receivedAtStr string
		var submitted int

		err := rows.Scan(
			&r.ID,
			&receivedAtStr,
			&r.DeviceAddr,
			&r.Payload,
			&r.PayloadHex,
			&r.Length,
			&submitted,
			&r.ParsedTS,
			&r.ParsedCO2,
			&r.ParsedHumid,
			&r.ParsedPress,
			&r.ParsedTemp,
		)
		if err != nil {
			return nil, err
		}

		r.ReceivedAt, _ = time.Parse(time.RFC3339Nano, receivedAtStr)
		r.Submitted = submitted != 0

		// Reconstruct Reading from parsed fields if they exist
		if r.ParsedCO2 != nil && r.ParsedTS != nil {
			r.Reading = &Reading{
				Timestamp: *r.ParsedTS,
				CO2:       *r.ParsedCO2,
			}
			if r.ParsedHumid != nil {
				r.Reading.Humidity = *r.ParsedHumid
			}
			if r.ParsedPress != nil {
				r.Reading.Pressure = *r.ParsedPress
			}
			if r.ParsedTemp != nil {
				r.Reading.Temperature = *r.ParsedTemp
			}
		}

		results = append(results, r)
	}

	return results, rows.Err()
}

func (s *Storage) MarkSubmitted(ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	// Chunk to avoid unbounded transaction sizes
	const maxBatchSize = 1000
	for i := 0; i < len(ids); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(ids) {
			end = len(ids)
		}

		if err := s.markSubmittedBatch(ids[i:end]); err != nil {
			return err
		}
	}

	return nil
}

func (s *Storage) markSubmittedBatch(ids []int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare("UPDATE payloads SET submitted = 1 WHERE id = ?")
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	for _, id := range ids {
		if _, err := stmt.Exec(id); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *Storage) Close() error {
	return s.db.Close()
}
