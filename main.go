package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"net/url"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/autopaho/queue/memory"
	"github.com/eclipse/paho.golang/paho"
	"tinygo.org/x/bluetooth"
)

var (
	sensorServiceUUID = mustParseUUID("0000ffe0-0000-1000-8000-00805f9b34fb")
	notifyCharUUID    = mustParseUUID("0000ffe4-0000-1000-8000-00805f9b34fb")
)

type config struct {
	deviceAddr  string
	channelSize int
	dbPath      string
	vmEndpoint  string
}

type Reading struct {
	Timestamp   uint32  `json:"timestamp"` // unix seconds (valid until 2106)
	CO2         uint16  `json:"co2_ppm"`
	Pressure    uint16  `json:"pressure_hPa"` // hPa  uint16 // hPa
	Humidity    float32 `json:"humidity"`
	Temperature float32 `json:"temperature"` // Celsius
}

func (r Reading) String() string {
	ts := time.Unix(int64(r.Timestamp), 0).Format(time.RFC3339)
	return fmt.Sprintf("%s | CO2: %4d ppm | Humidity: %4.1f%% | Pressure: %4d hPa | Temperature: %4.1fC",
		ts, r.CO2, r.Humidity, r.Pressure, r.Temperature)
}

type MQTTConfig struct {
	ServerURL string
	ClientID  string
	Username  string
	Password  string
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := config{
		deviceAddr:  getEnv("DEVICE_ADDR", "62:00:A1:3F:B4:26"),
		channelSize: 1,
		dbPath:      getEnv("DB_PATH", "./data/payloads.db"),
		vmEndpoint:  getEnv("VM_ENDPOINT", ""),
	}

	logger.Info("starting inkbird-monitor",
		"device_addr", cfg.deviceAddr,
		"db_path", cfg.dbPath,
		"vm_endpoint", cfg.vmEndpoint,
		"mqtt_server", getEnv("MQTT_SERVER", ""),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize storage
	storage, err := NewStorage(cfg.dbPath)
	if err != nil {
		logger.Error("storage init failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := storage.Close(); err != nil {
			logger.Error("storage close failed", "error", err)
		}
	}()

	stats, err := storage.GetStats()
	if err != nil {
		logger.Warn("failed to get storage stats", "error", err)
	} else {
		lastReading := "none"
		if stats.LastReadingAt != nil {
			lastReading = stats.LastReadingAt.Format(time.RFC3339)
		}
		logger.Info("storage initialized", "path", cfg.dbPath, "total_readings", stats.TotalReadings, "last_reading", lastReading)
	}

	// Initialize MQTT
	mqttConfig := MQTTConfig{
		ClientID:  "inkbird-iam-t1_" + time.Now().UTC().Format(time.RFC3339),
		ServerURL: os.Getenv("MQTT_SERVER"),
		Username:  os.Getenv("MQTT_USERNAME"),
		Password:  os.Getenv("MQTT_PASSWORD"),
	}
	var mqtt *mqttSession
	if mqttConfig.ServerURL != "" {
		mqtt, err = NewMQTT(ctx, mqttConfig, logger)
		if err != nil {
			logger.Warn("mqtt disabled, init failed", "error", err)
			mqtt = nil
		} else {
			defer func() {
				if err := mqtt.Close(); err != nil {
					logger.Error("mqtt close failed", "error", err)
				}
			}()
		}
	}

	// Initialize VM writer if configured
	var vmWriter *VMWriter
	if cfg.vmEndpoint != "" {
		vmWriter = NewVMWriter(logger, cfg.vmEndpoint, storage)
		defer func() {
			if err := vmWriter.Close(); err != nil {
				logger.Error("vm writer close failed", "error", err)
			}
		}()
	}

	// Registration — give BlueZ up to 10 minutes to become available (e.g. after reboot)
	adapter := bluetooth.DefaultAdapter
	adapterCtx, adapterCancel := context.WithTimeout(ctx, 10*time.Minute)
	if err := enableAdapterWithRetry(adapterCtx, logger, adapter); err != nil {
		adapterCancel()
		logger.Error("bluetooth adapter failed, exiting", "error", err)
		os.Exit(1)
	}
	adapterCancel()

	rawPayloads := make(chan []byte, cfg.channelSize)

	// Track last reading time for connection health monitoring
	var lastReadingTime atomic.Int64
	lastReadingTime.Store(time.Now().Unix())

	// Maintain connection with retry loop
	go maintainConnection(ctx, logger, adapter, cfg.deviceAddr, rawPayloads, &lastReadingTime)

	// Normal operation
	go processPayloads(ctx, logger, cfg.deviceAddr, storage, vmWriter, mqtt, rawPayloads, &lastReadingTime)

	// Shutdown handling
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	<-sig
	logger.Info("shutdown signal received")
	cancel()

	// Drain remaining
	close(rawPayloads)
	logger.Info("shutdown complete")
}

func maintainConnection(ctx context.Context, logger *slog.Logger, adapter *bluetooth.Adapter, deviceAddr string, rawPayloads chan<- []byte, lastReadingTime *atomic.Int64) {
	backoff := time.Second
	const maxBackoff = 60 * time.Second
	const staleTimeout = 10 * time.Minute
	const adapterResetEvery = 5
	const giveUpAfter = 10 * time.Minute

	consecutiveFailures := 0
	var failingSince time.Time

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// After 10 minutes of consecutive failures, exit for systemd restart.
		// A fresh process re-initializes the adapter from scratch.
		if !failingSince.IsZero() && time.Since(failingSince) > giveUpAfter {
			logger.Error("connection failing too long, exiting for restart",
				"failing_since", failingSince.Format(time.RFC3339),
				"consecutive_failures", consecutiveFailures,
			)
			os.Exit(1)
		}

		// Re-enable adapter periodically during failure streaks.
		// BlueZ may have restarted, or the adapter state may be wedged.
		if consecutiveFailures > 0 && consecutiveFailures%adapterResetEvery == 0 {
			logger.Warn("re-enabling bluetooth adapter", "consecutive_failures", consecutiveFailures)
			if err := adapter.Enable(); err != nil {
				logger.Error("adapter re-enable failed", "error", err)
			}
		}

		if consecutiveFailures > 0 {
			logger.Info("attempting reconnection", "attempt", consecutiveFailures+1, "backoff", backoff)
		}

		connCtx, connCancel := context.WithCancel(ctx)

		device, err := connectDevice(connCtx, logger, adapter, deviceAddr, rawPayloads)
		if err != nil {
			connCancel()
			if consecutiveFailures == 0 {
				failingSince = time.Now()
			}
			consecutiveFailures++
			logger.Error("connection failed", "attempt", consecutiveFailures, "error", err, "retry_in", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
			}
			continue
		}

		logger.Info("connection established", "after_failures", consecutiveFailures)
		backoff = time.Second
		consecutiveFailures = 0
		failingSince = time.Time{}

		go monitorConnectionHealth(connCtx, connCancel, logger, lastReadingTime, staleTimeout)

		<-connCtx.Done()

		// Disconnect with timeout — don't let a hung D-Bus call block the reconnect loop
		disconnectDevice(logger, device, 5*time.Second)

		if ctx.Err() != nil {
			return
		}

		logger.Warn("connection lost, will reconnect", "backoff", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff = min(backoff*2, maxBackoff)
		}
	}
}

func disconnectDevice(logger *slog.Logger, device bluetooth.Device, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		if err := device.Disconnect(); err != nil {
			logger.Error("disconnect failed", "error", err)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		logger.Warn("disconnect timed out, proceeding")
	}
}

func monitorConnectionHealth(ctx context.Context, cancel context.CancelFunc, logger *slog.Logger, lastReadingTime *atomic.Int64, staleTimeout time.Duration) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lastSeen := time.Unix(lastReadingTime.Load(), 0)
			staleDuration := time.Since(lastSeen)

			if staleDuration > staleTimeout {
				logger.Warn("no readings received, triggering reconnection",
					"last_reading", lastSeen.Format(time.RFC3339),
					"stale_duration", staleDuration.Round(time.Second),
				)
				cancel()
				return
			}
		}
	}
}

func connectDevice(ctx context.Context, log *slog.Logger, adapter *bluetooth.Adapter, deviceAddr string, rawPayloads chan<- []byte) (bluetooth.Device, error) {
	// Timeout for scan+connect+discovery. Must NOT be used for the notification
	// callback — that needs the parent ctx which lives for the connection lifetime.
	opCtx, opCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer opCancel()

	log.Info("scanning for device", "addr", deviceAddr)

	type scanResult struct {
		result bluetooth.ScanResult
		err    error
	}
	ch := make(chan scanResult, 1)

	go func() {
		var found *bluetooth.ScanResult
		err := adapter.Scan(func(a *bluetooth.Adapter, r bluetooth.ScanResult) {
			if r.Address.String() == deviceAddr {
				_ = a.StopScan()
				found = &r
			}
		})
		if err != nil {
			ch <- scanResult{err: err}
		} else if found != nil {
			ch <- scanResult{result: *found}
		} else {
			ch <- scanResult{err: fmt.Errorf("scan ended without finding %s", deviceAddr)}
		}
	}()

	var sr scanResult
	select {
	case <-opCtx.Done():
		_ = adapter.StopScan()
		return bluetooth.Device{}, opCtx.Err()
	case sr = <-ch:
		if sr.err != nil {
			return bluetooth.Device{}, fmt.Errorf("scan: %w", sr.err)
		}
	}

	device, err := adapter.Connect(sr.result.Address, bluetooth.ConnectionParams{})
	if err != nil {
		return bluetooth.Device{}, fmt.Errorf("connect: %w", err)
	}
	log.Info("connected", "addr", sr.result.Address)

	// BLE handshake settle time — Connect() returns before handshake completes
	select {
	case <-time.After(500 * time.Millisecond):
	case <-opCtx.Done():
		_ = device.Disconnect()
		return bluetooth.Device{}, opCtx.Err()
	}

	// Parent ctx for notification callback — lives for the connection lifetime
	if err := setupNotifications(ctx, log, device, rawPayloads); err != nil {
		_ = device.Disconnect()
		return bluetooth.Device{}, fmt.Errorf("setup notifications: %w", err)
	}

	return device, nil
}

func setupNotifications(ctx context.Context, log *slog.Logger, device bluetooth.Device, rawPayloads chan<- []byte) error {
	services, err := device.DiscoverServices([]bluetooth.UUID{sensorServiceUUID})
	if err != nil {
		return fmt.Errorf("discover services: %w", err)
	}
	if len(services) == 0 {
		return fmt.Errorf("service %s not found", sensorServiceUUID)
	}

	chars, err := services[0].DiscoverCharacteristics([]bluetooth.UUID{notifyCharUUID})
	if err != nil {
		return fmt.Errorf("discover characteristics: %w", err)
	}
	if len(chars) == 0 {
		return fmt.Errorf("characteristic %s not found", notifyCharUUID)
	}

	err = chars[0].EnableNotifications(func(buf []byte) {
		payload := make([]byte, len(buf))
		copy(payload, buf)

		select {
		case rawPayloads <- payload:
		case <-ctx.Done():
		default:
			log.Warn("payload channel full, dropping", "size", len(buf))
		}
	})
	if err != nil {
		return fmt.Errorf("enable notifications: %w", err)
	}

	log.Info("notifications enabled", "char", notifyCharUUID)
	return nil
}

func parseReading(data []byte) (Reading, bool) {
	if len(data) < 13 {
		return Reading{}, false
	}

	temperatureRaw := int16(data[5])<<8 | int16(data[6])
	temperature := float32(temperatureRaw) / 10.0

	humidityRaw := uint16(data[7])<<8 | uint16(data[8])
	humidity := float32(humidityRaw) / 10.0
	return Reading{
		Timestamp:   uint32(time.Now().Unix()),
		CO2:         uint16(data[9])<<8 | uint16(data[10]),
		Pressure:    uint16(data[11])<<8 | uint16(data[12]),
		Humidity:    humidity,
		Temperature: temperature,
	}, true
}

func mustParseUUID(s string) bluetooth.UUID {
	uuid, err := bluetooth.ParseUUID(s)
	if err != nil {
		panic(fmt.Sprintf("invalid UUID %q: %v", s, err))
	}
	return uuid
}

func enableAdapterWithRetry(ctx context.Context, logger *slog.Logger, adapter *bluetooth.Adapter) error {
	backoff := time.Second
	const maxBackoff = 60 * time.Second

	for {
		err := adapter.Enable()
		if err == nil {
			logger.Info("bluetooth adapter enabled")
			return nil
		}

		logger.Warn("adapter enable failed, retrying", "error", err, "retry_in", backoff)

		select {
		case <-ctx.Done():
			return fmt.Errorf("adapter enable: %w (last: %v)", ctx.Err(), err)
		case <-time.After(backoff):
			backoff = min(backoff*2, maxBackoff)
		}
	}
}

type mqttSession struct {
	conn *autopaho.ConnectionManager
}

func NewMQTT(ctx context.Context, config MQTTConfig, log *slog.Logger) (*mqttSession, error) {
	var err error
	serverURL, err := url.Parse(config.ServerURL)
	if err != nil {
		return nil, err
	}

	cliCfg := autopaho.ClientConfig{
		Queue:                         memory.New(),
		ServerUrls:                    []*url.URL{serverURL},
		ConnectUsername:               config.Username,
		ConnectPassword:               []byte(config.Password),
		KeepAlive:                     20, // Keepalive message should be sent every 20 seconds
		CleanStartOnInitialConnection: true,
		SessionExpiryInterval:         60, // If connection drops we want session to remain live whilst we reconnect
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			log.Info("mqtt: connection up", "server", config.ServerURL)
		},
		OnConnectError: func(err error) {
			log.Error("mqtt: connect failed", "server", config.ServerURL, "client_id", config.ClientID, "error", err)
		},
		// TODO: how do I just use slog here?
		// Errors:         logger{prefix: "publish"},
		// Debug:          logger{prefix: "publish: debug"},
		// PahoErrors: logger{prefix: "publishP"},
		// PahoDebug:      logger{prefix: "publishP: debug"},
		// eclipse/paho.golang/paho provides base mqtt functionality, the below config will be passed in for each connection
		ClientConfig: paho.ClientConfig{
			ClientID:      config.ClientID,
			OnClientError: func(err error) { log.Error("mqtt: client error", "error", err) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				if d.Properties != nil {
					log.Info("mqtt: server requested disconnect", "reason", d.Properties.ReasonString)
				} else {
					log.Info("mqtt: server requested disconnect", "reason", d.ReasonCode)
				}
			},
		},
	}

	c, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		return nil, err
	}
	return &mqttSession{conn: c}, nil

}

func (s *mqttSession) Close() error {
	return s.conn.Disconnect(context.Background())
}

func (s *mqttSession) Publish(ctx context.Context, topic string, msg []byte) error {
	const AtLeastOnce = byte(1)
	return s.conn.PublishViaQueue(ctx, &autopaho.QueuePublish{
		Publish: &paho.Publish{
			QoS:     AtLeastOnce,
			Topic:   topic,
			Payload: msg,
		}},
	)
}

func getEnv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

func processPayloads(ctx context.Context, logger *slog.Logger, deviceAddr string, storage *Storage, vmWriter *VMWriter, mqtt *mqttSession, rawPayloads <-chan []byte, lastReadingTime *atomic.Int64) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Periodic VM submission
			if vmWriter != nil {
				vmWriter.DrainUnsubmitted(ctx)
			}
		case payload := <-rawPayloads:
			// Update last reading timestamp
			lastReadingTime.Store(time.Now().Unix())

			reading, ok := parseReading(payload)
			if !ok {
				logger.Warn("failed to parse payload", "data", payload)
				// Store raw even if parsing failed
				if _, err := storage.SaveReading(deviceAddr, payload, nil); err != nil {
					logger.Error("failed to save unparsed payload", "error", err)
				}
				continue
			}

			// Store raw + parsed
			saved, err := storage.SaveReading(deviceAddr, payload, &reading)
			if err != nil {
				logger.Error("failed to save payload", "error", err)
				continue
			}

			// Submit to VM
			if vmWriter != nil {
				result, err := vmWriter.WriteBatch(ctx, []StoredReading{saved})
				if err != nil {
					logger.Error("vm submit failed", "error", err)
				} else if len(result.Written) > 0 {
					if err := storage.MarkSubmitted(result.Written); err != nil {
						logger.Error("failed to mark vm submitted", "error", err)
					}
				}
			}

			// Submit to MQTT
			if mqtt != nil {
				body, err := json.Marshal(reading)
				if err != nil {
					logger.Error("json marshal failed", "error", err)
				} else if err := mqtt.Publish(ctx, "inkbird-iam-t1/reading", body); err != nil {
					logger.Error("mqtt publish failed", "error", err)
				}
				// Note: MQTT doesn't mark as submitted since it's realtime only
			}

			// Print reading to console after successful processing
			logger.Info("reading",
				"co2", reading.CO2,
				"humidity", fmt.Sprintf("%.1f%%", reading.Humidity),
				"pressure", reading.Pressure,
				"temp", fmt.Sprintf("%.1fC", reading.Temperature),
			)
		}
	}
}
