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

type config struct {
	deviceAddr      string
	sensorServiceID string
	notifyCharID    string
	channelSize     int
	dbPath          string
	vmEndpoint      string
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
		deviceAddr:      getEnv("DEVICE_ADDR", "62:00:A1:3F:B4:26"),
		sensorServiceID: "0000ffe0-0000-1000-8000-00805f9b34fb",
		notifyCharID:    "0000ffe4-0000-1000-8000-00805f9b34fb",
		channelSize:     1,
		dbPath:          getEnv("DB_PATH", "./data/payloads.db"),
		vmEndpoint:      getEnv("VM_ENDPOINT", ""),
	}

	// Print config at startup
	logger.Info("starting inkbird-monitor",
		"device_addr", cfg.deviceAddr,
		"db_path", cfg.dbPath,
		"vm_endpoint", cfg.vmEndpoint,
		"mqtt_server", getEnv("MQTT_SERVER", ""),
		"sensor_service", cfg.sensorServiceID,
		"notify_char", cfg.notifyCharID,
		"channel_size", cfg.channelSize,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize storage
	storage, err := NewStorage(cfg.dbPath)
	assert("storage init", err)
	defer func() {
		if err := storage.Close(); err != nil {
			logger.Error("storage close failed", "error", err)
		}
	}()

	// Print storage statistics
	stats, err := storage.GetStats()
	if err != nil {
		logger.Warn("failed to get storage stats", "error", err)
	} else {
		if stats.LastReadingAt != nil {
			logger.Info("storage initialized",
				"path", cfg.dbPath,
				"total_readings", stats.TotalReadings,
				"last_reading", stats.LastReadingAt.Format(time.RFC3339),
			)
		} else {
			logger.Info("storage initialized",
				"path", cfg.dbPath,
				"total_readings", stats.TotalReadings,
				"last_reading", "none",
			)
		}
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
		assert("mqtt", err)
		defer func() {
			assert("mqtt close", mqtt.Close())
		}()
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

		// Trigger recovery by pushing 1 unsubmitted to rawPayloads
		go func() {
			payloads, err := storage.GetUnsubmitted(1)
			if err != nil {
				logger.Error("failed to get startup unsubmitted", "error", err)
			} else if len(payloads) > 0 {
				logger.Info("triggering recovery", "unsubmitted", "exists")
			}
		}()
	}

	// Registration
	adapter := bluetooth.DefaultAdapter
	assert("enable adapter", adapter.Enable())

	rawPayloads := make(chan []byte, cfg.channelSize)

	// Track last reading time for connection health monitoring
	var lastReadingTime atomic.Int64
	lastReadingTime.Store(time.Now().Unix())

	// Maintain connection with retry loop
	go maintainConnection(ctx, logger, adapter, cfg, rawPayloads, &lastReadingTime)

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

func maintainConnection(ctx context.Context, logger *slog.Logger, adapter *bluetooth.Adapter, cfg config, rawPayloads chan<- []byte, lastReadingTime *atomic.Int64) {
	backoff := time.Second
	const maxBackoff = 60 * time.Second
	const staleTimeout = 10 * time.Minute
	attemptCount := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		attemptCount++
		if attemptCount > 1 {
			logger.Info("attempting reconnection", "attempt", attemptCount, "backoff", backoff)
		}

		// Create a context for this connection that can be cancelled independently
		connCtx, connCancel := context.WithCancel(ctx)

		device, err := connectDevice(connCtx, logger, adapter, cfg, rawPayloads)
		if err != nil {
			connCancel()
			logger.Error("connection failed", "attempt", attemptCount, "error", err, "retry_in", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
			}
			continue
		}

		// Connected successfully, reset backoff and attempt counter
		logger.Info("connection established", "attempt", attemptCount)
		backoff = time.Second
		attemptCount = 0

		// Monitor connection health in background
		go monitorConnectionHealth(connCtx, connCancel, logger, lastReadingTime, staleTimeout)

		// Wait for disconnection signal (from health monitor) or shutdown
		<-connCtx.Done()

		// Cleanup device
		logger.Info("disconnecting device")
		if err := device.Disconnect(); err != nil {
			logger.Error("disconnect failed", "error", err)
		}

		// If parent context is done (shutdown), exit
		if ctx.Err() != nil {
			return
		}

		// Otherwise, connection was stale or died - reconnect after backoff
		logger.Warn("connection lost, will reconnect", "backoff", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff = min(backoff*2, maxBackoff)
		}
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

func connectDevice(ctx context.Context, log *slog.Logger, adapter *bluetooth.Adapter, cfg config, rawPayloads chan<- []byte) (bluetooth.Device, error) {
	log.Info("scanning for bluetooth device", "addr", cfg.deviceAddr)
	ch := make(chan bluetooth.ScanResult, 1)
	scanDone := make(chan error, 1)

	go func() {
		err := adapter.Scan(func(adapter *bluetooth.Adapter, result bluetooth.ScanResult) {
			if result.Address.String() == cfg.deviceAddr {
				if err := adapter.StopScan(); err != nil {
					log.Error("stop scan failed", "error", err)
				}
				ch <- result
			}
		})
		scanDone <- err
	}()

	var result bluetooth.ScanResult
	select {
	case <-ctx.Done():
		log.Warn("scan cancelled by context")
		_ = adapter.StopScan()
		return bluetooth.Device{}, ctx.Err()
	case err := <-scanDone:
		if err != nil {
			log.Error("scan failed", "error", err)
			return bluetooth.Device{}, fmt.Errorf("scan: %w", err)
		}
		result = <-ch
	case result = <-ch:
		// Got result
	}

	log.Debug("found bluetooth device, connecting", "addr", result.Address)

	device, err := adapter.Connect(result.Address, bluetooth.ConnectionParams{})
	if err != nil {
		log.Error("failed to connect to device", "addr", result.Address, "error", err)
		return bluetooth.Device{}, fmt.Errorf("connect: %w", err)
	}
	log.Info("connected to bluetooth device", "addr", result.Address)

	if err := setupNotifications(ctx, log, device, cfg, rawPayloads); err != nil {
		log.Error("failed to setup notifications", "error", err)
		_ = device.Disconnect()
		return bluetooth.Device{}, fmt.Errorf("setup notifications: %w", err)
	}

	return device, nil
}

func setupNotifications(ctx context.Context, log *slog.Logger, device bluetooth.Device, cfg config, rawPayloads chan<- []byte) error {
	// Discover services
	svcUUID := parseUUID(cfg.sensorServiceID)
	services, err := device.DiscoverServices([]bluetooth.UUID{svcUUID})
	if err != nil {
		log.Error("service discovery failed", "service_uuid", cfg.sensorServiceID, "error", err)
		return fmt.Errorf("discover services: %w", err)
	}
	log.Debug("discovered services", "count", len(services))

	for _, service := range services {
		log.Debug("discovered service", "uuid", service.UUID().String())

		charUUID := parseUUID(cfg.notifyCharID)
		chars, err := service.DiscoverCharacteristics([]bluetooth.UUID{charUUID})
		if err != nil {
			log.Error("characteristic discovery failed", "service_uuid", service.UUID().String(), "error", err)
			return fmt.Errorf("discover characteristics: %w", err)
		}
		log.Debug("discovered characteristics", "service_uuid", service.UUID().String(), "count", len(chars))

		for _, char := range chars {
			log.Info("discovered characteristic", "uuid", char.UUID().String())

			if char.UUID().String() == cfg.notifyCharID {
				err = char.EnableNotifications(func(buf []byte) {
					// Copy the buffer since it's reused
					payload := make([]byte, len(buf))
					copy(payload, buf)

					select {
					case rawPayloads <- payload:
					case <-ctx.Done():
						return
					default:
						log.Warn("raw payloads channel full, dropping notification", "size", len(buf))
					}
				})
				if err != nil {
					log.Error("failed to enable notifications", "characteristic", char.UUID().String(), "error", err)
					return fmt.Errorf("enable notifications: %w", err)
				}
				log.Info("notifications enabled", "notification", char.UUID().String())
				return nil
			}
		}
	}

	log.Error("notification characteristic not found", "expected_uuid", cfg.notifyCharID)
	return fmt.Errorf("notification characteristic %s not found", cfg.notifyCharID)
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

func parseUUID(s string) bluetooth.UUID {
	uuid, err := bluetooth.ParseUUID(s)
	assert("parse UUID", err)
	return uuid
}

func assert(action string, err error) {
	if err != nil {
		slog.Error("fatal error", "action", action, "error", err)
		os.Exit(1)
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
		OnConnectError: func(err error) { log.Error("publish: error whilst attempting connection", "error", err) },
		// TODO: how do I just use slog here?
		// Errors:         logger{prefix: "publish"},
		// Debug:          logger{prefix: "publish: debug"},
		// PahoErrors: logger{prefix: "publishP"},
		// PahoDebug:      logger{prefix: "publishP: debug"},
		// eclipse/paho.golang/paho provides base mqtt functionality, the below config will be passed in for each connection
		ClientConfig: paho.ClientConfig{
			ClientID:      config.ClientID,
			OnClientError: func(err error) { log.Error("publish: client error", "error", err) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				if d.Properties != nil {
					log.Info("publish: server requested disconnect", "reason", d.Properties.ReasonString)
				} else {
					log.Info("publish: server requested disconnect", "reason", d.ReasonCode)
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
	ticker := time.NewTicker(10 * time.Second)
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
