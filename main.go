package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
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
		deviceAddr:      "62:00:A1:3F:B4:26",
		sensorServiceID: "0000ffe0-0000-1000-8000-00805f9b34fb",
		notifyCharID:    "0000ffe4-0000-1000-8000-00805f9b34fb",
		channelSize:     1,
	}
	// parse configs from env
	mqttConfig := MQTTConfig{
		ClientID:  "inkbird-iam-t1_" + time.Now().UTC().Format(time.RFC3339),
		ServerURL: os.Getenv("MQTT_SERVER"),
		Username:  os.Getenv("MQTT_USERNAME"),
		Password:  os.Getenv("MQTT_PASSWORD"),
	}
	mqtt, err := NewMQTT(context.Background(), mqttConfig, logger)
	assert("mqtt", err)
	defer func() {
		assert("mqtt close", mqtt.Close())
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Registration
	adapter := bluetooth.DefaultAdapter
	assert("enable adapter", adapter.Enable())

	readings := make(chan Reading, cfg.channelSize)
	device := registerDevice(logger, adapter, cfg, readings)
	defer func() {
		logger.Info("disconnecting device")
		if err := device.Disconnect(); err != nil {
			logger.Error("disconnect failed", "error", err)
		}
	}()

	// Normal operation
	go collectReadings(ctx, logger, readings, *mqtt)

	// Shutdown handling
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	<-sig
	logger.Info("shutdown signal received")
	cancel()

	// Drain remaining readings
	close(readings)
	remaining := 0
	for range readings {
		remaining++
	}
	if remaining > 0 {
		logger.Info("drained buffered readings", "count", remaining)
	}
	logger.Info("shutdown complete")
}

func registerDevice(log *slog.Logger, adapter *bluetooth.Adapter, cfg config, readings chan<- Reading) bluetooth.Device {
	log.Info("scanning for bluetooth device", "addr", cfg.deviceAddr)
	ch := make(chan bluetooth.ScanResult, 1)

	err := adapter.Scan(func(adapter *bluetooth.Adapter, result bluetooth.ScanResult) {
		if result.Address.String() == cfg.deviceAddr {
			if err := adapter.StopScan(); err != nil {
				log.Error("stop scan failed", "error", err)
			}
			ch <- result
		}
	})
	assert("scan", err)

	result := <-ch
	log.Debug("found bluetooth device, connecting", "addr", result.Address)

	device, err := adapter.Connect(result.Address, bluetooth.ConnectionParams{})
	assert("connect", err)
	log.Info("connected to bluetooth device", "addr", result.Address)

	// Discover services
	svcUUID := parseUUID(cfg.sensorServiceID)
	services, err := device.DiscoverServices([]bluetooth.UUID{svcUUID})
	assert("discover services", err)

	for _, service := range services {
		log.Debug("discovered service", "uuid", service.UUID().String())

		// TODO:what more to discover?
		charUUID := parseUUID(cfg.notifyCharID)
		chars, err := service.DiscoverCharacteristics([]bluetooth.UUID{charUUID})
		assert("discover characteristics", err)

		for _, char := range chars {
			log.Info("discovered characteristic", "uuid", char.UUID().String())

			if char.UUID().String() == cfg.notifyCharID {
				err = char.EnableNotifications(func(buf []byte) {
					//TODO: discover whats on the first 3 bytes
					someRaw := int16(buf[2])<<8 | int16(buf[3])
					some := float32(someRaw) / 10.0
					log.Info("bluetooth notification", "rawData", buf, "some", some)
					if reading, ok := parseReading(buf); ok {
						select {
						case readings <- reading:
						default:
							log.Warn("readings channel full, dropping data")
						}
					} else {
						log.Warn("parseReadin failed", "data", buf)
					}
				})
				assert("enable notifications", err)
				log.Info("notifications enabled", "notification", char.UUID().String())
			}
		}
	}

	return device
}

func collectReadings(ctx context.Context, log *slog.Logger, readings chan Reading, mqtt mqttSession) {
	for {
		select {
		case <-ctx.Done():
			log.Info("context done")
			return
		case reading := <-readings:
			log.Info("received reading", "data", reading.String())
			body, err := json.Marshal(reading)
			if err != nil {
				log.Error("json marshal failed", "error", err)
			} else if err := mqtt.Publish(ctx, "inkbird-iam-t1/reading", body); err != nil {
				log.Error("mqtt publish failed", "error", err)
			}
		}
	}
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
