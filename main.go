package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tinygo.org/x/bluetooth"
)

type config struct {
	deviceAddr      string
	sensorServiceID string
	notifyCharID    string
	channelSize     int
}

type Reading struct {
	Timestamp uint32 // unix seconds (valid until 2106)
	CO2       uint16 // ppm
	Humidity  uint16 // tenths of percent (720 = 72.0%)
	Pressure  uint16 // hPa
}

func (r Reading) String() string {
	ts := time.Unix(int64(r.Timestamp), 0).Format("15:04:05")
	humidity := float64(r.Humidity) / 10.0
	return fmt.Sprintf("%s | CO2: %4d ppm | Humidity: %4.1f%% | Pressure: %4d hPa",
		ts, r.CO2, humidity, r.Pressure)
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := config{
		deviceAddr:      "62:00:A1:3F:B4:26",
		sensorServiceID: "0000ffe0-0000-1000-8000-00805f9b34fb",
		notifyCharID:    "0000ffe4-0000-1000-8000-00805f9b34fb",
		channelSize:     10000,
	}

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
	go collectReadings(ctx, logger, readings)

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

func registerDevice(logger *slog.Logger, adapter *bluetooth.Adapter, cfg config, readings chan<- Reading) bluetooth.Device {
	logger.Info("scanning for device", "addr", cfg.deviceAddr)
	ch := make(chan bluetooth.ScanResult, 1)

	err := adapter.Scan(func(adapter *bluetooth.Adapter, result bluetooth.ScanResult) {
		if result.Address.String() == cfg.deviceAddr {
			if err := adapter.StopScan(); err != nil {
				logger.Error("stop scan failed", "error", err)
			}
			ch <- result
		}
	})
	assert("scan", err)

	result := <-ch
	logger.Info("found device, connecting")

	device, err := adapter.Connect(result.Address, bluetooth.ConnectionParams{})
	assert("connect", err)
	logger.Info("connected")

	// Discover services
	svcUUID := parseUUID(cfg.sensorServiceID)
	services, err := device.DiscoverServices([]bluetooth.UUID{svcUUID})
	assert("discover services", err)

	for _, service := range services {
		logger.Debug("discovered service", "uuid", service.UUID().String())

		charUUID := parseUUID(cfg.notifyCharID)
		chars, err := service.DiscoverCharacteristics([]bluetooth.UUID{charUUID})
		assert("discover characteristics", err)

		for _, char := range chars {
			logger.Debug("discovered characteristic", "uuid", char.UUID().String())

			if char.UUID().String() == cfg.notifyCharID {
				err = char.EnableNotifications(func(buf []byte) {
					if reading, ok := parseReading(buf); ok {
						select {
						case readings <- reading:
						default:
							logger.Warn("readings channel full, dropping data")
						}
					}
				})
				assert("enable notifications", err)
				logger.Info("notifications enabled")
			}
		}
	}

	return device
}

func collectReadings(ctx context.Context, logger *slog.Logger, readings chan Reading) {
	for {
		select {
		case <-ctx.Done():
			return
		case reading := <-readings:
			logger.Info("reading", "data", reading.String())
		}
	}
}

func parseReading(data []byte) (Reading, bool) {
	if len(data) < 13 {
		return Reading{}, false
	}

	return Reading{
		Timestamp: uint32(time.Now().Unix()),
		Humidity:  uint16(data[7])<<8 | uint16(data[8]),
		CO2:       uint16(data[9])<<8 | uint16(data[10]),
		Pressure:  uint16(data[11])<<8 | uint16(data[12]),
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
