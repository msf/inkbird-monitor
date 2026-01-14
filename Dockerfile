# Build Stage
FROM golang:1.25-trixie AS build-stage

RUN apt-get update && apt-get install -y \
        ca-certificates

WORKDIR /app

COPY go.mod go.sum /app/
RUN go mod download

COPY . /app/
RUN CGO_ENABLED=0 go build -ldflags="-w -s" -o iam-t1-exporter .

# Final Stage
# Requires: docker run --privileged --network=host -v /var/run/dbus:/var/run/dbus
# --privileged: access to /dev/bus/usb for Bluetooth adapter
# --network=host: BLE requires host network stack
# -v /var/run/dbus: D-Bus access for BlueZ
FROM alpine:3.23
RUN apk add --no-cache \
        ca-certificates \
        bluez \
        dbus

WORKDIR /app

COPY --from=build-stage /app/iam-t1-exporter /app/
RUN chmod +x /app/iam-t1-exporter

CMD ["/app/iam-t1-exporter"]
