\c postgres
CREATE SCHEMA temp_monitoring;

CREATE SCHEMA IF NOT EXISTS temp_monitoring;

ALTER ROLE postgres SET search_path TO temp_monitoring, public;

CREATE TABLE IF NOT EXISTS temp_monitoring.device (
    device_id SERIAL PRIMARY KEY,
    device_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS temp_monitoring.status (
    status_id SERIAL PRIMARY KEY,
    status_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS temp_monitoring.temperature_readings (
    reading_id SERIAL PRIMARY KEY,
    device_id INT NOT NULL,
    status_id INT NOT NULL,
    date TIMESTAMPTZ NOT NULL,
    temperature FLOAT NOT NULL,
    FOREIGN KEY (device_id) REFERENCES temp_monitoring.device(device_id),
    FOREIGN KEY (status_id) REFERENCES temp_monitoring.status(status_id)
);

CREATE TABLE IF NOT EXISTS temp_monitoring.raw_data (
    id SERIAL PRIMARY KEY,
    "room_id/id" TEXT,
    noted_date TIMESTAMPTZ,
    temp FLOAT,
    "out/in" TEXT
);

CREATE TABLE IF NOT EXISTS temp_monitoring.date_raw_data (
    id SERIAL PRIMARY KEY,
    "room_id/id" TEXT,
    noted_date TIMESTAMPTZ,
    temp FLOAT,
    "out/in" TEXT
);

