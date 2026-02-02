\c postgres
CREATE SCHEMA temp_monitoring;

CREATE TABLE IF NOT EXISTS temp_monitoring.temperatire_readings (
	reading_id SERIAL PRIMARY KEY,
	device_id int NOT NULL,
	status_id int NOT NULL,
	date TIMESTAMPTZ NOT NULL,
	temperature float NOT NULL,
	FOREIGN KEY (device_id) REFERENCES temp_monitoring(device),
	FOREIGN KEY (status_id) REFERENCES temp_monitoring(status)	
);

CREATE TABLE IF NOT EXISTS temp_monitoriing.device (
	device_id SERIAL PRIMARY KEY,
	device_name text NOT NULL
);

CREATE TABLE IF NOT EXISTS temp_monitoring.status (
	status_id SERIAL PRIMARY KEY,
	status_name text NOT NULL
);



