-- STREAMS and CONFIGURATION

SET 'auto.offset.reset' = 'earliest';

CREATE STREAM ros_filtered_odom_stream (
  robot_id VARCHAR KEY,  
  timestamp BIGINT,
  position_x DOUBLE,
  position_y DOUBLE,
  position_z DOUBLE,
  source_topic VARCHAR
) WITH (
  KAFKA_TOPIC='ros_filtered_odom',
  VALUE_FORMAT='JSON',
  TIMESTAMP='timestamp'
);

CREATE STREAM speed_control_stream (
  robot_id VARCHAR KEY,
  status VARCHAR,
  config_names VARCHAR
) WITH (
  KAFKA_TOPIC='speed_control',
  VALUE_FORMAT='JSON',
  PARTITIONS=2
);

CREATE TABLE speed_config_table AS
  SELECT robot_id,
         LATEST_BY_OFFSET(status) AS status,
         LATEST_BY_OFFSET(config_names) AS config_names
  FROM speed_control_stream
  GROUP BY robot_id;

-- SAMPLING 1Hz (1 msg in 1 sec)

CREATE STREAM odom_sampled_1hz AS
SELECT
  o.robot_id AS robot_id,
  o.timestamp AS timestamp,
  o.position_x AS position_x,
  o.position_y AS position_y,
  o.position_z AS position_z,
  c.config_names AS active_configs
FROM ros_filtered_odom_stream o
JOIN speed_config_table c ON o.robot_id = c.robot_id
WHERE c.status = 'ON'
-- Only let through messages in the first 50ms window
AND (o.timestamp % 1000) < 50
EMIT CHANGES;

-- SPEED CALCULATION

CREATE TABLE robot_speed_alerts WITH (
  KAFKA_TOPIC='robot_speed_alerts',
  VALUE_FORMAT='JSON',
  PARTITIONS=2
) AS
SELECT
  robot_id,
  CAST(robot_id AS VARCHAR) AS robot_name,
  TIMESTAMPTOSTRING(WINDOWSTART, 'HH:mm:ss') AS window_start,
  TIMESTAMPTOSTRING(WINDOWEND, 'HH:mm:ss') AS window_end,
  
  LATEST_BY_OFFSET(position_x) AS current_x,
  LATEST_BY_OFFSET(position_y) AS current_y,
  LATEST_BY_OFFSET(active_configs) AS active_configs,
  
  CASE
     WHEN (LATEST_BY_OFFSET(timestamp) - EARLIEST_BY_OFFSET(timestamp)) > 0
     THEN (
        SQRT(
          POWER(LATEST_BY_OFFSET(position_x) - EARLIEST_BY_OFFSET(position_x), 2) +
          POWER(LATEST_BY_OFFSET(position_y) - EARLIEST_BY_OFFSET(position_y), 2) +
          POWER(LATEST_BY_OFFSET(position_z) - EARLIEST_BY_OFFSET(position_z), 2)
        )
      ) / ((LATEST_BY_OFFSET(timestamp) - EARLIEST_BY_OFFSET(timestamp)) / 1000.0)
     -- If time = 0 return 0.0
     ELSE CAST(0.0 AS DOUBLE)
  END AS avg_speed_mps,

  (LATEST_BY_OFFSET(timestamp) - EARLIEST_BY_OFFSET(timestamp)) / 1000.0 AS time_window_s

FROM odom_sampled_1hz
WINDOW HOPPING (SIZE 2 SECONDS, ADVANCE BY 1 SECOND)
GROUP BY robot_id

-- Ignore windows with one point
HAVING (LATEST_BY_OFFSET(timestamp) - EARLIEST_BY_OFFSET(timestamp)) > 800

EMIT CHANGES;





-- GEOFENCE LOGIC

CREATE STREAM ros_gps_stream (
  robot_id VARCHAR KEY,
  timestamp BIGINT,
  latitude DOUBLE,
  longitude DOUBLE,
  altitude DOUBLE
) WITH (
  KAFKA_TOPIC='ros_gps_fix',
  VALUE_FORMAT='JSON',
  TIMESTAMP='timestamp'
);

CREATE STREAM geofence_control_stream (
  robot_id VARCHAR KEY,
  status VARCHAR,
  poly_hex VARCHAR,
  config_names VARCHAR
) WITH (
  KAFKA_TOPIC='geofence_control',
  VALUE_FORMAT='JSON',
  PARTITIONS=2
);

CREATE TABLE geofence_config_table AS
  SELECT robot_id,
         LATEST_BY_OFFSET(status) AS status,
         LATEST_BY_OFFSET(poly_hex) AS poly_hex,
         LATEST_BY_OFFSET(config_names) AS config_names
  FROM geofence_control_stream
  GROUP BY robot_id;

CREATE STREAM gps_with_geofence_check AS
SELECT
  g.robot_id AS robot_id,
  g.timestamp AS timestamp,
  g.latitude AS latitude,
  g.longitude AS longitude,
  c.config_names AS active_configs,
  c.poly_hex AS poly_hex,
  INGEOFENCE(g.latitude, g.longitude, c.poly_hex) AS is_inside
FROM ros_gps_stream g
JOIN geofence_config_table c ON g.robot_id = c.robot_id
WHERE c.status = 'ON'
EMIT CHANGES;

CREATE TABLE robot_geofence_status WITH (
  KAFKA_TOPIC='robot_geofence_alerts',
  VALUE_FORMAT='JSON',
  PARTITIONS=2
) AS
SELECT
  robot_id,
  CAST(robot_id AS VARCHAR) AS robot_name,
  TIMESTAMPTOSTRING(WINDOWSTART, 'HH:mm:ss') AS window_start,
  TIMESTAMPTOSTRING(WINDOWEND, 'HH:mm:ss') AS window_end,
  TIMESTAMPTOSTRING(LATEST_BY_OFFSET(timestamp), 'HH:mm:ss') AS check_time,
  LATEST_BY_OFFSET(latitude) AS current_lat,
  LATEST_BY_OFFSET(longitude) AS current_lon,
  LATEST_BY_OFFSET(active_configs) AS active_configs,
  CASE 
    WHEN MIN(CASE WHEN is_inside THEN 1 ELSE 0 END) = 0 THEN TRUE
    ELSE FALSE
  END AS is_out_of_bounds
FROM gps_with_geofence_check
WINDOW TUMBLING (SIZE 1 SECOND)
GROUP BY robot_id
EMIT FINAL;