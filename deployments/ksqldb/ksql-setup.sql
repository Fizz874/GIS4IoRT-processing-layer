--++-- 0. BASE STREAMS

SET 'auto.offset.reset' = 'earliest';
SET 'max.task.idle.ms' = '0'; --by procesować wiadomość w oknie od soil humidity

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

CREATE STREAM sensor_raw_stream (
  sensor_id VARCHAR KEY,
  timestamp BIGINT,
  position_x DOUBLE, 
  position_y DOUBLE, 
  humidity DOUBLE
) WITH (
  KAFKA_TOPIC='sensor_proximity',
  VALUE_FORMAT='JSON',
  TIMESTAMP='timestamp'
);




--++-- 1. REGISTERED STREAMS
-- Robot registry
CREATE TABLE robot_registry (
  robot_id VARCHAR PRIMARY KEY,
  status VARCHAR
) WITH (
  KAFKA_TOPIC='robot_registration', 
  VALUE_FORMAT='JSON',
  PARTITIONS=2
);

-- GPS drop unregistered
CREATE STREAM ros_gps_registered WITH (PARTITIONS=2) AS
SELECT 
  s.robot_id AS robot_id,
  s.timestamp,
  s.latitude,
  s.longitude,
  s.altitude
FROM ros_gps_stream s
JOIN robot_registry r ON s.robot_id = r.robot_id
WHERE r.status = 'REGISTERED'
PARTITION BY s.robot_id;

-- Odometry drop unregistered
CREATE STREAM ros_odom_registered WITH (PARTITIONS=2) AS
SELECT 
  s.robot_id AS robot_id,
  s.timestamp,
  s.position_x,
  s.position_y,
  s.position_z
FROM ros_filtered_odom_stream s
JOIN robot_registry r ON s.robot_id = r.robot_id
WHERE r.status = 'REGISTERED'
PARTITION BY s.robot_id;

-- Sensor registry
CREATE TABLE sensor_registry (
  sensor_id VARCHAR PRIMARY KEY,
  status VARCHAR
) WITH (
  KAFKA_TOPIC='sensor_registration', 
  VALUE_FORMAT='JSON',
  PARTITIONS=2
);

-- Sensors drop unregistered
CREATE STREAM sensor_raw_registered WITH (PARTITIONS=2) AS
SELECT 
  s.sensor_id AS sensor_id,
  s.timestamp,
  s.position_x, 
  s.position_y, 
  s.humidity
FROM sensor_raw_stream s
JOIN sensor_registry r ON s.sensor_id = r.sensor_id
WHERE r.status = 'REGISTERED'
PARTITION BY s.sensor_id;






--++-- 2. SPEED MONITORING
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

-- activated
CREATE STREAM odom_active_speed_stream AS
SELECT
  o.robot_id AS robot_id,
  o.timestamp AS timestamp,
  o.position_x AS position_x,
  o.position_y AS position_y,
  o.position_z AS position_z,
  c.config_names AS active_configs
FROM ros_odom_registered o
JOIN speed_config_table c ON o.robot_id = c.robot_id
WHERE c.status = 'ON'
  AND o.timestamp IS NOT NULL
EMIT CHANGES;

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
  
  SQRT(
    POWER(LATEST_BY_OFFSET(position_x) - EARLIEST_BY_OFFSET(position_x), 2) +
    POWER(LATEST_BY_OFFSET(position_y) - EARLIEST_BY_OFFSET(position_y), 2) +
    POWER(LATEST_BY_OFFSET(position_z) - EARLIEST_BY_OFFSET(position_z), 2)
  ) / ((MAX(timestamp) - MIN(timestamp)) / 1000.0) AS avg_speed_mps,

  (MAX(timestamp) - MIN(timestamp)) / 1000.0 AS time_window_s,
  COUNT(*) AS sample_count

FROM odom_active_speed_stream
WINDOW HOPPING (SIZE 2 SECONDS, ADVANCE BY 1 SECOND)

WHERE timestamp IS NOT NULL
GROUP BY robot_id


HAVING 
  COUNT(*) >= 2
  AND (MAX(timestamp) - MIN(timestamp)) > 1500 -- points aren't duplicates
EMIT FINAL;



--++-- 3. GEOFENCE LOGIC
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
FROM ros_gps_registered g
JOIN geofence_config_table c ON g.robot_id = c.robot_id
WHERE c.status = 'ON'
EMIT CHANGES;

--OR AGREGATE THE RESULT:
--CREATE TABLE robot_geofence_status WITH (
--  KAFKA_TOPIC='robot_geofence_alerts',
--  VALUE_FORMAT='JSON',
--  PARTITIONS=2
--) AS
--SELECT
--  robot_id,
--  CAST(robot_id AS VARCHAR) AS robot_name,
--  TIMESTAMPTOSTRING(WINDOWSTART, 'HH:mm:ss') AS window_start,
--  TIMESTAMPTOSTRING(WINDOWEND, 'HH:mm:ss') AS window_end,
--  TIMESTAMPTOSTRING(LATEST_BY_OFFSET(timestamp), 'HH:mm:ss') AS check_time,
--  LATEST_BY_OFFSET(latitude) AS current_lat,
--  LATEST_BY_OFFSET(longitude) AS current_lon,
--  LATEST_BY_OFFSET(active_configs) AS active_configs,
--  --Odkomentować jeśli status report a nie alert:
--  --CASE 
--  --  WHEN MIN(CASE WHEN is_inside THEN 1 ELSE 0 END) = 0 THEN TRUE
--  --  ELSE FALSE
--  --END AS is_out_of_bounds
--  TRUE AS is_out_of_bounds-- zakomentować linijkę jeśli status report a nie alert
--FROM gps_with_geofence_check
--WINDOW TUMBLING (SIZE 1 SECOND)
--GROUP BY robot_id
--HAVING MIN(CASE WHEN is_inside THEN 1 ELSE 0 END) = 0--zakomentować linijkę jeśli status report a nie alert
--EMIT FINAL;

-- OR PUSH IT OUT for tests:
CREATE STREAM robot_geofence_alerts_stream WITH (
  KAFKA_TOPIC='robot_geofence_alerts',
  VALUE_FORMAT='JSON',
  PARTITIONS=2
) AS
SELECT
  'geofence' AS type,
  robot_id AS robot,
  timestamp AS ts,
  latitude AS lat,
  longitude AS lon,
  'OUTSIDE' AS msg,
  active_configs AS zones
FROM gps_with_geofence_check
WHERE is_inside = false
EMIT CHANGES;



--++-- 4. HUMIDITY LOGIC (sharded broadcast)
CREATE STREAM humidity_control_stream (
  sensor_id VARCHAR KEY,
  min_humidity DOUBLE,
  radius_m DOUBLE,
  status VARCHAR
) WITH (
  KAFKA_TOPIC='humidity_control',
  VALUE_FORMAT='JSON'
);

CREATE TABLE humidity_rules_table AS
  SELECT
    sensor_id,
    LATEST_BY_OFFSET(min_humidity) AS limit_humidity,
    LATEST_BY_OFFSET(radius_m) AS limit_radius,
    LATEST_BY_OFFSET(status) AS status
  FROM humidity_control_stream
  GROUP BY sensor_id;

CREATE STREAM sensor_data_with_rules AS
  SELECT
    s.sensor_id AS sensor_id,
    s.timestamp,
    s.position_x AS sensor_lon,
    s.position_y AS sensor_lat,
    s.humidity AS current_humidity,
    r.limit_humidity AS limit_humidity,
    r.limit_radius AS limit_radius
  FROM sensor_raw_registered s
  JOIN humidity_rules_table r ON s.sensor_id = r.sensor_id
  WHERE r.status = 'ON';

-- Sensors Explode (manual broadcast)
-- Generates 2 copies of every sensor reading (Shard P0 and Shard P1)
CREATE STREAM sensors_exploded AS
  SELECT
    sensor_id,
    timestamp,
    sensor_lat,
    sensor_lon,
    current_humidity,
    limit_radius,
    limit_humidity,
    EXPLODE(ARRAY['P0', 'P1']) AS shard_link
  FROM sensor_data_with_rules;

-- Sensors, Repartition
CREATE STREAM sensors_broadcast WITH (PARTITIONS=2) AS
  SELECT * FROM sensors_exploded
  PARTITION BY shard_link;

-- Robots step 1, Last digit split
CREATE STREAM robots_pre_shard AS
  SELECT 
    robot_id,
    timestamp,
    latitude,
    longitude,
    CASE 
      WHEN SUBSTRING(robot_id, LEN(robot_id), 1) IN ('1', '3', '5', '7', '9') THEN 'P1'
      ELSE 'P0'
    END AS shard_link
  FROM ros_gps_registered;

-- ROBOTS step 2, Repartition
CREATE STREAM robots_sharded WITH (PARTITIONS=2) AS 
  SELECT *
  FROM robots_pre_shard
  PARTITION BY shard_link;

-- Final JOIN
--CREATE STREAM robot_humidity_alerts WITH (
--  KAFKA_TOPIC='robot_humidity_alerts',
--  VALUE_FORMAT='JSON',
--  PARTITIONS=2
--) AS
--  SELECT 
--    r.shard_link,
--    r.robot_id,
--    s.sensor_id,
--    TIMESTAMPTOSTRING(r.timestamp, 'HH:mm:ss') AS time,
--    s.current_humidity,
--    s.limit_humidity,
--    GEO_DISTANCE(r.latitude, r.longitude, s.sensor_lat, s.sensor_lon) * 1000 AS dist_m
--  FROM robots_sharded r
--  JOIN sensors_broadcast s
--    WITHIN 1800 SECONDS  --30 minut
--    ON r.shard_link = s.shard_link
--  WHERE 
--    (GEO_DISTANCE(r.latitude, r.longitude, s.sensor_lat, s.sensor_lon) * 1000) < s.limit_radius
--    AND s.current_humidity > s.limit_humidity 
--  EMIT CHANGES;



-- Push for testing Final JOIN timestamps table join:
CREATE STREAM robot_humidity_alerts WITH (
  KAFKA_TOPIC='robot_humidity_alerts',
  VALUE_FORMAT='JSON',
  PARTITIONS=2
) AS
  SELECT
    r.shard_link,
    'humidity' AS type,
    r.robot_id AS robot,
    r.timestamp AS ts,
    r.latitude AS lat,
    r.longitude AS lon,
    s.sensor_id AS sensor,
    s.current_humidity AS humidity,
    s.limit_humidity AS threshold,
    GEO_DISTANCE(r.latitude, r.longitude, s.sensor_lat, s.sensor_lon) * 1000 AS distance_m
  FROM robots_sharded r
  JOIN sensors_broadcast s
    WITHIN 15000 MILLISECONDS -- 1. Widen this buffer to be safe
    ON r.shard_link = s.shard_link
  WHERE 
    (GEO_DISTANCE(r.latitude, r.longitude, s.sensor_lat, s.sensor_lon) * 1000) < s.limit_radius
    AND s.current_humidity > s.limit_humidity 
    -- 2. Strict Time Boxing to eliminate duplicates:
    AND s.timestamp <= r.timestamp              -- Must be in the past
    AND s.timestamp > (r.timestamp - 10000)     -- Must be newer than 10s ago
  EMIT CHANGES;