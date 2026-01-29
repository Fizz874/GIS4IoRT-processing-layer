import requests
import time
import json
import sys

# API CONFIGURATION
API_BASE_URL = "http://localhost:8000/ksqldb"
HEADERS = {"Content-Type": "application/json"}

# SCENARIO DEFINITIONS

SCENARIO_RULES = {
    # ITERATIONS 1-3: GEOFENCE
    1: {
        "geofences": [
            {"robot_id": "unit1", "zone_id": "1 MONT"},
            {"robot_id": "unit2", "zone_id": "1 MONT"}
        ]
    },
    2: {
        "geofences": [
            {"robot_id": "unit1", "zone_id": "1 MONT"},
            {"robot_id": "unit2", "zone_id": "1 MONT"}
        ]
    },
    3: {
        "geofences": [
            {"robot_id": "unit1", "zone_id": "1 MONT"},
            {"robot_id": "unit2", "zone_id": "1 MONT"}
        ]
    },
    
    # ITERATIONS 4-6: HUMIDITY (SENSOR)
    4: {
        "sensors": [
            {"sensor_id": "1", "min_humidity": 80.0, "alert_radius_m": 3.0},
            {"sensor_id": "2", "min_humidity": 80.0, "alert_radius_m": 3.0},
            {"sensor_id": "3", "min_humidity": 80.0, "alert_radius_m": 3.0},
            {"sensor_id": "4", "min_humidity": 80.0, "alert_radius_m": 3.0},
            {"sensor_id": "5", "min_humidity": 80.0, "alert_radius_m": 3.0},
            {"sensor_id": "6", "min_humidity": 80.0, "alert_radius_m": 3.0}
        ],
        "robots": [
            {"robot_id": "unit1"},
            {"robot_id": "unit2"}
        ]
    },
    5: {
        "sensors": [
            {"sensor_id": "1", "min_humidity": 80.0, "alert_radius_m": 3.0},
            {"sensor_id": "2", "min_humidity": 80.0, "alert_radius_m": 3.0},
            {"sensor_id": "3", "min_humidity": 80.0, "alert_radius_m": 3.0},
            {"sensor_id": "4", "min_humidity": 80.0, "alert_radius_m": 3.0},
            {"sensor_id": "5", "min_humidity": 80.0, "alert_radius_m": 3.0},
            {"sensor_id": "6", "min_humidity": 80.0, "alert_radius_m": 3.0}
        ],
        "robots": [
            {"robot_id": "unit1"},
            {"robot_id": "unit2"}
        ]
    },
    6: {
        "sensors": [
            {"sensor_id": "1", "min_humidity": 80.0, "alert_radius_m": 3.0},
            {"sensor_id": "2", "min_humidity": 80.0, "alert_radius_m": 3.0},
            {"sensor_id": "3", "min_humidity": 80.0, "alert_radius_m": 3.0},
            {"sensor_id": "4", "min_humidity": 80.0, "alert_radius_m": 3.0},
            {"sensor_id": "5", "min_humidity": 80.0, "alert_radius_m": 3.0},
            {"sensor_id": "6", "min_humidity": 80.0, "alert_radius_m": 3.0}
        ],
        "robots": [
            {"robot_id": "unit1"},
            {"robot_id": "unit2"}
        ]
    },

    # ITERATIONS 7-9: COLLISION
    7: {
        "robots": [
            {"robot_id": "unit1"},
            {"robot_id": "unit2"}
        ]
    },
    8: {
        "robots": [
            {"robot_id": "unit1"},
            {"robot_id": "unit2"}
        ]
    },
    9: {
        "robots": [
            {"robot_id": "unit1"},
            {"robot_id": "unit2"}
        ]
    }
}

DEFAULT_ROS_SETTINGS = {
    "bag_file": "/bags/leader_follower/leader_follower.db3",
    "topics": "/leader/gps/fix /follower/gps/fix"
}

# Map assigning config type to iteration
TEST_CONFIGS_MAP = {
    1: {
        "type": "GEOFENCE",
        "ros_settings": DEFAULT_ROS_SETTINGS
    },
    2: {
        "type": "GEOFENCE",
        "ros_settings": DEFAULT_ROS_SETTINGS
    },
    3: {
        "type": "GEOFENCE",
        "ros_settings": DEFAULT_ROS_SETTINGS
    },
    4: {
        "type": "HUMIDITY",
        "ros_settings": DEFAULT_ROS_SETTINGS
    },
    5: {
        "type": "HUMIDITY",
        "ros_settings": DEFAULT_ROS_SETTINGS
    },
    6: {
        "type": "HUMIDITY",
        "ros_settings": DEFAULT_ROS_SETTINGS
    },
    7: {
        "type": "COLLISION",
        "ros_settings": {
            "bag_file": "/bags/leader_inv_follower/leader_inv_follower.db3",
            "topics": "/leader/gps/fix /follower/gps/fix"
        }
    },
    8: {
        "type": "COLLISION",
        "ros_settings": {
            "bag_file": "/bags/leader_inv_follower/leader_inv_follower.db3",
            "topics": "/leader/gps/fix /follower/gps/fix"
        }
    },
    9: {
        "type": "COLLISION",
        "ros_settings": {
            "bag_file": "/bags/leader_inv_follower/leader_inv_follower.db3",
            "topics": "/leader/gps/fix /follower/gps/fix"
        }
    }
}

# CONFIGURATION LOGIC (API Calls)

def configure_geofence_rules(config_name, iteration):
    """Iterates through geofence list and sends to API"""
    rules = SCENARIO_RULES.get(iteration, {}).get("geofences", [])
    
    print(f"   [API] Uploading {len(rules)} GEOFENCE rules...")

    for i, rule in enumerate(rules):
        payload = {
            "config_name": config_name,
            "robot_id": rule["robot_id"],
            "zone_id": rule["zone_id"]
        }
        try:
            resp = requests.post(f"{API_BASE_URL}/geofence", json=payload, headers=HEADERS)
            if resp.status_code != 200:
                print(f"      [{i+1}] Error: {resp.text}")
        except Exception as e:
            print(f"      [{i+1}] Critical: {e}")

def configure_humidity_rules(config_name, iteration):
    """
    Handles two endpoints: sensor definition and humidity rule assignment.
    """
    data = SCENARIO_RULES.get(iteration, {})
    sensors = data.get("sensors", [])
    robots = data.get("robots", [])

    print(f"   [API] HUMIDITY Configuration: {len(sensors)} sensors, {len(robots)} robots...")

    # Register sensors
    for i, s in enumerate(sensors):
        payload = {
            "sensor_id": s["sensor_id"]
        }
        try:
            resp = requests.post(f"{API_BASE_URL}/sensors", json=payload, headers=HEADERS)
            if resp.status_code not in [200, 409]:  # 409 = already exists
                print(f"      [Sensor {s['sensor_id']}] Error: {resp.text}")
        except Exception as e:
            print(f"      [Sensor {s['sensor_id']}] Critical: {e}")

    # Register robots
    for i, r in enumerate(robots):
        payload = {
            "id": r["robot_id"]
        }
        try:
            resp = requests.post(f"{API_BASE_URL}/robots", json=payload, headers=HEADERS)
            if resp.status_code not in [200, 409]:
                print(f"      [Robot {r['robot_id']}] Error: {resp.text}")
        except Exception as e:
             print(f"      [Robot {r['robot_id']}] Critical: {e}")

    # Add humidity rules
    for i, s in enumerate(sensors):
        payload = {
            "config_name": config_name,
            "sensor_id": s["sensor_id"],
            "min_humidity": s["min_humidity"],
            "alert_radius_m": s["alert_radius_m"]
        }
        try:
            resp = requests.post(f"{API_BASE_URL}/humidity", json=payload, headers=HEADERS)
            if resp.status_code != 200:
                print(f"      [Rule {s['sensor_id']}] Error: {resp.text}")
        except Exception as e:
            print(f"      [Rule {s['sensor_id']}] Critical: {e}")

def configure_collision_rules(config_name, iteration):
    """Register robots and activate collision monitoring"""
    robots = SCENARIO_RULES.get(iteration, {}).get("robots", [])
    
    print(f"   [API] Configuring COLLISION detection for {len(robots)} robots...")

    # Register robots
    for i, r in enumerate(robots):
        payload = {
            "id": r["robot_id"]
        }
        try:
            resp = requests.post(f"{API_BASE_URL}/robots", json=payload, headers=HEADERS)
            if resp.status_code not in [200, 409]:
                 print(f"      [{r['robot_id']}] Error: {resp.text}")
        except Exception as e:
            print(f"      [{r['robot_id']}] Critical: {e}")
    
    # Activate collision monitoring
    print(f"   [API] Activating collision monitoring...")
    payload = {
        "config_name": config_name
    }
    try:
        resp = requests.post(f"{API_BASE_URL}/collision/activate", json=payload, headers=HEADERS)
        if resp.status_code == 200:
            print(f"Collision monitoring ACTIVATED")
        else:
            print(f"Activation Error: {resp.text}")
    except Exception as e:
        print(f"Critical: {e}")

# 3. HELPER FUNCTIONS

def get_unique_name(iteration_number, config_type):
    return f"iter_{iteration_number:03d}_{config_type}"

def get_test_metadata(iteration_number):
    """
    Returns a dictionary with dynamic parameters for the given iteration:
    - output_topic: Name of Kafka topic to listen to
    - ros_cmd: Ready bash command to run inside Docker
    """
    config = TEST_CONFIGS_MAP.get(iteration_number)
    if not config:
        return None

    # Calculate Output Topic based on type
    unique_name = get_unique_name(iteration_number, config['type'])
    
    topic_map = {
        "GEOFENCE": "robot_geofence_alerts",
        "HUMIDITY": "robot_humidity_alerts",
        "COLLISION": "robot_collision_alerts"
    }
    output_topic = topic_map.get(config['type'], "robot_geofence_alerts")

    # Build ROS Command
    ros_settings = config.get("ros_settings", DEFAULT_ROS_SETTINGS)
    bag_file = ros_settings["bag_file"]
    topics = ros_settings["topics"]
    
    if config['type'] == 'COLLISION':
        ros_cmd = (
            "source /opt/ros/humble/setup.bash && "
            f"ros2 bag play {bag_file} --rate 1.0 "
            "--remap /leader_inv/gps/fix:=/leader/gps/fix "
            "/follower_inv/gps/fix:=/follower/gps/fix"
        )
    else:
        # Standard behavior for other tests
        topics = ros_settings["topics"]
        ros_cmd = (
            "source /opt/ros/humble/setup.bash && "
            f"ros2 bag play {bag_file} --rate 1.0 "
            f"--topics {topics}"
        )

    return {
        "config_name": unique_name,
        "output_topic": output_topic,
        "ros_cmd": ros_cmd
    }

def cleanup_geofence(config_name, iteration):
    """Remove geofence assignments for this config"""
    rules = SCENARIO_RULES.get(iteration, {}).get("geofences", [])
    
    for rule in rules:
        payload = {
            "config_name": config_name,
            "robot_id": rule["robot_id"],
            "zone_id": rule["zone_id"]
        }
        try:
            requests.delete(f"{API_BASE_URL}/geofence", json=payload, headers=HEADERS)
        except:
            pass

def cleanup_humidity(config_name, iteration):
    """Remove humidity rules for this config"""
    data = SCENARIO_RULES.get(iteration, {})
    sensors = data.get("sensors", [])
    
    for s in sensors:
        payload = {
            "config_name": config_name,
            "sensor_id": s["sensor_id"],
            "min_humidity": s["min_humidity"],
            "alert_radius_m": s["alert_radius_m"]
        }
        try:
            requests.delete(f"{API_BASE_URL}/humidity", json=payload, headers=HEADERS)
        except:
            pass

def cleanup_collision(config_name, iteration):
    """Deactivate collision monitoring"""
    print(f"   [API] Deactivating collision monitoring...")
    try:
        resp = requests.post(f"{API_BASE_URL}/collision/deactivate", headers=HEADERS)
        if resp.status_code == 200:
            print(f"Collision monitoring DEACTIVATED")
        else:
            print(f"Deactivation response: {resp.status_code}")
    except Exception as e:
        print(f"Deactivation error: {e}")

# 4. MAIN DEPLOYMENT FUNCTION

def deploy_configuration(iteration_number):
    print(f"\n   [KSQLDB MANAGER] Configuration for iteration #{iteration_number}")
    
    config_payload = TEST_CONFIGS_MAP.get(iteration_number)
    if not config_payload:
        print(f"   [WARN] No configuration for iteration {iteration_number}.")
        return None

    unique_name = get_unique_name(iteration_number, config_payload['type'])
    job_type = config_payload['type']

    print(f"   [KSQLDB MANAGER] Deploying {job_type} configuration...")
    time.sleep(2)

    if job_type == 'GEOFENCE':
        configure_geofence_rules(unique_name, iteration_number)
        
    elif job_type == 'HUMIDITY':
        configure_humidity_rules(unique_name, iteration_number)
        
    elif job_type == 'COLLISION':
        configure_collision_rules(unique_name, iteration_number)

    return unique_name

def cleanup_job(iteration_number):
    config_payload = TEST_CONFIGS_MAP.get(iteration_number)
    if config_payload:
        unique_name = get_unique_name(iteration_number, config_payload['type'])
        job_type = config_payload['type']
        
        print(f"   [KSQLDB MANAGER] Cleaning up {job_type} for iteration {iteration_number}...")
        
        if job_type == 'GEOFENCE':
            cleanup_geofence(unique_name, iteration_number)
        elif job_type == 'HUMIDITY':
            cleanup_humidity(unique_name, iteration_number)
        elif job_type == 'COLLISION':
            cleanup_collision(unique_name, iteration_number)