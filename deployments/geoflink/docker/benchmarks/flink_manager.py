import requests
import time
import json
import sys

# --- API CONFIGURATION ---
API_BASE_URL = "http://localhost:8000/geoflink"
HEADERS = {"Content-Type": "application/json"}

# ==========================================
# 1. JOB CONFIGURATION TEMPLATES (Flink Engine)
# ==========================================

TEMPLATE_GEOFENCE = {
    "type": "GEOFENCE",
    "parallelism": 2,
    "bootStrapServers": "broker:29092",
    "localWebUi": False,
    "inputTopicName": "multi_gps_fix",
    "range": 0.0000001,
    "cellLengthMeters": 0,
    "uniformGridSize": 100,
    "gridMinX": 3.430,
    "gridMinY": 46.336,
    "gridMaxX": 3.436,
    "gridMaxY": 46.342
}

TEMPLATE_SENSOR = {
    "type": "SENSOR",
    "parallelism": 2,
    "bootStrapServers": "broker:29092",
    "localWebUi": False,
    "inputTopicName": "multi_gps_fix",
    "sensorTopicName": "sensor_proximity",
    "cellLengthMeters": 0,
    "uniformGridSize": 100,
    "gridMinX": 3.430,
    "gridMinY": 46.336,
    "gridMaxX": 3.436,
    "gridMaxY": 46.342
}

TEMPLATE_COLLISION = {
    "type": "COLLISION",
    "parallelism": 2,
    "bootStrapServers": "broker:29092",
    "localWebUi": False,
    "inputTopicName": "multi_gps_fix",
    "collisionThreshold": 6,
    "robotStateTtlMillis": 5000,
    "robotAlertCooldownMillis": 0,
    "cellLengthMeters": 0,
    "uniformGridSize": 100,
    "gridMinX": 3.430,
    "gridMinY": 46.336,
    "gridMaxX": 3.436,
    "gridMaxY": 46.342
}

# ==========================================
# 2. SCENARIO DEFINITIONS (BUSINESS PAYLOADS)
# ==========================================
# Here you define object lists for each iteration.

SCENARIO_RULES = {
    # 1: {
    #      "robots": [
    #         {"robot_id": "follower"},
    #         {"robot_id": "leader"}
    #     ]
    # },
    # ... (commented out code preserved)
    #--- ITERATION 1: GEOFENCE ---
    1: {
        "geofences": [
            {"robot_id": "follower", "zone_id": "1 MONT"},
            {"robot_id": "leader",   "zone_id": "1 MONT"}
        ]
    },
    2: {
        "geofences": [
            {"robot_id": "follower", "zone_id": "1 MONT"},
            {"robot_id": "leader",   "zone_id": "1 MONT"}
        ]
    },

    3: {
        "geofences": [
            {"robot_id": "follower", "zone_id": "1 MONT"},
            {"robot_id": "leader",   "zone_id": "1 MONT"}
        ]
    },
    # --- ITERATIONS 4-6: SENSOR (Test Configuration) ---
    4: {
    "sensors": [
        {"sensor_id": "1", "radius": 3.0, "humidity_threshold": 80.0},
        {"sensor_id": "2", "radius": 3.0, "humidity_threshold": 80.0},
        {"sensor_id": "3", "radius": 3.0, "humidity_threshold": 80.0},
        {"sensor_id": "4", "radius": 3.0, "humidity_threshold": 80.0},
        {"sensor_id": "5", "radius": 3.0, "humidity_threshold": 80.0},
        {"sensor_id": "6", "radius": 3.0, "humidity_threshold": 80.0}
    ],
    "robots": [
        {"robot_id": "follower"},
        {"robot_id": "leader"}
    ]
    },

    5: {
        "sensors": [
            {"sensor_id": "1", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "2", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "3", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "4", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "5", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "6", "radius": 3.0, "humidity_threshold": 80.0}
        ],
        "robots": [
            {"robot_id": "follower"},
            {"robot_id": "leader"}
        ]
    },

    6: {
        "sensors": [
            {"sensor_id": "1", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "2", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "3", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "4", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "5", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "6", "radius": 3.0, "humidity_threshold": 80.0}
        ],
        "robots": [
            {"robot_id": "follower"},
            {"robot_id": "leader"}
        ]
    },

    # --- ITERATIONS 7-9: COLLISION ---
    7: {
        "robots": [
            {"robot_id": "follower"},
            {"robot_id": "leader"}
        ]
    },
    8: {
        "robots": [
            {"robot_id": "follower"},
            {"robot_id": "leader"}
        ]
    },
    9: {
        "robots": [
            {"robot_id": "follower"},
            {"robot_id": "leader"}
        ]
    }

}

DEFAULT_ROS_SETTINGS = {
    "bag_file": "/app/data/files/leader_follower/leader_follower.db3",
    "topics": "/leader/gps/fix /follower/gps/fix"
}

# Map assigning config type to iteration
TEST_CONFIGS_MAP = {
    # ... (commented out code preserved)
    1: {
        **TEMPLATE_GEOFENCE, 
        "ros_settings": DEFAULT_ROS_SETTINGS
    },
    2: {
        **TEMPLATE_GEOFENCE, 
        "ros_settings": DEFAULT_ROS_SETTINGS
    },
    3: {
        **TEMPLATE_GEOFENCE, 
        "ros_settings": DEFAULT_ROS_SETTINGS
    },
    4: {
        **TEMPLATE_SENSOR,
        "ros_settings": DEFAULT_ROS_SETTINGS
    },
    5: {
        **TEMPLATE_SENSOR,
        "ros_settings": DEFAULT_ROS_SETTINGS
    },
    6: {
        **TEMPLATE_SENSOR,
        "ros_settings": DEFAULT_ROS_SETTINGS
    },
    7: {
        **TEMPLATE_COLLISION,
        "ros_settings": #DEFAULT_ROS_SETTINGS
        {
            "bag_file": "/app/data/files/leader_inv_follower/leader_inv_follower.db3",
            "topics": "/leader/gps/fix /follower/gps/fix"
        }
    },
    8: {
        **TEMPLATE_COLLISION,
        "ros_settings": #DEFAULT_ROS_SETTINGS
        {
            "bag_file": "/app/data/files/leader_inv_follower/leader_inv_follower.db3",
            "topics": "/leader/gps/fix /follower/gps/fix"
        }
    },
    9: {
        **TEMPLATE_COLLISION,
        "ros_settings": #DEFAULT_ROS_SETTINGS
        #
        {
            "bag_file": "/app/data/files/leader_inv_follower/leader_inv_follower.db3",
            "topics": "/leader/gps/fix /follower/gps/fix"
        }
    }
   
}


# ==========================================
# 3. SENDING LOGIC (Loop & Send)
# ==========================================

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

def configure_sensor_rules(config_name, iteration):
    """
    Handles two endpoints: sensor definition and robot assignment.
    """
    data = SCENARIO_RULES.get(iteration, {})
    sensors = data.get("sensors", [])
    robots = data.get("robots", [])

    print(f"   [API] SENSOR Configuration: {len(sensors)} sensors, {len(robots)} robots...")

    # A. Defining sensors (POST /sensor)
    for i, s in enumerate(sensors):
        payload = {
            "config_name": config_name,
            "sensor_id": s["sensor_id"],
            "radius": s["radius"],
            "humidity_threshold": s["humidity_threshold"]
        }
        try:
            resp = requests.post(f"{API_BASE_URL}/sensor", json=payload, headers=HEADERS)
            if resp.status_code != 200:
                print(f"      [Sensor {s['sensor_id']}] Error: {resp.text}")
        except Exception as e:
            print(f"      [Sensor {s['sensor_id']}] Critical: {e}")

    # B. Adding robots (POST /sensor/robot)
    for i, r in enumerate(robots):
        payload = {
            "config_name": config_name,
            "robot_id": r["robot_id"]
        }
        try:
            resp = requests.post(f"{API_BASE_URL}/sensor/robot", json=payload, headers=HEADERS)
            if resp.status_code != 200:
                print(f"      [Robot {r['robot_id']}] Error: {resp.text}")
        except Exception as e:
             print(f"      [Robot {r['robot_id']}] Critical: {e}")

def configure_collision_rules(config_name, iteration):
    """Iterates through robot list for collision"""
    robots = SCENARIO_RULES.get(iteration, {}).get("robots", [])
    
    print(f"   [API] Uploading {len(robots)} robots to COLLISION...")

    for i, r in enumerate(robots):
        payload = {
            "config_name": config_name,
            "robot_id": r["robot_id"]
        }
        try:
            resp = requests.post(f"{API_BASE_URL}/collision", json=payload, headers=HEADERS)
            if resp.status_code != 200:
                 print(f"      [{r['robot_id']}] Error: {resp.text}")
        except Exception as e:
            print(f"      [{r['robot_id']}] Critical: {e}")


# ==========================================
# 4. HELPER FUNCTIONS
# ==========================================

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

    # 1. Calculate Output Topic
    # Convention from your API: output_{config_name}
    unique_name = get_unique_name(iteration_number, config['type'])
    output_topic = f"output_{unique_name}"

    # 2. Build ROS Command
    ros_settings = config.get("ros_settings", DEFAULT_ROS_SETTINGS)
    bag_file = ros_settings["bag_file"]
    topics = ros_settings["topics"]
    
    # You can also parameterize RATE here if needed
    ros_cmd = (
        "source /opt/ros/jazzy/setup.bash && "
        f"ros2 bag play {bag_file} --rate 20.0 "
        f"--topics {topics}"
    )

    return {
        "config_name": unique_name,
        "output_topic": output_topic,
        "ros_cmd": ros_cmd
    }


def delete_config_if_exists(config_name):
    url = f"{API_BASE_URL}/config/{config_name}"
    try:
        requests.delete(url)
    except: pass
    time.sleep(1) 

def create_config(payload):
    url = f"{API_BASE_URL}/config"
    try:
        print(f"   [API] Registering Job: {payload['name']} (Type: {payload['type']})...")
        response = requests.post(url, json=payload, headers=HEADERS)
        if response.status_code == 200:
            return True
        elif response.status_code == 409:
            print(f"   [API ERROR] Config conflict (already exists).")
            return False
        else:
            print(f"   [API ERROR] {response.text}")
            return False
    except Exception as e:
        print(f"   [API CRITICAL] {e}")
        return False

# ==========================================
# 5. MAIN DEPLOYMENT FUNCTION
# ==========================================

def deploy_configuration(iteration_number):
    print(f"\n   [FLINK MANAGER] Configuration for iteration #{iteration_number}")
    

    config_payload = TEST_CONFIGS_MAP.get(iteration_number)
    if not config_payload:
        print(f"   [WARN] No configuration for iteration {iteration_number}.")
        return None

    unique_name = get_unique_name(iteration_number, config_payload['type'])
    payload_to_send = config_payload.copy()
    payload_to_send['name'] = unique_name

    delete_config_if_exists(unique_name)
    
    if not create_config(payload_to_send):
        raise RuntimeError("Flink Config Deployment Failed")

    print("   [FLINK MANAGER] Waiting 5s for Flink engine start...")
    time.sleep(5)

    job_type = config_payload['type']

    if job_type == 'GEOFENCE':
        configure_geofence_rules(unique_name, iteration_number)
        
    elif job_type == 'SENSOR':
        configure_sensor_rules(unique_name, iteration_number)
        
    elif job_type == 'COLLISION':
        configure_collision_rules(unique_name, iteration_number)

    return unique_name

def cleanup_job(iteration_number):
    config_payload = TEST_CONFIGS_MAP.get(iteration_number)
    if config_payload:
        unique_name = get_unique_name(iteration_number, config_payload['type'])
        delete_config_if_exists(unique_name)