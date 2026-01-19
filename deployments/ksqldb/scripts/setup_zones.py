#!/usr/bin/env python3
"""
Setup script to register zones before running tests.
Run this once before running tests: python3 scripts/setup_zones.py
"""

import requests
import json

API_BASE_URL = "http://localhost:8000/ksqldb"
HEADERS = {"Content-Type": "application/json"}

# Zone definition from GEOFLINK CSV
ZONES = [
    {
        "id": "1 MONT",
        "geo": "0103000020E61000000100000005000000C081182C28780B4068436B9D732B4740803F61E91B770B4074FEB9DD6E2B474080629C0F4C770B4010DE9EC4602B474000977541CD780B403C520AF8692B4740C081182C28780B4068436B9D732B4740"
    }
]

def register_zones():
    print("=== ZONE REGISTRATION ===")
    
    for zone in ZONES:
        print(f"\nRegistering zone: {zone['id']}")
        
        payload = {
            "id": zone["id"],
            "geo": zone["geo"]
        }
        
        try:
            resp = requests.post(f"{API_BASE_URL}/zones", json=payload, headers=HEADERS)
            
            if resp.status_code == 200:
                print(f"✓ Zone '{zone['id']}' registered successfully")
            else:
                print(f"✗ Error: {resp.status_code} - {resp.text}")
        except Exception as e:
            print(f"✗ Critical error: {e}")
    
    print("\n=== ZONE REGISTRATION COMPLETE ===")

def register_sensors():
    print("\n=== SENSOR REGISTRATION ===")
    
    sensors = ["1", "2", "3", "4", "5", "6"]
    
    for sensor_id in sensors:
        print(f"Registering sensor {sensor_id}...")
        payload = {"sensor_id": sensor_id}
        
        try:
            resp = requests.post(f"{API_BASE_URL}/sensors", json=payload, headers=HEADERS)
            if resp.status_code in [200, 409]:
                print(f"✓ Sensor {sensor_id} registered")
            else:
                print(f"✗ Error: {resp.text}")
        except Exception as e:
            print(f"✗ Critical: {e}")

def register_robots():
    print("\n=== ROBOT REGISTRATION ===")
    
    robots = ["unit1", "unit2"]
    
    for robot_id in robots:
        print(f"Registering robot {robot_id}...")
        payload = {"id": robot_id}
        
        try:
            resp = requests.post(f"{API_BASE_URL}/robots", json=payload, headers=HEADERS)
            # 200 = Created/Updated, 409 = Conflict (already exists, which is fine)
            if resp.status_code in [200, 409]:
                print(f"✓ Robot {robot_id} registered")
            else:
                print(f"✗ Error: {resp.status_code} - {resp.text}")
        except Exception as e:
            print(f"✗ Critical: {e}")
            
if __name__ == "__main__":
    register_zones()
    register_sensors()
    register_robots()