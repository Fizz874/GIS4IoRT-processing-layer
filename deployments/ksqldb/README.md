# ksqlDB for GIS4IoRT Chist-Era Project

## Usage

### Starting testing
Navigate to the deployment directory:
cd GIS4IoRT-processing-layer/deployments/ksqldb


**1. In Terminal 1**:
docker compose up -d --build


**2. In Terminal 2**:
docker compose exec -it ros2_bridge /bin/bash -c "source /opt/ros/humble/setup.bash && python3 /scripts/multi_robot_kafka_bridgeKSQLDB.py"


**3. In Terminal 1**:
python3 scripts/setup_zones.py


**4. In Terminal 1**:
python3 scripts/run_testsKSQLDB.py


**5. To kill everything (Terminal 1)**:
docker compose down -v

---

### Starting general usage
Navigate to the deployment directory:
cd GIS4IoRT-processing-layer/deployments/ksqldb

**1. In Terminal 1**:
docker compose up -d --build

**2. System Configuration**:
Configure the system via the API (http://localhost:8000). You can use the FastAPI Swagger UI at http://localhost:8000/docs to:
* register robots and sensors,
* send zone definitions and humidity sensor settings,
* activate the desired queries.

**3. In Terminal 2**:
docker compose exec -it ros2_bridge /bin/bash -c "source /opt/ros/humble/setup.bash && python3 /scripts/ros2_live_bridge.py"


**4. In Terminal 3**:
docker compose exec -it python-env python3 /scripts/sensor_realtime_generator.py


**5. In Terminal 4** (Change ROS2_DIRECTORY to the desired data):
docker compose exec -it ros2_bridge /bin/bash -c "source /opt/ros/humble/setup.bash && ros2 bag play /bags/ROS2_DIRECTORY"

**6. To kill everything (Terminal 1)**:
docker compose down -v
