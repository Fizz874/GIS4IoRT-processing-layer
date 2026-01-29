FROM ros:jazzy-ros-base

RUN apt-get update && apt-get install -y \
    ros-jazzy-sensor-msgs \
    ros-jazzy-rmw-cyclonedds-cpp \
    python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install paho-mqtt --break-system-packages --no-cache-dir
