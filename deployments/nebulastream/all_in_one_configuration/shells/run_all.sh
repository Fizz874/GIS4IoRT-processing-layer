docker compose up nes_coordinator -d
docker compose up nes_ui -d
docker compose up mosquitto -d
sleep 3

docker compose up ros2_mqtt_leader -d
docker compose up ros2_mqtt_follower -d
docker compose up ros2_mqtt_leader_inv -d
sleep 3

docker compose up nes_worker_leader -d
sleep 3
docker compose up nes_worker_follower -d
sleep 3
docker compose up nes_worker_leader_inv -d
sleep 3
docker compose up nes_worker_humidity -d
sleep 3

docker compose up geofence_query -d
sleep 5
docker compose up humidity_query -d
sleep 5
docker compose up collision_query -d
sleep 5

docker compose up visualizer -d
sleep 3

docker compose up humidity_producer -d
docker compose up ros2_play_leader -d
docker compose up ros2_play_follower -d
docker compose up ros2_play_leader_inv -d

docker compose up subscriber
