import logging

from app.adapters.geoflink import database
from .consumer_manager import consumer_manager
from .kafka_service import kafka_service
from .flink_service import get_flink_service
from .schemas import ZoneCreate

logger = logging.getLogger("uvicorn.info")

async def restore_application_state():
    configs = database.get_all_configs()
    flink_service = get_flink_service()

    logger.info(f"Starting System Recovery for {len(configs)} configurations...")
    restored_count = 0
    
    for config in configs:
        config_name = config["name"]
        saved_job_id = config["flink_job_id"]
        
        output_topic = config.get("output_topic") or f"output_{config_name}"


        assignments = database.get_geofence_assignments(config_name)
        has_rules = len(assignments) > 0

        is_running = await flink_service.is_job_running(saved_job_id)

        if is_running:
            logger.info(f"Job {saved_job_id} ({config_name}) is already RUNNING.")
            await kafka_service.create_topic(output_topic)
            await consumer_manager.start_consumer(output_topic)

            continue

        else:
            if not has_rules:
                if saved_job_id: 
                    logger.info(f"Cleaning up stale Job ID {saved_job_id} from database.")
                    database.update_job_status(config_name, None)
                continue
            
            logger.warning(f"Job for '{config_name}' is DEAD but has {len(assignments)} rules. Resurrecting...")
            
            try:
                control_topic = await flink_service.ensure_running(config_name)

                logger.info(f"Reloading {len(assignments)} rules for '{config_name}'...")
                await reload_assignments_to_kafka(config_name, control_topic, assignments)
                
                restored_count += 1
                logger.info(f"Configuration '{config_name}' successfully restored.")
                
            except Exception as e:
                logger.error(f"Failed to restore '{config_name}': {e}")

    logger.info(f"Recovery complete. Resurrected {restored_count} jobs.")

async def reload_assignments_to_kafka(config_name: str, topic: str, assignments: list):

    unique_zone_ids = set()
    robot_zone_map = {}

    for assignment in assignments:
        r_id = assignment['robot_id']
        z_id = assignment['zone_id']

        if z_id:
            unique_zone_ids.add(z_id)

        if r_id and z_id:
            if r_id not in robot_zone_map:
                robot_zone_map[r_id] = []
            robot_zone_map[r_id].append(z_id)

    logger.info(f"Reloading state: {len(unique_zone_ids)} zones and permissions for {len(robot_zone_map)} robots to '{topic}'")
    
    

    for zone_id in unique_zone_ids:
        try:
            zone_data = database.get_zone(zone_id)
            if zone_data:
                await kafka_service.send_zone(ZoneCreate(**zone_data), topic)
        except Exception as e:
            logger.error(f"Failed to replay zone {zone_id}: {e}")

    for robot_id, zones_list in robot_zone_map.items():
        try:
            robot_data = database.get_robot(robot_id)
            
            if robot_data:
                await kafka_service.send_robot_assignment(
                    robot_id=robot_id, 
                    zone_ids=zones_list, 
                    topic=topic
                )
            else:
                logger.warning(f"Skipping assignment for deleted robot {robot_id}")
                
        except Exception as e:
            logger.error(f"Failed to replay permissions for robot {robot_id}: {e}")