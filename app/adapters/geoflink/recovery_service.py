import logging

from app.adapters.geoflink import database
from .consumer_manager import consumer_manager
from .kafka_service import kafka_service
from .flink_service import get_flink_service
from .schemas import RobotCreate, ZoneCreate

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

    unique_robot_ids = set()
    unique_zone_ids = set()

    for assignment in assignments:
        if assignment['robot_id']:
            unique_robot_ids.add(assignment['robot_id'])
        if assignment['zone_id']:
            unique_zone_ids.add(assignment['zone_id'])

    logger.info(f"Sending {len(unique_robot_ids)} robots and {len(unique_zone_ids)} zones to '{topic}'")

    for robot_id in unique_robot_ids:
        try:
            robot_data = database.get_robot(robot_id)
            if robot_data:
                await kafka_service.send_robot(RobotCreate(**robot_data), topic)
        except Exception as e:
            logger.error(f"Failed to replay robot {robot_id}: {e}")

    for zone_id in unique_zone_ids:
        try:
            zone_data = database.get_zone(zone_id)
            if zone_data:
                await kafka_service.send_zone(ZoneCreate(**zone_data), topic)
        except Exception as e:
            logger.error(f"Failed to replay zone {zone_id}: {e}")