import httpx
import logging

import app.actions.client as client

from dateparser import parse as dp
from datetime import datetime, timedelta, timezone
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig, PullSensorObservationsPerStationConfig, get_auth_config
from app.services.action_scheduler import trigger_action
from app.services.activity_logger import activity_logger
from app.services.gundi import send_observations_to_gundi
from app.services.state import IntegrationStateManager
from app.services.utils import generate_batches

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


MAX_DAYS_PER_QUERY = 2
STEVENS_CONNECT_BASE_URL = "https://api.stevens-connect.com"


def generate_date_pairs(lower_date, upper_date, interval=MAX_DAYS_PER_QUERY):
    while upper_date > lower_date:
        yield max(lower_date, upper_date - timedelta(days=interval)), upper_date
        upper_date -= timedelta(days=interval)


def transform(station_sensor, timestamp, readings):
    source_name = f"{station_sensor.station['station_name']} - Sensor '{station_sensor.sensor['name']}'"
    readings_additional = {}

    for reading in readings:
        channel_info = next(
            (channel for channel in station_sensor.sensor['channels'] if channel['id'] == reading.channel_id), None
        )

        reading_additional = {
            channel_info['name']: f"{reading.reading} {next((unit['unit'] for unit in station_sensor.units if str(unit['id']) == channel_info['unit_id']), '')}",
            f"{channel_info['name']} Health": f"{channel_info['channel_health'].get('health')}%"
        }

        readings_additional.update(reading_additional)

    return {
        "source_name": source_name,
        "source": station_sensor.sensor["id"],
        "type": "stationary-object",
        "subtype": "weather_station",
        "recorded_at": timestamp,
        "location": {
            "lat": station_sensor.station['station_latitude'],
            "lon": station_sensor.station['station_longitude'],
        },
        "additional": {**readings_additional}
    }


async def action_auth(integration, action_config: AuthenticateConfig):
    logger.info(f"Executing 'auth' action with integration ID {integration.id} and action_config {action_config}...")

    base_url = integration.base_url or STEVENS_CONNECT_BASE_URL

    try:
        response = await client.get_token(integration, base_url, action_config)
        if not response:
            logger.error(f"Failed to authenticate with integration {integration.id} using {action_config}")
            return {"valid_credentials": False, "message": "Bad credentials"}
        return {"valid_credentials": True}
    except client.StevensConnectBadRequestException as e:
        return {"valid_credentials": False, "status_code": e.status_code, "message": "Bad credentials"}
    except httpx.HTTPStatusError as e:
        return {"error": True, "status_code": e.response.status_code}


@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    logger.info(f"Executing 'pull_observations' action with integration ID {integration.id} and action_config {action_config}...")

    base_url = integration.base_url or STEVENS_CONNECT_BASE_URL
    auth_config = get_auth_config(integration)

    try:
        projects = await client.get_projects(integration, base_url, auth_config)
        if projects:
            logger.info(f"Found {len(projects.projects)} projects for integration {integration.id} Email: {auth_config.email}")
            now = datetime.now(timezone.utc)
            sensors_triggered = 0
            for project in projects.projects:
                stations = project.stations
                logger.info(f"Found {len(stations)} stations for integration {integration.id} Project: {project.id}")
                for station in stations:
                    station_info = {
                        "station_name": station.name,
                        "station_longitude": station.longitude,
                        "station_latitude": station.latitude
                    }
                    # Filter out the "Statistics" and "Diagnostic Parameters" sensors
                    sensors = [sensor for sensor in station.sensors if sensor.name not in ["Statistics", "Diagnostic Parameters"]]

                    logger.info(f"Found {len(sensors)} sensors for integration {integration.id} Station: {station.name}")

                    for sensor in sensors:
                        logger.info(f"Triggering 'action_pull_sensor_observations_per_station' action for sensor {sensor.name}...")
                        device_state = await state_manager.get_state(
                            integration_id=integration.id,
                            action_id="pull_observations",
                            source_id=sensor.id
                        )
                        if not device_state:
                            logger.info(f"Setting initial lookback days for sensor {sensor.id} to {action_config.default_lookback_days}")
                            start_date = (now - timedelta(days=action_config.default_lookback_days))
                        else:
                            logger.info(f"Setting begin time for device {sensor.id} to {device_state.get('updated_at')}")
                            start_date = dp(device_state.get("updated_at")).replace(tzinfo=timezone.utc)

                        stop_date = datetime.now(timezone.utc)

                        # Generate date pairs for the given start and stop dates
                        for lower, upper in generate_date_pairs(start_date, stop_date):
                            parsed_config = PullSensorObservationsPerStationConfig(
                                start=lower,
                                stop=upper,
                                project_id=project.id,
                                station=station_info,
                                sensor=sensor,
                                units=projects.units
                            )
                            await trigger_action(integration.id, "pull_sensor_observations_per_station", config=parsed_config)
                            sensors_triggered += 1

                        # Save latest device updated_at
                        latest_time = sensor.channels[0].channel_health["last_reading"].replace(" (UTC)", "")
                        state = {"updated_at": latest_time}

                        await state_manager.set_state(
                            integration_id=integration.id,
                            action_id="pull_observations",
                            state=state,
                            source_id=sensor.id
                        )

            return {"sensors_triggered": sensors_triggered}
        else:
            logger.warning(f"No projects found for integration {integration.id} Email: {auth_config.email}")
            return {"sensors_triggered": 0}
    except (client.StevensConnectUnauthorizedException, client.StevensConnectBadRequestException) as e:
        message = f"Failed to authenticate with integration {integration.id} using {auth_config}. Exception: {e}"
        logger.exception(message)
        raise e
    except httpx.HTTPStatusError as e:
        message = f"'pull_observations' action error with integration {integration.id} using {auth_config}. Exception: {e}"
        logger.exception(message)
        raise e


@activity_logger()
async def action_pull_sensor_observations_per_station(integration, action_config: PullSensorObservationsPerStationConfig):
    logger.info(f"Executing action 'pull_sensor_observations_per_station' for integration ID {integration.id} and action_config {action_config}...")

    base_url = integration.base_url or STEVENS_CONNECT_BASE_URL
    observations_extracted = 0

    try:
        sensor_observations = await client.get_sensor_readings(integration, base_url, action_config)
        if sensor_observations:
            transformed_data = []
            for timestamp, readings in sensor_observations.items():
                logger.info(
                    f"Extracted {len(readings)} readings for sensor '{action_config.sensor['name']}' from '{timestamp}'"
                )

                transformed_data.append(transform(action_config, timestamp, readings))

            for i, batch in enumerate(generate_batches(transformed_data, 200)):
                logger.info(f'Sending observations batch #{i}: {len(batch)} observations. Sensor: {action_config.sensor["name"]}')
                response = await send_observations_to_gundi(observations=batch, integration_id=integration.id)
                observations_extracted += len(response)

            return {"observations_extracted": observations_extracted}
        else:
            logger.warning(f"No observations found for sensor '{action_config.sensor['name']}'")
            return {"observations_extracted": 0}
    except (client.StevensConnectUnauthorizedException, client.StevensConnectBadRequestException) as e:
        message = f"Failed to authenticate with integration {integration.id} using {action_config}. Exception: {e}"
        logger.exception(message)
        raise e
    except httpx.HTTPStatusError as e:
        message = f"'pull_sensor_observations_per_station' action error with integration {integration.id} using {action_config}. Exception: {e}"
        logger.exception(message)
        raise e
