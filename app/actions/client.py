import logging
import httpx
import pydantic
import stamina

from datetime import datetime, timezone
from typing import List

from app.actions.configurations import get_auth_config
from app.services.state import IntegrationStateManager
from collections import defaultdict


logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


class Channel(pydantic.BaseModel):
    id: str
    name: str
    unit_id: str
    channel_health: dict


class Sensor(pydantic.BaseModel):
    id: str
    name: str
    channels: List[Channel]


class Station(pydantic.BaseModel):
    name: str
    latitude: float
    longitude: float
    sensors: List[Sensor]


class Project(pydantic.BaseModel):
    id: str
    name: str
    stations: List[Station]


class ChannelReading(pydantic.BaseModel):
    channel_id: str
    reading: float
    timestamp: datetime

    @pydantic.validator('timestamp', always=True)
    def parse_time_string(cls, v):
        if not v.tzinfo:
            return v.replace(tzinfo=timezone.utc)
        return v


class Unit(pydantic.BaseModel):
    id: int
    name: str
    unit: str


class ChannelReadingsResponse(pydantic.BaseModel):
    readings: dict[str, List[ChannelReading]]
    paging: dict


class ProjectResponse(pydantic.BaseModel):
    projects: List[Project]
    units: List[Unit]


class StevensConnectNotFoundException(Exception):
    def __init__(self, error: Exception, message: str, status_code=404):
        self.status_code = status_code
        self.message = message
        self.error = error
        super().__init__(f"'{self.status_code}: {self.message}, Error: {self.error}'")


class StevensConnectBadRequestException(Exception):
    def __init__(self, error: Exception, message: str, status_code=400):
        self.status_code = status_code
        self.message = message
        self.error = error
        super().__init__(f"'{self.status_code}: {self.message}, Error: {self.error}'")


class StevensConnectUnauthorizedException(Exception):
    def __init__(self, error: Exception, message: str, status_code=401):
        self.status_code = status_code
        self.message = message
        self.error = error
        super().__init__(f"'{self.status_code}: {self.message}, Error: {self.error}'")


@stamina.retry(on=httpx.HTTPError, wait_initial=4.0, wait_jitter=5.0, wait_max=32.0)
async def get_token(integration, base_url, auth):
    async with httpx.AsyncClient(timeout=120) as session:
        logger.info(f"-- Getting token for integration ID: {integration.id} Email: {auth.email} --")

        url = f"{base_url}/authenticate"

        data = {
            "email": auth.email,
            "password": auth.password.get_secret_value()
        }

        try:
            response = await session.post(url, json=data)
            if response.is_error:
                logger.error(f"Error 'get_token'. Response body: {response.text}")
            response.raise_for_status()
            parsed_response = response.json()
            if parsed_response:
                token = parsed_response["data"].get("token")
                return token
            else:
                return response.text
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 400:
                raise StevensConnectBadRequestException(e, "Bad Request")
            elif e.response.status_code == 404:
                raise StevensConnectNotFoundException(e, "User not found")
            raise e


@stamina.retry(on=StevensConnectUnauthorizedException, wait_initial=4.0, wait_jitter=5.0, wait_max=32.0)
async def get_projects(integration, base_url, auth):
    async with httpx.AsyncClient(timeout=120) as session:
        url = f"{base_url}/config-packet"

        token = await get_token(integration, base_url, auth)

        logger.info(f"-- Getting projects for integration ID: {integration.id} Email: {auth.email} --")

        try:
            response = await session.get(url, headers={"Authorization": f"Bearer {token}"})
            if response.is_error:
                logger.error(f"Error 'get_projects'. Response body: {response.text}")
            response.raise_for_status()
            parsed_response = response.json()
            if parsed_response:
                projects = ProjectResponse.parse_obj(parsed_response["data"].get("config_packet"))
                return projects
            else:
                return response.text
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise StevensConnectUnauthorizedException(e, "Unauthorized access")
            elif e.response.status_code == 404:
                raise StevensConnectNotFoundException(e, "User not found")
            raise e


@stamina.retry(on=StevensConnectUnauthorizedException, wait_initial=4.0, wait_jitter=5.0, wait_max=32.0)
async def get_sensor_readings(integration, base_url, config):
    async with httpx.AsyncClient(timeout=120) as session:
        auth = get_auth_config(integration)
        url = f"{base_url}/project/{config.project_id}/readings/v3/channels"

        token = await get_token(integration, base_url, auth)

        current_page = 1
        has_data = True

        total_readings = []

        params = {
            "channel_ids": ",".join([channel['id'] for channel in config.sensor['channels']]),
            "range_type": "absolute",
            "start_date": config.start.strftime("%Y-%m-%d %H:%M:%S"),
            "end_date": config.stop.strftime("%Y-%m-%d %H:%M:%S")
        }

        while has_data:
            params["page"] = current_page

            logger.info(f"-- Getting sensor readings for integration ID: {integration.id} sensor: {config.sensor['name']} --")

            try:
                response = await session.get(url, params=params, headers={"Authorization": f"Bearer {token}"})
                if response.is_error:
                    logger.error(f"Error 'get_projects'. Response body: {response.text}")
                response.raise_for_status()
                parsed_response = response.json()
                if parsed_response:
                    readings = ChannelReadingsResponse.parse_obj(parsed_response.get("data"))
                    total_readings.extend(readings.readings.values())

                    # Check paging response and validate if there are more pages
                    if readings.paging["last_page"] > current_page:
                        current_page += 1
                    else:
                        has_data = False
                else:
                    return response.text
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 401:
                    raise StevensConnectUnauthorizedException(e, "Unauthorized access")
                elif e.response.status_code == 404:
                    raise StevensConnectNotFoundException(e, "User not found")
                raise e

        # Gather all the readings with the same timestamp
        grouped_readings = defaultdict(list)

        for channel_readings in total_readings:
            for reading in channel_readings:
                grouped_readings[reading.timestamp].append(reading)

        return grouped_readings
