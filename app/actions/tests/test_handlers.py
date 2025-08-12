import pytest

from unittest.mock import AsyncMock, patch, MagicMock
from app.actions import handlers

@pytest.mark.asyncio
async def test_action_pull_observations_good(mocker, mock_publish_event):
    integration = MagicMock()
    integration.id = "int1"
    integration.base_url = None

    action_config = MagicMock()
    action_config.sensor_featured_properties = ["Sensor1: Temp, Humidity"]
    action_config.default_lookback_days = 2

    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)

    # Mock dependencies
    with patch("app.actions.handlers.get_auth_config", return_value=MagicMock()), \
         patch("app.actions.handlers.client.get_projects", new_callable=AsyncMock) as mock_get_projects, \
         patch("app.actions.handlers.trigger_action", new_callable=AsyncMock), \
         patch("app.actions.handlers.state_manager.get_state", new_callable=AsyncMock, return_value=None), \
         patch("app.actions.handlers.state_manager.set_state", new_callable=AsyncMock):

        # Setup mock projects, stations, sensors, units
        sensor = MagicMock()
        sensor.name = "Sensor1"
        sensor.id = "sensor1"
        sensor.channels = [MagicMock(channel_health={"last_reading": "2024-01-01 00:00:00 (UTC)"})]

        station = MagicMock()
        station.name = "Station1"
        station.longitude = 1.0
        station.latitude = 2.0
        station.sensors = [sensor]

        project = MagicMock()
        project.id = 123
        project.stations = [station]

        projects = MagicMock()
        projects.projects = [project]
        projects.units = []

        mock_get_projects.return_value = projects

        result = await handlers.action_pull_observations(integration, action_config)
        assert result["sensors_triggered"] == 2

@pytest.mark.asyncio
async def test_action_pull_observations_bad_no_projects(mocker, mock_publish_event):
    integration = MagicMock()
    integration.id = "int1"
    integration.base_url = None

    action_config = MagicMock()
    action_config.sensor_featured_properties = None
    action_config.default_lookback_days = 2

    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)

    with patch("app.actions.handlers.get_auth_config", return_value=MagicMock()), \
         patch("app.actions.handlers.client.get_projects", new_callable=AsyncMock, return_value=None):

        result = await handlers.action_pull_observations(integration, action_config)
        assert result["sensors_triggered"] == 0

@pytest.mark.asyncio
async def test_action_pull_observations_bad_auth_exception(mocker, mock_publish_event):
    integration = MagicMock()
    integration.id = "int1"
    integration.base_url = None

    action_config = MagicMock()
    action_config.sensor_featured_properties = None
    action_config.default_lookback_days = 2

    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)

    unauthorized_exc = handlers.client.StevensConnectUnauthorizedException("error", "message")

    with patch("app.actions.handlers.get_auth_config", return_value=MagicMock()), \
         patch("app.actions.handlers.client.get_projects", new_callable=AsyncMock, side_effect=unauthorized_exc):

        with pytest.raises(handlers.client.StevensConnectUnauthorizedException):
            await handlers.action_pull_observations(integration, action_config)

@pytest.mark.asyncio
async def test_action_pull_sensor_observations_per_station_good(mocker, mock_publish_event):
    integration = MagicMock()
    integration.id = "int1"
    integration.base_url = None

    action_config = MagicMock()
    action_config.sensor_featured_properties = []
    action_config.sensor = {"name": "Sensor1"}
    action_config.units = []
    # Simulate sensor_observations
    sensor_observations = {
        "2024-01-01T00:00:00Z": [MagicMock(channel_id=1, reading=10)]
    }

    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)

    with patch("app.actions.handlers.client.get_sensor_readings", new_callable=AsyncMock, return_value=sensor_observations), \
         patch("app.actions.handlers.transform", return_value={}), \
         patch("app.actions.handlers.generate_batches", return_value=[[{}]]), \
         patch("app.actions.handlers.send_observations_to_gundi", new_callable=AsyncMock, return_value=[1]):

        result = await handlers.action_pull_sensor_observations_per_station(integration, action_config)
        assert result["observations_extracted"] == 1

@pytest.mark.asyncio
async def test_action_pull_sensor_observations_per_station_bad_no_observations(mocker, mock_publish_event):
    integration = MagicMock()
    integration.id = "int1"
    integration.base_url = None

    action_config = MagicMock()
    action_config.sensor_featured_properties = []
    action_config.sensor = {"name": "Sensor1"}
    action_config.units = []

    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)

    with patch("app.actions.handlers.client.get_sensor_readings", new_callable=AsyncMock, return_value=None):

        result = await handlers.action_pull_sensor_observations_per_station(integration, action_config)
        assert result["observations_extracted"] == 0

@pytest.mark.asyncio
async def test_action_pull_sensor_observations_per_station_bad_auth_exception(mocker, mock_publish_event):
    integration = MagicMock()
    integration.id = "int1"
    integration.base_url = None

    action_config = MagicMock()
    action_config.sensor_featured_properties = []
    action_config.sensor = {"name": "Sensor1"}
    action_config.units = []

    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)

    unauthorized_exc = handlers.client.StevensConnectUnauthorizedException("error", "message")

    with patch("app.actions.handlers.client.get_sensor_readings", new_callable=AsyncMock, side_effect=unauthorized_exc):

        with pytest.raises(handlers.client.StevensConnectUnauthorizedException):
            await handlers.action_pull_sensor_observations_per_station(integration, action_config)
