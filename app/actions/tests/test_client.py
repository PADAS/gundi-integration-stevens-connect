import pytest
import httpx

from unittest.mock import patch, AsyncMock, MagicMock
from app.actions import client

@pytest.mark.asyncio
async def test_get_token_good():
    integration = MagicMock()
    integration.id = "int1"
    auth = MagicMock()
    auth.email = "test@example.com"
    auth.password.get_secret_value.return_value = "pw"
    base_url = "https://api.stevens-connect.com"

    response_data = {"data": {"token": "abc123"}}
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value.is_error = False
        mock_post.return_value.json = MagicMock(return_value=response_data)
        mock_post.return_value.raise_for_status.return_value = None
        token = await client.get_token(integration, base_url, auth)
        assert token == "abc123"

@pytest.mark.asyncio
async def test_get_token_bad_400():
    integration = MagicMock()
    auth = MagicMock()
    base_url = "url"
    error = httpx.HTTPStatusError("msg", request=MagicMock(), response=MagicMock(status_code=400))
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.side_effect = error
        with pytest.raises(client.StevensConnectBadRequestException):
            await client.get_token(integration, base_url, auth)

@pytest.mark.asyncio
async def test_get_token_bad_404():
    integration = MagicMock()
    auth = MagicMock()
    base_url = "url"
    error = httpx.HTTPStatusError("msg", request=MagicMock(), response=MagicMock(status_code=404))
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.side_effect = error
        with pytest.raises(client.StevensConnectNotFoundException):
            await client.get_token(integration, base_url, auth)

@pytest.mark.asyncio
async def test_get_projects_good():
    integration = MagicMock()
    integration.id = "int1"
    base_url = "url"
    auth = MagicMock()
    token = "abc"
    response_data = {"data": {"config_packet": {"projects": [], "units": []}}}
    with patch("app.actions.client.get_token", new_callable=AsyncMock, return_value=token), \
         patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get, \
         patch("app.actions.client.ProjectResponse.parse_obj", return_value="parsed") as mock_parse:
        mock_get.return_value.is_error = False
        mock_get.return_value.json = MagicMock(return_value=response_data)
        mock_get.return_value.raise_for_status.return_value = None
        result = await client.get_projects(integration, base_url, auth)
        assert result == "parsed"

@pytest.mark.asyncio
async def test_get_projects_bad_401():
    async def test_get_projects_bad_401():
        integration = MagicMock()
        base_url = "url"
        auth = MagicMock()
        token = "abc"
        error = httpx.HTTPStatusError(
            "msg", request=MagicMock(), response=MagicMock(status_code=401)
        )
        with patch("app.actions.client.get_token", new_callable=AsyncMock, return_value=token), \
                patch("httpx.AsyncClient.get", new_callable=AsyncMock, side_effect=error):
            with pytest.raises(client.StevensConnectUnauthorizedException):
                await client.get_projects(integration, base_url, auth)

@pytest.mark.asyncio
async def test_get_projects_bad_404():
    integration = MagicMock()
    base_url = "url"
    auth = MagicMock()
    token = "abc"
    error = httpx.HTTPStatusError("msg", request=MagicMock(), response=MagicMock(status_code=404))
    with patch("app.actions.client.get_token", new_callable=AsyncMock, return_value=token), \
         patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.side_effect = error
        with pytest.raises(client.StevensConnectNotFoundException):
            await client.get_projects(integration, base_url, auth)

@pytest.mark.asyncio
async def test_get_sensor_readings_good():
    integration = MagicMock()
    base_url = "url"
    config = MagicMock()
    config.project_id = "pid"
    config.sensor = {"channels": [{"id": "cid"}], "name": "sname"}
    config.start.strftime.return_value = "2024-01-01 00:00:00"
    config.stop.strftime.return_value = "2024-01-02 00:00:00"
    token = "abc"
    response_data = {
        "data": {
            "readings": {"2024-01-01T00:00:00Z": []},
            "paging": {"last_page": 1}
        }
    }
    with patch("app.actions.client.get_auth_config", return_value=MagicMock()), \
         patch("app.actions.client.get_token", new_callable=AsyncMock, return_value=token), \
         patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get, \
         patch("app.actions.client.ChannelReadingsResponse.parse_obj") as mock_parse:
        mock_get.return_value.is_error = False
        mock_get.return_value.json = MagicMock(return_value=response_data)
        mock_get.return_value.raise_for_status.return_value = None
        mock_parse.return_value.readings = {"2024-01-01T00:00:00Z": []}
        mock_parse.return_value.paging = {"last_page": 1}
        result = await client.get_sensor_readings(integration, base_url, config)
        assert isinstance(result, dict)

@pytest.mark.asyncio
async def test_get_sensor_readings_bad_401():
    async def test_get_sensor_readings_bad_401():
        integration = MagicMock()
        base_url = "url"
        config = MagicMock()
        config.project_id = "pid"
        config.sensor = {"channels": [{"id": "cid"}], "name": "sname"}
        config.start.strftime.return_value = "2024-01-01 00:00:00"
        config.stop.strftime.return_value = "2024-01-02 00:00:00"
        token = "abc"
        error = httpx.HTTPStatusError("msg", request=MagicMock(), response=MagicMock(status_code=401))
        with patch("app.actions.client.get_auth_config", return_value=MagicMock()), \
                patch("app.actions.client.get_token", new_callable=AsyncMock, return_value=token), \
                patch("httpx.AsyncClient.get", new_callable=AsyncMock, side_effect=error):
            with pytest.raises(client.StevensConnectUnauthorizedException):
                await client.get_sensor_readings(integration, base_url, config)

@pytest.mark.asyncio
async def test_get_sensor_readings_bad_404():
    integration = MagicMock()
    base_url = "url"
    config = MagicMock()
    config.project_id = "pid"
    config.sensor = {"channels": [{"id": "cid"}], "name": "sname"}
    config.start.strftime.return_value = "2024-01-01 00:00:00"
    config.stop.strftime.return_value = "2024-01-02 00:00:00"
    token = "abc"
    error = httpx.HTTPStatusError(
        "msg", request=MagicMock(), response=MagicMock(status_code=404)
    )
    with patch("app.actions.client.get_auth_config", return_value=MagicMock()), \
            patch("app.actions.client.get_token", new_callable=AsyncMock, return_value=token), \
            patch("httpx.AsyncClient.get", new_callable=AsyncMock, side_effect=error):
        with pytest.raises(client.StevensConnectNotFoundException):
            await client.get_sensor_readings(integration, base_url, config)
