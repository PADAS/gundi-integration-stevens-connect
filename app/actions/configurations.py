import pydantic

from typing import Any, List, Optional
from datetime import datetime, timezone

from app.actions.core import AuthActionConfiguration, PullActionConfiguration, ExecutableActionMixin, InternalActionConfiguration
from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action, UIOptions, FieldWithUIOptions, GlobalUISchemaOptions


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    email: str
    password: pydantic.SecretStr = pydantic.Field(..., format="password")

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "email",
            "password",
        ],
    )


class PullObservationsConfig(PullActionConfiguration):
    default_lookback_days: int = FieldWithUIOptions(
        7,
        title="Default Lookback Days",
        description="Initial number of days to look back for observations Min: 1, Default: 7",
        ge=1,
        le=15,
        ui_options=UIOptions(
            widget="range",  # This will be rendered ad a range slider
        )
    )
    sensor_featured_properties: Optional[List[str]] = pydantic.Field(
        title='Featured Readings',
        description='A comma-separated list of sensor data to display as "featured_property" in ER. (format: SENSOR_NAME: DATA_TYPE_1, DATA_TYPE_2, ...)',
    )


class PullSensorObservationsPerStationConfig(InternalActionConfiguration):
    start: datetime
    stop: datetime
    project_id: int
    station: dict
    sensor_featured_properties: list
    sensor: Any
    units: Any

    @pydantic.validator('start', always=True)
    def parse_time_string(cls, v):
        if not v.tzinfo:
            return v.replace(tzinfo=timezone.utc)
        return v


def get_auth_config(integration):
    # Look for the login credentials, needed for any action
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)


def get_pull_config(integration):
    # Look for the login credentials, needed for any action
    pull_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="pull_observations"
    )
    if not pull_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return PullObservationsConfig.parse_obj(pull_config.data)
