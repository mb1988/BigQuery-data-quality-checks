"""
Configuration loader with environment variable support.
Handles sensitive credentials securely.
"""
import os
import yaml
import logging
from typing import Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    """Raised when configuration is invalid or missing."""
    pass


class Config:
    """
    Application configuration manager.

    Loads configuration from YAML and environment variables.
    Prioritizes environment variables over YAML for security.
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration.

        Args:
            config_path: Path to config.yaml file
        """
        self.config_path = config_path or self._find_config()
        self._config = self._load_config()
        self._validate_config()

    def _find_config(self) -> str:
        """Find config.yaml in project directory."""
        possible_paths = [
            Path.cwd() / "config" / "config.yaml",
            Path.cwd() / "config.yaml",
            Path(__file__).parent.parent / "config" / "config.yaml",
        ]

        for path in possible_paths:
            if path.exists():
                return str(path)

        raise ConfigurationError(
            "config.yaml not found. Please create config/config.yaml"
        )

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)

            # Replace ${ENV_VAR} placeholders with actual values
            config = self._substitute_env_vars(config)

            logger.info(f"Configuration loaded from {self.config_path}")
            return config

        except FileNotFoundError:
            raise ConfigurationError(f"Config file not found: {self.config_path}")
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in config file: {e}")

    def _substitute_env_vars(self, obj: Any) -> Any:
        """
        Recursively substitute ${ENV_VAR} with environment variables.

        Args:
            obj: Configuration object (dict, list, or string)

        Returns:
            Object with environment variables substituted
        """
        if isinstance(obj, dict):
            return {k: self._substitute_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._substitute_env_vars(item) for item in obj]
        elif isinstance(obj, str) and obj.startswith('${') and obj.endswith('}'):
            env_var = obj[2:-1]
            value = os.getenv(env_var)
            if value is None:
                raise ConfigurationError(
                    f"Environment variable {env_var} is required but not set"
                )
            return value
        return obj

    def _validate_config(self):
        """Validate required configuration fields."""
        required_fields = [
            ('datasource', 'bigquery_project'),
            ('datasource', 'bigquery_dataset'),
            ('datasource', 'bigquery_table'),
        ]

        for *path, field in required_fields:
            obj = self._config
            for key in path:
                obj = obj.get(key, {})

            if not obj.get(field):
                raise ConfigurationError(
                    f"Required configuration missing: {'.'.join(path + [field])}"
                )

        logger.info("Configuration validated successfully")

    @property
    def bigquery_project(self) -> str:
        """Get BigQuery project ID."""
        return self._config['datasource']['bigquery_project']

    @property
    def bigquery_dataset(self) -> str:
        """Get BigQuery dataset."""
        return self._config['datasource']['bigquery_dataset']

    @property
    def bigquery_table(self) -> str:
        """Get BigQuery table."""
        return self._config['datasource']['bigquery_table']

    @property
    def tier_1_countries(self) -> list:
        """Get tier 1 (core business) countries."""
        return self._config['dimensions']['country']['priority_tiers'][
            'tier_1_core_business'
        ]['countries']

    @property
    def tier_2_countries(self) -> list:
        """Get tier 2 (strategic watch) countries."""
        return self._config['dimensions']['country']['priority_tiers'][
            'tier_2_strategic_watch'
        ]['countries']

    @property
    def tier_1_thresholds(self) -> Dict[str, float]:
        """Get tier 1 alert thresholds."""
        tier = self._config['dimensions']['country']['priority_tiers'][
            'tier_1_core_business'
        ]
        return {
            'drop': tier['alert_threshold_drop'],
            'spike': tier['alert_threshold_spike'],
        }

    @property
    def tier_2_thresholds(self) -> Dict[str, float]:
        """Get tier 2 alert thresholds."""
        tier = self._config['dimensions']['country']['priority_tiers'][
            'tier_2_strategic_watch'
        ]
        return {
            'drop': tier['alert_threshold_drop'],
            'spike': tier['alert_threshold_spike'],
        }

    @property
    def tier_3_thresholds(self) -> Dict[str, float]:
        """Get tier 3 alert thresholds."""
        tier = self._config['dimensions']['country']['priority_tiers'][
            'tier_3_all_others'
        ]
        return {
            'drop': tier['alert_threshold_drop'],
            'spike': tier['alert_threshold_spike'],
        }

    @property
    def platforms(self) -> list:
        """Get list of platforms to monitor."""
        return self._config['dimensions']['platform']['values']

    @property
    def temporal_labels(self) -> list:
        """Get list of temporal labels to check."""
        return self._config['dimensions']['temporalLabel']['values']

    @property
    def email_config(self) -> Dict[str, Any]:
        """Get email configuration from environment variables."""
        return {
            'smtp_host': os.getenv('EMAIL_SMTP_HOST'),
            'smtp_port': int(os.getenv('EMAIL_SMTP_PORT', '587')),
            'username': os.getenv('EMAIL_USERNAME'),
            'password': os.getenv('EMAIL_PASSWORD'),
            'from_addr': os.getenv('EMAIL_FROM'),
            'to_addrs': os.getenv('EMAIL_TO', '').split(','),
            'enabled': os.getenv('EMAIL_ENABLED', 'false').lower() == 'true',
        }

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by dot-notation key.

        Args:
            key: Configuration key (e.g., 'datasource.bigquery_project')
            default: Default value if key not found

        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self._config

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default

        return value if value is not None else default

