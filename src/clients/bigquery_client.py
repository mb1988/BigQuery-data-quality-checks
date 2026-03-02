"""
Secure BigQuery client with parameterized queries and cost controls.
"""
import os
import logging
from typing import List, Dict, Any, Optional
from datetime import date, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core import exceptions

from src.utils import retry
from src.config import Config

logger = logging.getLogger(__name__)


class BigQueryClientError(Exception):
    """Base exception for BigQuery client errors."""
    pass


class QueryCostError(BigQueryClientError):
    """Raised when query exceeds cost threshold."""
    pass


class BigQueryClient:
    """
    Secure BigQuery client with production features:
    - Parameterized queries (no SQL injection)
    - Cost estimation and controls
    - Retry logic
    - Timeout protection
    - Structured error handling
    """

    def __init__(self, config: Config):
        """
        Initialize BigQuery client.

        Args:
            config: Application configuration
        """
        self.config = config
        self.client = None
        self.project_id = config.bigquery_project
        self.dataset = config.bigquery_dataset
        self.table = config.bigquery_table

        # Cost control (default: 1GB max query)
        self.max_query_bytes = int(os.getenv('MAX_QUERY_BYTES', '1073741824'))

        self._connect()

    def _connect(self):
        """Establish secure connection to BigQuery."""
        try:
            credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

            if credentials_path and os.path.exists(credentials_path):
                # Use service account credentials with full BigQuery scope
                # Note: We need bigquery scope (not just readonly) to run query jobs
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path,
                    scopes=["https://www.googleapis.com/auth/bigquery"],
                )
                self.client = bigquery.Client(
                    project=self.project_id,
                    credentials=credentials
                )
                logger.info(f"Connected to BigQuery using service account")
            else:
                # Use application default credentials
                self.client = bigquery.Client(project=self.project_id)
                logger.info(f"Connected to BigQuery using default credentials")

            logger.info(f"BigQuery project: {self.project_id}")

        except Exception as e:
            logger.error(f"Failed to connect to BigQuery: {e}")
            raise BigQueryClientError(f"BigQuery connection failed: {e}")

    def _estimate_query_cost(self, query: str) -> int:
        """
        Estimate query cost using dry run.

        Args:
            query: SQL query

        Returns:
            Estimated bytes processed

        Raises:
            QueryCostError: If query exceeds cost threshold
        """
        try:
            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            query_job = self.client.query(query, job_config=job_config)

            bytes_processed = query_job.total_bytes_processed
            bytes_billed = query_job.total_bytes_billed or bytes_processed

            logger.info(f"Query will process {bytes_processed:,} bytes")

            if bytes_billed > self.max_query_bytes:
                raise QueryCostError(
                    f"Query would process {bytes_billed:,} bytes, "
                    f"exceeding limit of {self.max_query_bytes:,} bytes"
                )

            return bytes_processed

        except QueryCostError:
            raise
        except Exception as e:
            logger.warning(f"Could not estimate query cost: {e}")
            return 0

    @retry(
        max_attempts=3,
        delay=2.0,
        backoff=2.0,
        exceptions=(exceptions.GoogleAPIError, exceptions.RetryError)
    )
    def query(
        self,
        query: str,
        parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        timeout: int = 300,
    ) -> List[Dict[str, Any]]:
        """
        Execute parameterized BigQuery query with safety checks.

        Args:
            query: SQL query with parameters
            parameters: List of query parameters
            timeout: Query timeout in seconds

        Returns:
            List of result rows as dictionaries

        Raises:
            BigQueryClientError: On query failure
            QueryCostError: If query exceeds cost threshold
        """
        try:
            # Cost estimation
            self._estimate_query_cost(query)

            # Configure query job
            job_config = bigquery.QueryJobConfig(
                query_parameters=parameters or [],
                use_query_cache=True,
                use_legacy_sql=False,
            )

            # Execute query
            logger.info("Executing BigQuery query...")
            query_job = self.client.query(
                query,
                job_config=job_config,
                timeout=timeout
            )

            # Get results
            results = list(query_job.result(timeout=timeout))

            logger.info(f"Query returned {len(results)} rows")

            # Convert to dictionaries
            return [dict(row) for row in results]

        except QueryCostError:
            raise
        except (TimeoutError, Exception) as e:
            # Catch timeout errors (can be TimeoutError or other timeout-related exceptions)
            if 'timeout' in str(e).lower() or 'deadline' in str(e).lower():
                logger.error(f"Query timeout after {timeout}s: {e}")
                raise BigQueryClientError(f"Query timeout: {e}")
            # Re-raise if not a timeout error
            if isinstance(e, exceptions.GoogleAPIError):
                logger.error(f"BigQuery API error: {e}")
                raise BigQueryClientError(f"BigQuery API error: {e}")
            raise
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise BigQueryClientError(f"Query failed: {e}")

    def get_historical_data(
        self,
        check_date: date,
        weeks_back: int = 8,
        platforms: Optional[List[str]] = None,
        temporal_labels: Optional[List[str]] = None,
    ) -> Dict[tuple, Dict[str, Any]]:
        """
        Get historical data for anomaly detection with same day of week.
        Now supports multiple temporal labels (daily, weekly, monthly).

        Uses parameterized query to prevent SQL injection.

        Args:
            check_date: Date to check
            weeks_back: Number of weeks to look back
            platforms: List of platforms (default: from config)
            temporal_labels: List of temporal labels (default: from config)

        Returns:
            Dictionary of historical data keyed by (country, platform, temporal_label)
        """
        if platforms is None:
            platforms = self.config.platforms

        if temporal_labels is None:
            temporal_labels = self.config.temporal_labels

        # Calculate date range based on temporal labels
        # For daily: use same day of week (current approach)
        # For weekly: use week-start dates (Mondays)
        # For monthly: use month-start dates (first day of month)

        # Daily dates (same day of week)
        daily_dates = [check_date]
        for i in range(1, weeks_back + 1):
            historical_date = check_date - timedelta(weeks=i)
            daily_dates.append(historical_date)

        # Weekly dates (for weekly temporal label)
        # Get Monday (start of week) for the most recent complete week
        weekly_dates = []
        if 'weekly' in temporal_labels:
            # Find Monday of current week (0 = Monday, 6 = Sunday)
            days_since_monday = check_date.weekday()
            current_week_monday = check_date - timedelta(days=days_since_monday)
            # Use the most recent complete week (current week start)
            base_week_start = current_week_monday
            weekly_dates.append(base_week_start)

            # Go back additional weeks
            for i in range(1, weeks_back + 1):
                weekly_dates.append(base_week_start - timedelta(weeks=i))

        # Monthly dates (for monthly temporal label)
        # Use the most recent complete month (previous month start)
        monthly_dates = []
        if 'monthly' in temporal_labels:
            from datetime import datetime
            current_month = datetime(check_date.year, check_date.month, 1).date()
            prev_month_end = current_month - timedelta(days=1)
            prev_month_start = datetime(prev_month_end.year, prev_month_end.month, 1).date()
            monthly_dates.append(prev_month_start)

            # Go back additional months
            for i in range(1, 9):
                year = prev_month_start.year
                month = prev_month_start.month - i
                while month <= 0:
                    month += 12
                    year -= 1
                monthly_dates.append(date(year, month, 1))

        # Combine all dates
        all_dates = list(set(daily_dates + weekly_dates + monthly_dates))

        logger.info(
            f"Querying {weeks_back} weeks of data for {check_date.strftime('%A')}s"
        )
        logger.info(f"Temporal labels: {', '.join(temporal_labels)}")
        if 'weekly' in temporal_labels:
            logger.info(
                f"Including {len(weekly_dates)} weekly reference dates (Mondays, most recent complete week)"
            )
        if 'monthly' in temporal_labels:
            logger.info(
                f"Including {len(monthly_dates)} monthly reference dates (previous month starts)"
            )

        # Parameterized query (NO SQL INJECTION RISK)
        query = f"""
        SELECT 
            calendarDate,
            temporalLabel,
            country,
            platform,
            componentCount,
            screenViews
        FROM `{self.project_id}.{self.dataset}.{self.table}`
        WHERE calendarDate IN UNNEST(@dates)
          AND temporalLabel IN UNNEST(@temporal_labels)
          AND platform IN UNNEST(@platforms)
        ORDER BY country, platform, temporalLabel, calendarDate DESC
        """

        # Query parameters (secure)
        parameters = [
            bigquery.ArrayQueryParameter('dates', 'DATE', all_dates),
            bigquery.ArrayQueryParameter('temporal_labels', 'STRING', temporal_labels),
            bigquery.ArrayQueryParameter('platforms', 'STRING', platforms),
        ]

        # Execute query
        results = self.query(query, parameters=parameters)

        # Organize data by country/platform/temporal_label
        data = {}
        for row in results:
            country = row['country']
            platform = row['platform']
            temporal_label = row['temporalLabel']
            date_str = str(row['calendarDate'])

            key = (country, platform, temporal_label)
            if key not in data:
                data[key] = {}

            data[key][date_str] = {
                'componentCount': row['componentCount'],
                'screenViews': row['screenViews'],
            }

        logger.info(f"Retrieved data for {len(data)} country/platform/temporal combinations")

        return data

    def close(self):
        """Close BigQuery client connection."""
        if self.client:
            self.client.close()
            logger.info("BigQuery client closed")
