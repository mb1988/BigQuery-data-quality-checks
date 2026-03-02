"""
Anomaly detection with statistical analysis and seasonality awareness.
"""
import statistics
import logging
from typing import Dict, List, Any, Tuple, Optional
from datetime import date

from src.config import Config

logger = logging.getLogger(__name__)


class AnomalyDetector:
    """
    Statistical anomaly detector for time series data.

    Uses Z-score analysis with seasonality awareness (same day of week comparison).
    Tier-based thresholds for different market priorities.
    """

    def __init__(self, config: Config):
        """
        Initialize anomaly detector.

        Args:
            config: Application configuration
        """
        self.config = config
        self.tier_1_countries = config.tier_1_countries
        self.tier_2_countries = config.tier_2_countries

        # Load thresholds
        self.tier_1_thresholds = config.tier_1_thresholds
        self.tier_2_thresholds = config.tier_2_thresholds
        self.tier_3_thresholds = config.tier_3_thresholds

    def get_country_tier(self, country: str) -> int:
        """
        Determine which tier a country belongs to.

        Args:
            country: Country code

        Returns:
            Tier number (1, 2, or 3)
        """
        if country in self.tier_1_countries:
            return 1
        elif country in self.tier_2_countries:
            return 2
        else:
            return 3

    def detect_anomalies(
        self,
        historical_data: Dict[Tuple[str, str, str], Dict[str, Any]],
        check_date: date,
    ) -> List[Dict[str, Any]]:
        """
        Detect anomalies in historical data using statistical analysis.

        Args:
            historical_data: Dictionary keyed by (country, platform, temporal_label) with date->metrics
            check_date: Date being checked

        Returns:
            List of detected anomalies
        """
        anomalies = []
        check_date_str = str(check_date)

        logger.info(f"Analyzing {len(historical_data)} country/platform/temporal combinations...")

        for (country, platform, temporal_label), dates_data in historical_data.items():
            anomaly = self._analyze_combination(
                country=country,
                platform=platform,
                temporal_label=temporal_label,
                dates_data=dates_data,
                check_date_str=check_date_str,
            )

            if anomaly:
                anomalies.append(anomaly)

        logger.info(f"Detected {len(anomalies)} anomalies")

        return anomalies

    def _analyze_combination(
        self,
        country: str,
        platform: str,
        temporal_label: str,
        dates_data: Dict[str, Dict[str, Any]],
        check_date_str: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Analyze a single country/platform/temporal_label combination for anomalies.

        Args:
            country: Country code
            platform: Platform name
            temporal_label: Temporal label (daily, weekly, monthly)
            dates_data: Historical data for this combination
            check_date_str: Date being checked (as string)

        Returns:
            Anomaly dict if detected, None otherwise
        """
        # For weekly data, use the Monday (start of week) as the key
        # For monthly data, use the first day of the month as the key
        # For daily data, use the check_date as-is
        from datetime import datetime, timedelta

        if temporal_label == 'weekly':
            # Use the most recent complete week (week starts Monday).
            check_dt = datetime.strptime(check_date_str, '%Y-%m-%d')
            days_since_monday = check_dt.weekday()  # 0 = Monday, 6 = Sunday
            week_start = check_dt.date() - timedelta(days=days_since_monday)
            lookup_date_str = str(week_start)
        elif temporal_label == 'monthly':
            # Use the most recent complete month (first day of previous month).
            check_dt = datetime.strptime(check_date_str, '%Y-%m-%d')
            month_start = datetime(check_dt.year, check_dt.month, 1).date()
            prev_month_end = month_start - timedelta(days=1)
            prev_month_start = datetime(prev_month_end.year, prev_month_end.month, 1).date()
            lookup_date_str = str(prev_month_start)
        else:
            # Daily - use as-is
            lookup_date_str = check_date_str

        # Check if we have data for the check date
        if lookup_date_str not in dates_data:
            return None

        current_value = dates_data[lookup_date_str]['componentCount']

        # Get historical values (excluding current)
        historical_values = [
            v['componentCount']
            for d, v in dates_data.items()
            if d != lookup_date_str and v['componentCount'] > 0
        ]

        if len(historical_values) < 3:
            # Not enough history for statistical analysis
            return None

        # Statistical analysis
        mean = statistics.mean(historical_values)
        stdev = statistics.stdev(historical_values) if len(historical_values) > 1 else 0
        median = statistics.median(historical_values)

        # Calculate z-score (standard deviations from mean)
        if stdev > 0:
            z_score = (current_value - mean) / stdev
        else:
            z_score = 0

        # Calculate percentage difference from median
        pct_diff_median = ((current_value - median) / median * 100) if median > 0 else 0

        # Get tier and thresholds
        tier = self.get_country_tier(country)

        # Get thresholds from config
        if tier == 1:
            z_threshold = self.config.get(
                'dimensions.country.priority_tiers.tier_1_core_business.z_score_threshold',
                3.0
            )
            drop_threshold = self.config.get(
                'dimensions.country.priority_tiers.tier_1_core_business.alert_threshold_drop',
                10
            )
            spike_threshold = self.config.get(
                'dimensions.country.priority_tiers.tier_1_core_business.alert_threshold_spike',
                50
            )
        elif tier == 2:
            z_threshold = self.config.get(
                'dimensions.country.priority_tiers.tier_2_strategic_watch.z_score_threshold',
                4.0
            )
            drop_threshold = self.config.get(
                'dimensions.country.priority_tiers.tier_2_strategic_watch.alert_threshold_drop',
                20
            )
            spike_threshold = self.config.get(
                'dimensions.country.priority_tiers.tier_2_strategic_watch.alert_threshold_spike',
                100
            )
        else:
            z_threshold = self.config.get(
                'dimensions.country.priority_tiers.tier_3_all_others.z_score_threshold',
                5.0
            )
            drop_threshold = self.config.get(
                'dimensions.country.priority_tiers.tier_3_all_others.alert_threshold_drop',
                50
            )
            spike_threshold = self.config.get(
                'dimensions.country.priority_tiers.tier_3_all_others.alert_threshold_spike',
                200
            )

        # Detect anomalies
        is_anomaly = False
        severity = None
        reason = None

        # Check for drops or spikes
        z_score_exceeded = abs(z_score) > z_threshold
        drop_exceeded = pct_diff_median < -drop_threshold
        spike_exceeded = pct_diff_median > spike_threshold

        if z_score_exceeded or drop_exceeded or spike_exceeded:
            is_anomaly = True

            if z_score < -z_threshold or drop_exceeded:
                severity = 'CRITICAL' if tier == 1 else 'ALERT' if tier == 2 else 'WARNING'
                if z_score < -z_threshold:
                    reason = f'Significant drop: {abs(z_score):.1f}σ below normal'
                else:
                    reason = f'Significant drop: {abs(pct_diff_median):.1f}% below normal'
            else:
                severity = 'WARNING'
                if z_score > z_threshold:
                    reason = f'Significant spike: {z_score:.1f}σ above normal'
                else:
                    reason = f'Significant spike: {pct_diff_median:.1f}% above normal'

        # Special case: Zero values
        if current_value == 0 and mean > 100:
            is_anomaly = True
            severity = 'CRITICAL'
            reason = f'ZERO users (expected: {mean:,.0f})'

        if is_anomaly:
            return {
                'severity': severity,
                'tier': tier,
                'country': country,
                'platform': platform,
                'measure': 'users',  # Currently tracking componentCount (users)
                'temporal_label': temporal_label,
                'date': check_date_str,
                'period_date': lookup_date_str,
                'message': reason,
                'current_value': int(current_value),
                'expected_value': int(mean),
                'median_value': int(median),
                'z_score': float(z_score),
                'pct_diff_median': float(pct_diff_median),
                'historical_count': len(historical_values),
                'type': 'statistical_anomaly',
            }

        return None

    def generate_summary(
        self,
        anomalies: List[Dict[str, Any]],
        total_checked: int,
        check_date: date,
    ) -> Dict[str, Any]:
        """
        Generate summary statistics for detected anomalies.

        Args:
            anomalies: List of detected anomalies
            total_checked: Total number of combinations checked
            check_date: Date that was checked

        Returns:
            Summary dictionary
        """
        # Group by tier
        tier_1_anomalies = [a for a in anomalies if a['tier'] == 1]
        tier_2_anomalies = [a for a in anomalies if a['tier'] == 2]
        tier_3_anomalies = [a for a in anomalies if a['tier'] == 3]

        # Count critical anomalies
        critical_count = len([a for a in anomalies if a['severity'] == 'CRITICAL'])

        return {
            'check_date': str(check_date),
            'timestamp': check_date.strftime('%Y-%m-%d'),
            'total_checked': total_checked,
            'total_anomalies': len(anomalies),
            'total_normal': total_checked - len(anomalies),
            'normal_pct': ((total_checked - len(anomalies)) / total_checked * 100) if total_checked > 0 else 0,
            'anomaly_pct': (len(anomalies) / total_checked * 100) if total_checked > 0 else 0,
            'critical_anomalies': critical_count,
            'tier_1_summary': {
                'countries': self.tier_1_countries,
                'anomalies': len(tier_1_anomalies),
                'checked': len(self.tier_1_countries) * len(self.config.platforms) * len(self.config.temporal_labels),
                'issues': tier_1_anomalies,
            },
            'tier_2_summary': {
                'countries': self.tier_2_countries,
                'anomalies': len(tier_2_anomalies),
                'checked': len(self.tier_2_countries) * len(self.config.platforms) * len(self.config.temporal_labels),
                'issues': tier_2_anomalies,
            },
            'tier_3_summary': {
                'anomalies': len(tier_3_anomalies),
                'issues': tier_3_anomalies,
            },
        }
