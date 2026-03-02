"""
Main entry point for data quality monitoring system.
Production-grade with comprehensive error handling and monitoring.
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any

# Load environment variables from .env file FIRST
from dotenv import load_dotenv

# Find and load .env file
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    print(f"✅ Loaded environment from: {env_path}")
else:
    print(f"⚠️  No .env file found at: {env_path}")

# Add parent directory to path so imports work
sys.path.insert(0, str(Path(__file__).parent.parent))

# Now import from src package
from src.config import Config, ConfigurationError
from src.clients import BigQueryClient, EmailClient, BigQueryClientError
from src.detectors import AnomalyDetector
from src.utils import setup_logger, get_logger

# Initialize logger
logger = get_logger(__name__)


def save_report(summary: Dict[str, Any], anomalies: list, timestamp: str) -> tuple:
    """
    Save JSON and Markdown reports.

    Args:
        summary: Summary statistics
        anomalies: List of detected anomalies
        timestamp: Timestamp string

    Returns:
        Tuple of (json_path, markdown_path)
    """
    import json

    reports_dir = Path(__file__).parent.parent / "reports"
    reports_dir.mkdir(exist_ok=True)

    # JSON report
    report_data = {
        'summary': summary,
        'anomalies': anomalies,
        'metadata': {
            'generated_at': datetime.now().isoformat(),
            'version': '2.0.0',
            'method': 'statistical_anomaly_detection',
        }
    }

    json_path = reports_dir / f"anomaly_check_{timestamp}.json"
    with open(json_path, 'w') as f:
        json.dump(report_data, f, indent=2)

    # Markdown report
    md_content = generate_markdown_report(summary, anomalies)
    md_path = reports_dir / f"anomaly_check_{timestamp}.md"
    with open(md_path, 'w') as f:
        f.write(md_content)

    return str(json_path), str(md_path)


def generate_markdown_report(summary: Dict[str, Any], anomalies: list) -> str:
    """Generate human-readable Markdown report."""
    def build_grouped_anomalies(items: list) -> Dict[str, Dict[int, list]]:
        grouped = {label: {1: [], 2: [], 3: []} for label in ['daily', 'weekly', 'monthly']}
        for anomaly in items:
            label = anomaly.get('temporal_label')
            tier = anomaly.get('tier')
            if label in grouped and tier in grouped[label]:
                grouped[label][tier].append(anomaly)
        return grouped

    def compute_check_periods(check_date_str: str) -> Dict[str, str]:
        check_dt = datetime.strptime(check_date_str, '%Y-%m-%d')
        daily = check_dt.date()
        # Most recent complete week starts on Monday of the current week.
        week_start = daily - timedelta(days=daily.weekday())
        # Most recent complete month is the previous month.
        month_start = datetime(check_dt.year, check_dt.month, 1).date()
        prev_month_end = month_start - timedelta(days=1)
        prev_month_start = datetime(prev_month_end.year, prev_month_end.month, 1).date()
        return {
            'daily': str(daily),
            'weekly': str(week_start),
            'monthly': str(prev_month_start),
        }

    check_periods = compute_check_periods(summary['check_date'])

    lines = [
        "# 📊 Data Quality Anomaly Check Report",
        "",
        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"**Check Date:** {summary['check_date']}",
        f"**Daily Check:** {check_periods['daily']}",
        f"**Weekly Check (week start):** {check_periods['weekly']}",
        f"**Monthly Check (month start):** {check_periods['monthly']}",
        "",
        "---",
        "",
        "## 📈 OVERALL SUMMARY",
        "",
        f"**Total Combinations Checked:** {summary['total_checked']:,}",
        "",
        f"- ✅ **Normal:** {summary['total_normal']:,} ({summary['normal_pct']:.1f}%)",
        f"- ⚠️ **Anomalies Detected:** {summary['total_anomalies']:,} ({summary['anomaly_pct']:.1f}%)",
        "",
    ]

    # Status
    if summary['total_anomalies'] == 0:
        lines.append("### 🎉 STATUS: EXCELLENT")
        lines.append("")
        lines.append("All metrics are within normal patterns.")
    else:
        lines.append("### ⚠️ STATUS: ATTENTION REQUIRED")
        lines.append("")
        lines.append(f"{summary['total_anomalies']} anomalies detected - review needed.")

    lines.extend(["", "---", ""])

    # Core markets
    tier1 = summary['tier_1_summary']
    lines.extend([
        "## 🌟 TIER 1 - CORE BUSINESS MARKETS",
        "",
        f"**Countries:** {', '.join(tier1['countries'])}",
        "",
        f"**Combinations Checked:** {tier1['checked']}",
        f"- ✅ Normal: {tier1['checked'] - tier1['anomalies']}",
        f"- ⚠️ Anomalies: {tier1['anomalies']}",
        "",
    ])

    if tier1['issues']:
        lines.append("### 🔴 ANOMALIES DETECTED")
        lines.append("")
        for issue in tier1['issues']:
            temporal_period = issue.get('temporal_label', 'daily').upper()
            measure = issue.get('measure', 'users').capitalize()
            lines.extend([
                f"#### [{issue['severity']}] {issue['country']} - {issue['platform']} - {measure} ({temporal_period})",
                "",
                f"**Period Date:** {issue.get('period_date', issue.get('date', 'N/A'))}",
                f"**Issue:** {issue['message']}",
                "",
                "| Metric | Value |",
                "|--------|-------|",
                f"| Current | {issue['current_value']:,} |",
                f"| Expected (avg) | {issue['expected_value']:,} |",
                f"| Median | {issue['median_value']:,} |",
                f"| Z-score | {issue['z_score']:.2f}σ |",
                f"| % Diff from Median | {issue['pct_diff_median']:+.1f}% |",
                "",
            ])
    else:
        lines.extend([
            "### ✅ ALL CORE MARKETS HEALTHY",
            "",
            "No anomalies detected in core business markets!",
            "",
        ])

    lines.extend(["---", ""])

    # Detailed anomalies by temporal label and tier
    grouped = build_grouped_anomalies(anomalies)
    lines.extend([
        "## 📅 DETAILED ANOMALIES (DAILY, WEEKLY, MONTHLY)",
        "",
    ])

    for temporal_label in ['daily', 'weekly', 'monthly']:
        lines.extend([
            f"### {temporal_label.upper()} SUMMARY",
            "",
        ])
        for tier in [1, 2, 3]:
            tier_anomalies = grouped[temporal_label][tier]
            lines.extend([
                f"#### Tier {tier}",
                "",
            ])
            if tier_anomalies:
                for issue in tier_anomalies:
                    measure = issue.get('measure', 'users').capitalize()
                    lines.extend([
                        f"- [{issue['severity']}] {issue['country']} - {issue['platform']} - {measure}",
                        f"  - Period: {issue.get('period_date', issue.get('date', 'N/A'))}",
                        f"  - Issue: {issue['message']}",
                        f"  - Current: {issue['current_value']:,}",
                        f"  - Expected (avg): {issue['expected_value']:,}",
                        f"  - Median: {issue['median_value']:,}",
                        f"  - Z-score: {issue['z_score']:.2f}σ",
                        f"  - % Diff from Median: {issue['pct_diff_median']:+.1f}%",
                        "",
                    ])
            else:
                lines.extend([
                    "- ✅ No anomalies",
                    "",
                ])

    lines.extend(["---", ""])

    # Footer
    lines.extend([
        "## 💡 METHODOLOGY",
        "",
        "This report uses **statistical anomaly detection** accounting for sports seasonality:",
        "",
        f"- **Same Day of Week Comparison:** Compares {summary['check_date']} vs previous 8 weeks",
        "- **Multiple Temporal Granularities:** Checks daily, weekly, and monthly aggregations",
        "- **Z-score Analysis:** Measures standard deviations from historical mean",
        "- **Tier-Specific Thresholds:**",
        "  - Core markets: Alert at >3σ",
        "  - Strategic markets: Alert at >4σ",
        "  - Other markets: Alert at >5σ",
        "",
        "**Accounts for:** Weekend spikes, Champions League days, Premier League matches, etc.",
        "",
        "---",
        "",
        "*Report generated automatically by Data Quality Monitoring System v2.0*",
    ])

    return "\n".join(lines)


def main():
    """Main execution function."""
    try:
        # Setup logging
        log_level = os.getenv('LOG_LEVEL', 'INFO')
        log_file = Path(__file__).parent.parent / "logs" / "data_quality.log"
        setup_logger(log_level=log_level, log_file=str(log_file))

        logger.info("=" * 80)
        logger.info("Data Quality Monitoring System - Production v2.0")
        logger.info("=" * 80)

        # Load configuration
        logger.info("Loading configuration...")
        config = Config()
        logger.info("✓ Configuration loaded successfully")

        # Calculate check date (yesterday = most recent complete day)
        check_date = (datetime.now() - timedelta(days=1)).date()
        logger.info(f"Checking date: {check_date} ({check_date.strftime('%A')})")

        # Initialize BigQuery client
        logger.info("Connecting to BigQuery...")
        bq_client = BigQueryClient(config)
        logger.info("✓ BigQuery client initialized")

        # Get historical data
        weeks_back = int(os.getenv('LOOKBACK_WEEKS', '8'))
        logger.info(f"Retrieving {weeks_back} weeks of historical data...")

        historical_data = bq_client.get_historical_data(
            check_date=check_date,
            weeks_back=weeks_back,
        )

        logger.info(f"✓ Retrieved data for {len(historical_data)} combinations")

        # Detect anomalies
        logger.info("Analyzing data for anomalies...")
        detector = AnomalyDetector(config)

        anomalies = detector.detect_anomalies(
            historical_data=historical_data,
            check_date=check_date,
        )

        # Calculate total checked
        total_checked = len(historical_data)

        # Generate summary
        summary = detector.generate_summary(
            anomalies=anomalies,
            total_checked=total_checked,
            check_date=check_date,
        )

        # Display summary
        logger.info("")
        logger.info("=" * 80)
        logger.info(" ANALYSIS COMPLETE")
        logger.info("=" * 80)
        logger.info(f" Total Checked: {total_checked:,}")
        logger.info(f" ✅ Normal: {summary['total_normal']:,} ({summary['normal_pct']:.1f}%)")
        logger.info(f" ⚠️  Anomalies: {summary['total_anomalies']:,} ({summary['anomaly_pct']:.1f}%)")
        logger.info("=" * 80)

        if summary['tier_1_summary']['anomalies'] > 0:
            logger.warning(
                f"⚠️  {summary['tier_1_summary']['anomalies']} anomalies "
                f"detected in CORE MARKETS"
            )
        else:
            logger.info("🎉 All core markets are healthy!")

        logger.info("")

        # Save reports
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_path, md_path = save_report(summary, anomalies, timestamp)

        logger.info(f"📝 JSON report: {json_path}")
        logger.info(f"📄 Markdown report: {md_path}")

        # Send email notification
        if os.getenv('EMAIL_ENABLED', 'false').lower() == 'true':
            logger.info("Sending email notification...")
            email_client = EmailClient(config)

            email_sent = email_client.send_anomaly_alert(summary, anomalies)

            if email_sent:
                logger.info("✓ Email notification sent successfully")
            else:
                logger.warning("⚠ Email notification failed (check logs)")
        else:
            logger.info("Email notifications are disabled")

        # Close connections
        bq_client.close()

        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ DATA QUALITY CHECK COMPLETE")
        logger.info("=" * 80)

        # Exit code based on anomalies
        if summary['critical_anomalies'] > 0:
            logger.warning("Exiting with code 1 (critical anomalies detected)")
            return 1

        return 0

    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        logger.error("Please check your .env file and config.yaml")
        return 2

    except BigQueryClientError as e:
        logger.error(f"BigQuery error: {e}")
        logger.error("Please check your GCP credentials and permissions")
        return 3

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 4


if __name__ == "__main__":
    sys.exit(main())

