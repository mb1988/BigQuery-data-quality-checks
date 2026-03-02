"""
Email notification client with SMTP support.
Sends alerts with HTML and plain text versions (no PII).
"""
import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, List
from datetime import datetime, timedelta

from src.config import Config

logger = logging.getLogger(__name__)


class EmailClientError(Exception):
    """Base exception for email client errors."""
    pass


class EmailClient:
    """
    Email notification client using SMTP.

    Features:
    - HTML and plain text email
    - No PII in emails (only aggregates)
    - Graceful failure (doesn't crash monitoring)
    - Structured logging
    """

    def __init__(self, config: Config):
        """
        Initialize email client.

        Args:
            config: Application configuration
        """
        self.config = config
        email_config = config.email_config

        self.smtp_host = email_config['smtp_host']
        self.smtp_port = email_config['smtp_port']
        self.username = email_config['username']
        self.password = email_config['password']
        self.from_addr = email_config['from_addr']
        self.to_addrs = [addr.strip() for addr in email_config['to_addrs'] if addr.strip()]
        self.enabled = email_config['enabled']

        if not self.enabled:
            logger.info("Email notifications are disabled")

    def send_anomaly_alert(self, summary: Dict[str, Any], anomalies: List[Dict[str, Any]]) -> bool:
        """
        Send email alert for detected anomalies.

        Args:
            summary: Anomaly detection summary with statistics
            anomalies: List of detected anomalies

        Returns:
            True if email sent successfully, False otherwise
        """
        if not self.enabled:
            logger.debug("Email disabled, skipping notification")
            return False

        if not self._validate_config():
            logger.warning("Email configuration incomplete, skipping notification")
            return False

        try:
            subject = self._generate_subject(summary)
            text_body = self._generate_text_body(summary, anomalies)
            html_body = self._generate_html_body(summary, anomalies)

            return self._send_email(subject, text_body, html_body)

        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
            # Don't raise - email failure shouldn't stop the monitoring
            return False

    def _validate_config(self) -> bool:
        """Validate email configuration."""
        if not self.smtp_host:
            logger.error("EMAIL_SMTP_HOST not configured")
            return False
        if not self.username:
            logger.error("EMAIL_USERNAME not configured")
            return False
        if not self.password:
            logger.error("EMAIL_PASSWORD not configured")
            return False
        if not self.to_addrs:
            logger.error("EMAIL_TO not configured")
            return False
        return True

    def _generate_subject(self, summary: Dict[str, Any]) -> str:
        """Generate email subject line."""
        anomaly_count = summary.get('total_anomalies', 0)
        date = summary.get('check_date', datetime.now().strftime('%Y-%m-%d'))

        if anomaly_count == 0:
            return f"✅ Data Quality Check - All Clear ({date})"
        elif summary.get('critical_anomalies', 0) > 0:
            return f"🔴 Data Quality Alert - {anomaly_count} Anomalies Detected ({date})"
        else:
            return f"⚠️ Data Quality Alert - {anomaly_count} Anomalies Detected ({date})"

    def _generate_text_body(self, summary: Dict[str, Any], anomalies: List[Dict[str, Any]]) -> str:
        """Generate plain text email body."""
        grouped = self._group_anomalies_by_temporal_tier(anomalies)
        check_periods = self._compute_check_periods(summary.get('check_date', ''))
        lines = [
            "LiveScore Data Quality Check",
            "=" * 50,
            "",
            f"Date: {summary.get('check_date', 'N/A')}",
            f"Check Time: {summary.get('timestamp', 'N/A')}",
            f"Daily Check: {check_periods['daily']}",
            f"Weekly Check (week start): {check_periods['weekly']}",
            f"Monthly Check (month start): {check_periods['monthly']}",
            "",
        ]

        # Overall status
        status = "⚠️ ATTENTION REQUIRED" if summary.get('total_anomalies', 0) > 0 else "✅ ALL NORMAL"
        lines.append(f"Status: {status}")
        lines.append("")

        # Summary statistics
        lines.append("Summary:")
        lines.append(f"- Total Checks: {summary.get('total_checked', 0):,}")
        lines.append(f"- Normal: {summary.get('total_normal', 0):,} ({summary.get('normal_pct', 0):.1f}%)")
        lines.append(f"- Anomalies: {summary.get('total_anomalies', 0):,} ({summary.get('anomaly_pct', 0):.1f}%)")
        lines.append("")

        # Full anomaly breakdown
        lines.append("Anomalies (daily, weekly, monthly by tier):")
        lines.append("")
        for temporal_label in ['daily', 'weekly', 'monthly']:
            lines.append(f"{temporal_label.upper()}:")
            for tier in [1, 2, 3]:
                issues = grouped[temporal_label][tier]
                lines.append(f"  Tier {tier}:")
                if issues:
                    for issue in issues:
                        period = issue.get('period_date', issue.get('date', 'N/A'))
                        lines.append(
                            f"    - [{issue['severity']}] {issue['country']} / {issue['platform']} ({period}): {issue['message']}"
                        )
                else:
                    lines.append("    - No anomalies")
            lines.append("")

        # Action items
        if summary.get('total_anomalies', 0) > 0:
            lines.append("Action Required:")
            lines.append("- Review the anomalies in the full report")
            lines.append("- Check if data collection is functioning properly")
            lines.append("- Investigate significant drops in core markets")
        else:
            lines.append("No action required - all metrics within normal ranges.")

        lines.append("")
        lines.append("-" * 50)
        lines.append("This is an automated email. Do not reply.")

        return "\n".join(lines)

    def _generate_html_body(self, summary: Dict[str, Any], anomalies: List[Dict[str, Any]]) -> str:
        """Generate HTML email body."""
        grouped = self._group_anomalies_by_temporal_tier(anomalies)
        check_periods = self._compute_check_periods(summary.get('check_date', ''))
        status_color = "#dc3545" if summary.get('total_anomalies', 0) > 0 else "#28a745"
        status_text = "⚠️ ATTENTION REQUIRED" if summary.get('total_anomalies', 0) > 0 else "✅ ALL NORMAL"

        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .header {{ background: #f8f9fa; padding: 20px; border-radius: 5px; }}
                .status {{ font-size: 18px; font-weight: bold; color: {status_color}; }}
                .metric {{ display: inline-block; margin: 10px 20px 10px 0; }}
                .metric-label {{ color: #666; font-size: 14px; }}
                .metric-value {{ font-size: 24px; font-weight: bold; color: #333; }}
                .section {{ margin: 20px 0; padding: 15px; background: #f8f9fa; border-radius: 5px; }}
                .issue {{ padding: 8px; margin: 5px 0; background: #fff3cd; border-left: 3px solid #ffc107; }}
                .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #dee2e6; color: #666; font-size: 12px; }}
                table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
                th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background: #f8f9fa; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>LiveScore Data Quality Check</h2>
                <div class="status">{status_text}</div>
                <p><strong>Date:</strong> {summary.get('check_date', 'N/A')}</p>
                <p><strong>Daily Check:</strong> {check_periods['daily']}</p>
                <p><strong>Weekly Check (week start):</strong> {check_periods['weekly']}</p>
                <p><strong>Monthly Check (month start):</strong> {check_periods['monthly']}</p>
            </div>
            
            <div class="section">
                <h3>Summary</h3>
                <div class="metric">
                    <div class="metric-label">Total Checks</div>
                    <div class="metric-value">{summary.get('total_checked', 0):,}</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Normal</div>
                    <div class="metric-value" style="color: #28a745;">{summary.get('total_normal', 0):,}</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Anomalies</div>
                    <div class="metric-value" style="color: #dc3545;">{summary.get('total_anomalies', 0):,}</div>
                </div>
            </div>
        """

        # Full anomaly breakdown
        html += "<div class='section'><h3>Anomalies (daily, weekly, monthly by tier)</h3>"
        for temporal_label in ['daily', 'weekly', 'monthly']:
            html += f"<h4>{temporal_label.upper()}</h4>"
            for tier in [1, 2, 3]:
                issues = grouped[temporal_label][tier]
                html += f"<h5>Tier {tier}</h5>"
                if issues:
                    for issue in issues:
                        period = issue.get('period_date', issue.get('date', 'N/A'))
                        html += f"""
                        <div class="issue">
                            <strong>[{issue['severity']}] {issue['country']} / {issue['platform']} ({period})</strong><br>
                            {issue['message']}<br>
                            <small>Current: {issue.get('current_value', 'N/A'):,} | Expected: {issue.get('expected_value', 'N/A'):,}</small>
                        </div>
                        """
                else:
                    html += "<div class='issue'><em>No anomalies</em></div>"
        html += "</div>"

        # Action items
        html += "<div class='section'>"
        if summary.get('total_anomalies', 0) > 0:
            html += """
            <h3>Action Required</h3>
            <ul>
                <li>Review the anomalies in the full report</li>
                <li>Check if data collection is functioning properly</li>
                <li>Investigate significant drops in core markets</li>
            </ul>
            """
        else:
            html += "<h3>No Action Required</h3><p>All metrics are within normal ranges.</p>"
        html += "</div>"

        html += """
            <div class="footer">
                <p>This is an automated email from the LiveScore Data Quality Monitoring System.</p>
                <p>Do not reply to this email.</p>
            </div>
        </body>
        </html>
        """

        return html

    def _send_email(self, subject: str, text_body: str, html_body: str) -> bool:
        """
        Send email via SMTP.

        Args:
            subject: Email subject
            text_body: Plain text body
            html_body: HTML body

        Returns:
            True if sent successfully
        """
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.from_addr
            msg['To'] = ', '.join(self.to_addrs)

            # Attach both versions
            msg.attach(MIMEText(text_body, 'plain'))
            msg.attach(MIMEText(html_body, 'html'))

            # Send via SMTP
            with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=30) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg)

            logger.info(f"Email sent successfully to {', '.join(self.to_addrs)}")
            return True

        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"SMTP authentication failed: {e}")
            return False
        except smtplib.SMTPException as e:
            logger.error(f"SMTP error: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False

    @staticmethod
    def _group_anomalies_by_temporal_tier(
        anomalies: List[Dict[str, Any]]
    ) -> Dict[str, Dict[int, List[Dict[str, Any]]]]:
        grouped = {label: {1: [], 2: [], 3: []} for label in ['daily', 'weekly', 'monthly']}
        for anomaly in anomalies:
            label = anomaly.get('temporal_label')
            tier = anomaly.get('tier')
            if label in grouped and tier in grouped[label]:
                grouped[label][tier].append(anomaly)
        return grouped

    @staticmethod
    def _compute_check_periods(check_date_str: str) -> Dict[str, str]:
        if not check_date_str:
            return {'daily': 'N/A', 'weekly': 'N/A', 'monthly': 'N/A'}
        try:
            check_dt = datetime.strptime(check_date_str, '%Y-%m-%d')
        except ValueError:
            return {'daily': 'N/A', 'weekly': 'N/A', 'monthly': 'N/A'}
        daily = check_dt.date()
        week_start = daily - timedelta(days=daily.weekday())
        month_start = datetime(check_dt.year, check_dt.month, 1).date()
        prev_month_end = month_start - timedelta(days=1)
        prev_month_start = datetime(prev_month_end.year, prev_month_end.month, 1).date()
        return {
            'daily': str(daily),
            'weekly': str(week_start),
            'monthly': str(prev_month_start),
        }
