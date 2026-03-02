# BigQuery Data Quality Monitoring

Automated data quality monitoring system for BigQuery with statistical anomaly detection, email alerts, and GitHub Actions integration.

## What It Does

- Statistical Analysis: Z-score based anomaly detection with seasonality awareness
- Smart Comparisons: Compares same day of week (Monday vs previous 8 Mondays)
- Weekly uses Monday week-start and skips the current (incomplete) week
- Monthly uses the first day of the month for comparisons
- Tiered Alerting: Different thresholds for different market priorities
- Automated Reports: Generates JSON and human-readable Markdown reports
- Email Notifications: Sends alerts when anomalies are detected
- GitHub Actions: Runs automatically on schedule

Perfect for sports apps, e-commerce, or any application with weekly/seasonal patterns.

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
nano .env
```

Required environment variables:
- GCP_PROJECT_ID - Your BigQuery project ID
- GCP_DATASET - Your BigQuery dataset name
- GCP_TABLE - Your BigQuery table name
- GOOGLE_APPLICATION_CREDENTIALS - Path to service account JSON key
- EMAIL_* - Email notification settings (optional)

### 3. Run the Check

```bash
python3 src/main.py
```

Reports are generated in reports/anomaly_check_*.md

## Google Cloud Setup

### Create Service Account

1. Go to Google Cloud Console
2. Navigate to IAM & Admin then Service Accounts
3. Click Create Service Account
4. Grant required roles:
   - BigQuery Data Viewer (read data)
   - BigQuery Job User (run queries)
5. Create and download JSON key
6. Set path in .env file

### Required Table Schema

Your BigQuery table should have these columns:
- calendarDate (DATE) - The date of the data
- temporalLabel (STRING) - Aggregation level
- country (STRING) - Country code
- platform (STRING) - Platform identifier
- componentCount (INTEGER) - User count or similar metric
- screenViews (INTEGER) - Views or similar metric

## Email Notifications (Optional)

### Gmail Setup

1. Enable 2-Factor Authentication on your Google account
2. Create App Password at: https://myaccount.google.com/apppasswords
3. Configure in .env:

```bash
EMAIL_ENABLED=true
EMAIL_SMTP_HOST=smtp.gmail.com
EMAIL_SMTP_PORT=587
EMAIL_USERNAME=your-email@gmail.com
EMAIL_PASSWORD=xxxx-xxxx-xxxx-xxxx
EMAIL_FROM=alerts@yourdomain.com
EMAIL_TO=team@yourdomain.com
```

## GitHub Actions Automation

### Setup Secrets

Go to Settings then Secrets and variables then Actions

Add these repository secrets:
- GCP_PROJECT_ID
- GCP_DATASET
- GCP_TABLE
- GCP_SERVICE_ACCOUNT_KEY (base64 encoded JSON)
- EMAIL_ENABLED
- EMAIL_SMTP_HOST
- EMAIL_SMTP_PORT
- EMAIL_USERNAME
- EMAIL_PASSWORD
- EMAIL_FROM
- EMAIL_TO

### Encode Service Account Key

```bash
base64 -i path/to/service-account-key.json | pbcopy
```

Paste the output as GCP_SERVICE_ACCOUNT_KEY secret.

### Workflow Schedule

The workflow runs daily at 9 AM UTC by default.

Manual trigger is available in the Actions tab.

## Configuration

### Alert Thresholds

Edit config/config.yaml to customize alert thresholds.

### Lookback Period

In .env set LOOKBACK_WEEKS=8

## Understanding Reports

Reports show:
- Overall Summary: Total checks, normal vs anomalies
- Tier 1 Markets: Your priority countries/platforms
- Anomaly Details: Z-scores, percentage changes, expected vs actual values
- Methodology: Explains the statistical approach

### What to Look For

Good:
- More than 95% of checks are normal
- Z-scores within 3 sigma for priority markets
- Consistent week-over-week patterns

Alert:
- Multiple anomalies in priority markets
- Z-scores greater than 3 sigma
- Zero values when traffic expected
- Sudden drops greater than 50%

## Troubleshooting

### 403 Insufficient authentication scopes

Solution: Ensure service account has both required roles:
- BigQuery Data Viewer
- BigQuery Job User

The code requires full BigQuery scope (not just readonly) to run query jobs.

### Email not sending

Check:
- EMAIL_ENABLED=true (exactly, lowercase)
- Using app password (not regular password)
- 2FA enabled on Google account
- Correct SMTP settings

### ModuleNotFoundError

Solution: pip install -r requirements.txt

### No data returned

Check:
- Table name is correct in .env
- Service account has access to the table
- Data exists for the date range
- Column names match expected schema

## Project Structure

```
bigquery-data-quality-checks/
├── src/
│   ├── main.py
│   ├── config.py
│   ├── clients/
│   │   ├── bigquery_client.py
│   │   └── email_client.py
│   ├── detectors/
│   │   └── anomaly_detector.py
│   └── utils/
│       ├── logger.py
│       └── retry.py
├── config/
│   └── config.yaml
├── keys/
│   └── README.md
├── reports/
├── logs/
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

## Security Best Practices

DO:
- Use service accounts with minimal required permissions
- Store credentials in .env (never commit)
- Use app passwords for email (not regular passwords)
- Rotate service account keys annually
- Keep .env in .gitignore
- Remove generated reports/logs before publishing publicly

DONT:
- Commit credentials to version control
- Share service account keys
- Use personal accounts for automation
- Expose internal project/table names publicly

## Maintenance

Daily: Review email alerts for anomalies
Weekly: Adjust thresholds if needed
Monthly: Review and tune configurations
Annually: Rotate service account keys, update dependencies

## Methodology

This system uses statistical anomaly detection that accounts for seasonality:

- Same Day of Week Comparison: Compares the current day against the same day of week from previous weeks
- Weekly Comparison: Uses Monday week-start and skips the current week for latency
- Monthly Comparison: Uses the first day of the month for comparisons
- Z-score Analysis: Measures how many standard deviations the current value is from the historical mean
- Tier-Based Thresholds: Different sensitivity levels for different market priorities
- Accounts for Patterns: Handles weekend spikes, event-driven traffic, and seasonal variations

---

Version: 2.0.0
Status: Production Ready
License: MIT

