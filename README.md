# Redshift Lag Monitor

A Python script to monitor replication lag in Amazon Redshift, with email and Slack alerting capabilities.

## Project Structure
- `config/r_lagMonitor.ini`: Configuration for database connection and monitored tables.
- `secrets/emailConfig.ini`: SMTP and Slack webhook configuration (excluded from version control).
- `logs/lag_monitor.log`: Log file.

## Usage
1. Configure your `.ini` files with the required database and alert details.
2. Run the script with:
   ```bash
   python3 main.py
3. Schedule this using cron or airflow to run every 30 minutes or so.
