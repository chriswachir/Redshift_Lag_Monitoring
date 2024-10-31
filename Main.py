import psycopg2 as pg  # PostgreSQL database adapter for Python
import datetime  # For timestamping events
from configparser import ConfigParser  # For reading configuration from ini files
import smtplib  # For handling email sending
from email.mime.multipart import MIMEMultipart  # To create multi-part email messages
from email.mime.text import MIMEText  # For plain text content in emails
import requests  # For sending alerts to Slack via webhook

def send_email(subject, body, to_email, config):
    """
    Sends an email alert using SMTP with the specified subject and body.
    
    Parameters:
        subject (str): The subject of the email.
        body (str): The main content of the email.
        to_email (str): Recipient email address.
        config (dict): Contains SMTP configuration details.
    """
    # Initialize SMTP connection
    server = smtplib.SMTP_SSL(config['smtp_host'], config.getint('smtp_port'))
    fromaddr = config['smtp_username']
    my_pass = config['smtp_password']
    
    # Construct the email message
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        # Login and send the email
        server.login(fromaddr, my_pass)
        server.sendmail(fromaddr, to_email, msg.as_string())
        print(f"Email sent to {to_email} at {datetime.datetime.now()}")
    except Exception as e:
        print(f"Error sending email to {to_email}: {e}")
    finally:
        # Close the SMTP server connection
        server.quit()

def send_slack_alert(message, config):
    """
    Sends an alert message to Slack using a webhook URL.
    
    Parameters:
        message (str): The message content to be sent to Slack.
        config (dict): Contains Slack webhook configuration.
    """
    try:
        slack_webhook_url = config['slack_webhook_url']
        payload = {"text": message}  # Slack expects the payload to be in JSON format
        response = requests.post(slack_webhook_url, json=payload)
        if response.status_code == 200:
            print(f"Slack alert sent at {datetime.datetime.now()}")
        else:
            print(f"Slack alert failed with status code {response.status_code}")
    except Exception as e:
        print(f"Error sending Slack alert: {e}")

def server_config(filename, section):
    """
    Reads server or section-specific configurations from an ini file.
    
    Parameters:
        filename (str): Path to the ini configuration file.
        section (str): The section name in the ini file to read.
    
    Returns:
        dict: Configuration items for the specified section.
    """
    parser = ConfigParser()
    parser.read(filename)
    if parser.has_section(section):
        return dict(parser.items(section))
    else:
        raise Exception(f"Section {section} not found in the {filename} file")

def get_lag_and_alert(config_file):
    """
    Connects to the Redshift database, checks replication lag for each table defined,
    and sends an alert if the lag exceeds the threshold.
    
    Parameters:
        config_file (str): Path to the ini file containing database and alert configurations.
    """
    parser = ConfigParser()
    parser.read(config_file)

    # Load Redshift connection details
    config = server_config(config_file, 'yoda_r_lake')
    conn = None
    try:
        conn = pg.connect(**config)  # Establish Redshift database connection
        cur = conn.cursor()

        # Iterate over sections in the configuration file
        for section in parser.sections():
            if section.startswith('yoda_hub'):
                table_config = server_config(config_file, section)
                
                # Extract table and replication task details
                table_name = table_config['table']
                database_name = table_config['database']
                host_name = table_config['host']
                replication_task = table_config.get('replication_task', None)

                # Query to calculate lag in minutes
                q_get_lag = f"SELECT DATEDIFF(minute, MAX(dateCreated), GETDATE()) FROM {database_name}.{table_name}"
                cur.execute(q_get_lag)
                lag_result = cur.fetchone()
                lag = lag_result[0]

                # Check if the lag exceeds threshold (15 minutes) and send alerts if necessary
                if int(lag) > 15:
                    # Construct message for alerts
                    message = (f"Redshift has a lag of {lag} minutes for {database_name}.{table_name}.\n\n"
                               f"DETAILS:\nSource Host: {host_name}\n"
                               f"Source Database: {database_name}\n"
                               f"Source Table: {table_name}\n"
                               f"Replication Task: {replication_task}")
                    
                    # Send email and Slack alerts to primary recipients
                    send_email(f"Redshift Lag Alert: {database_name}.{table_name}", message, 
                               "reciever_email@gmail.com", server_config(config_file, 'email'))
                    send_slack_alert(message, server_config(config_file, 'slack'))

                    # If lag is critical (over 240 minutes), alert additional recipients
                    if int(lag) > 240:
                        send_email(f"Critical Lag Alert: {database_name}.{table_name}", message, 
                                   "reciever_email@gmail.com", server_config(config_file, 'email'))
                        send_slack_alert(message, server_config(config_file, 'slack'))
                else:
                    print(f"Replication lag is within acceptable limits for {database_name}.{table_name}")

        cur.close()
    except Exception as err:
        print(f"Fetching lag from Redshift failed with error ({err})")
    finally:
        if conn:
            conn.close()

def main():
    """
    Main function to execute the lag monitoring job, logging start and end times.
    """
    start_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print("\n........Starting Job..................", start_time, "\n")
    get_lag_and_alert('/path/to/r_lagMonitor.ini')
    end_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print("\n.......Job Finished.......", end_time, "\n")

# Entry point for the script
if __name__ == '__main__':
    main()
