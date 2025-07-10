import os
import time
import pytz
import smtplib
import schedule
from datetime import datetime
from faker import Faker
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pyspark.sql.functions import current_timestamp, lit

# Configuration
delta_path = "/tmp/delta/users"  # Set your Delta table path
num_rows_per_batch = 10
interval_minutes = 5
timezone = "Asia/Kolkata"
email_sender = "youremail@example.com"
email_receiver = "receiver@example.com"
smtp_server = "smtp.gmail.com"
smtp_port = 587
smtp_password = "yourpassword"

# Initialize Spark
spark = (
    SparkSession.builder.appName("DeltaIngestionPipeline")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.conf.set("spark.sql.session.timeZone", timezone)

# Generate Fake Data
def generate_fake_data(n):
    faker = Faker()
    data = [(faker.name(), faker.address(), faker.email()) for _ in range(n)]
    return spark.createDataFrame(data, ["Name", "Address", "Email"]).withColumn("Timestamp", current_timestamp())

# Append to Delta Table
def append_to_delta(df):
    if not DeltaTable.isDeltaTable(spark, delta_path):
        df.write.format("delta").mode("overwrite").save(delta_path)
    else:
        df.write.format("delta").mode("append").save(delta_path)

# Get Latest Version
def get_latest_version():
    return DeltaTable.forPath(spark, delta_path).history().select("version").orderBy("version", ascending=False).first()["version"]

# Get Latest Records by Timestamp
def get_latest_data():
    df = spark.read.format("delta").load(delta_path)
    max_ts = df.agg({"Timestamp": "max"}).collect()[0][0]
    return df.filter(df["Timestamp"] == max_ts)

# Send Email Notification
def send_email(subject, html_content):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = email_sender
    msg["To"] = email_receiver

    part = MIMEText(html_content, "html")
    msg.attach(part)

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(email_sender, smtp_password)
        server.sendmail(email_sender, email_receiver, msg.as_string())

# Ingestion Task
def run_pipeline():
    print("Running pipeline at", datetime.now(pytz.timezone(timezone)))
    df = generate_fake_data(num_rows_per_batch)
    append_to_delta(df)
    latest_df = get_latest_data()
    version = get_latest_version()

    html_table = latest_df.toPandas().to_html(index=False)
    send_email(
        subject=f"Delta Pipeline Update - Version {version}",
        html_content=f"<h2>New Data Appended</h2><p>Version: {version}</p>{html_table}"
    )
    print(f"Version {version} data appended and email sent.")

# Scheduler
schedule.every(interval_minutes).minutes.do(run_pipeline)

if __name__ == "__main__":
    print(f"Starting scheduler... Interval = {interval_minutes} minutes")
    run_pipeline()  # Run once at start
    while True:
        schedule.run_pending()
        time.sleep(1)
