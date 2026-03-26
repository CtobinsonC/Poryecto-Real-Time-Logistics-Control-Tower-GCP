import subprocess
import sys

cmd = [
    "gcloud", "scheduler", "jobs", "create", "http", "dbt-scheduler",
    "--location=us-central1",
    "--schedule=*/5 * * * *",
    "--uri=https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/gcp-real-time-logistics-cont/jobs/dbt-transformer:run",
    "--http-method=POST",
    "--oauth-service-account-email=logistics-dbt-sa@gcp-real-time-logistics-cont.iam.gserviceaccount.com"
]

result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
print("STDOUT:", result.stdout)
print("STDERR:", result.stderr)
if result.returncode != 0:
    sys.exit(result.returncode)
print("Scheduler creado exitosamente")
