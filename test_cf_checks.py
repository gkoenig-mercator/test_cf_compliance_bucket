import csv
import os
import subprocess
import tempfile
import boto3
from datetime import datetime
from dotenv import load_dotenv
import yaml


load_dotenv(override=True)  # loads the .env file into os.environ automatically

# ── Config ─────────────────────────────────────────────────────────────────
with open("config.yaml") as f:
    config = yaml.safe_load(f)

BUCKET_NAME    = config["bucket_name"]
ONE_PER_FOLDER = config["one_per_folder"]
OUTPUT_CSV     = config["output_csv"]

# ── S3 client from environment variables ───────────────────────────────────
s3 = boto3.client(
    "s3",
    endpoint_url          = f"{os.environ['AWS_S3_ENDPOINT']}",
    aws_access_key_id     = os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"],
    aws_session_token     = os.environ["AWS_SESSION_TOKEN"],
    region_name           = os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
)


def list_netcdf_files(bucket: str, one_per_folder: bool) -> list:
    """List NetCDF files in the bucket, optionally one per folder."""
    nc_extensions = (".nc", ".nc4", ".cdf", ".netcdf")
    paginator     = s3.get_paginator("list_objects_v2")
    selected      = {}

    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(nc_extensions):
                continue

            folder = os.path.dirname(key)

            if one_per_folder:
                if folder not in selected:
                    selected[folder] = obj
            else:
                selected[key] = obj

    return list(selected.values())


def run_cf_check(filepath: str) -> dict:
    """Run cfchecks on a local file."""
    try:
        result = subprocess.run(
            ["cfchecks", filepath],
            capture_output=True,
            text=True,
            timeout=120
        )
        stdout = result.stdout
        stderr = result.stderr

        errors   = stdout.lower().count("error:")
        warnings = stdout.lower().count("warning:")
        info     = stdout.lower().count("info:")
        passed   = "cf check successful" in stdout.lower() or errors == 0

        return {
            "status":   "OK" if passed else "FAILED",
            "errors":   errors,
            "warnings": warnings,
            "info":     info,
            "details":  stdout.strip() or stderr.strip(),
        }

    except subprocess.TimeoutExpired:
        return {"status": "TIMEOUT", "errors": 0, "warnings": 0, "info": 0, "details": "timed out"}
    except Exception as e:
        return {"status": "ERROR",   "errors": 0, "warnings": 0, "info": 0, "details": str(e)}


def main():
    print(f"Listing files in bucket: {BUCKET_NAME}")
    objects = list_netcdf_files(BUCKET_NAME, ONE_PER_FOLDER)
    print(f"Found {len(objects)} file(s) to check.\n")

    fieldnames = ["folder", "object_name", "size_bytes", "last_modified",
                  "cf_status", "errors", "warnings", "info", "details", "checked_at"]

    with open(OUTPUT_CSV, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for i, obj in enumerate(objects, 1):
            key  = obj["Key"]
            size = obj["Size"]
            print(f"[{i}/{len(objects)}] {key} ({size / 1e6:.1f} MB)")

            # Download to a temp file
            tmp_path = None
            try:
                suffix   = os.path.splitext(key)[-1]
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                    tmp_path = tmp.name

                s3.download_file(BUCKET_NAME, key, tmp_path)
                cf_result = run_cf_check(tmp_path)

            finally:
                if tmp_path and os.path.exists(tmp_path):
                    os.remove(tmp_path)

            writer.writerow({
                "folder":        os.path.dirname(key),
                "object_name":   os.path.basename(key),
                "size_bytes":    size,
                "last_modified": obj["LastModified"],
                "cf_status":     cf_result["status"],
                "errors":        cf_result["errors"],
                "warnings":      cf_result["warnings"],
                "info":          cf_result["info"],
                "details":       cf_result["details"],
                "checked_at":    datetime.utcnow().isoformat(),
            })

            print(f"    → {cf_result['status']} | errors={cf_result['errors']} warnings={cf_result['warnings']}\n")

    print(f"Done! Results saved to: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
