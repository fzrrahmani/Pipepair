import os
import time

PIPELINE_DIR = os.getcwd()
REQUIRED_FILES = ['input.csv', 'config.json']
MAX_FILE_SIZE_MB = 100

print("Step 1: Validating pipeline directory...")

# Check for required files
for filename in REQUIRED_FILES:
    path = os.path.join(PIPELINE_DIR, filename)
    if not os.path.exists(path):
        print(f"[ERROR] Missing required file: {filename}")
        exit(1)
    else:
        print(f"[OK] Found: {filename}")

# Check file sizes
for file in os.listdir(PIPELINE_DIR):
    full_path = os.path.join(PIPELINE_DIR, file)
    if os.path.isfile(full_path):
        size_mb = os.path.getsize(full_path) / 1024 / 1024
        if size_mb > MAX_FILE_SIZE_MB:
            print(f"[WARNING] File '{file}' is too large ({size_mb:.2f} MB)")

# Optional: check for delays in file updates (for stale pipelines)
now = time.time()
for file in REQUIRED_FILES:
    full_path = os.path.join(PIPELINE_DIR, file)
    mtime = os.path.getmtime(full_path)
    age_minutes = (now - mtime) / 60
    if age_minutes > 60:
        print(f"[NOTICE] '{file}' is over {int(age_minutes)} minutes old")

print("Step 1 complete: Validation passed.")