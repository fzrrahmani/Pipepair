import subprocess, time, yaml, os, sys
from datetime import datetime

LOG_FILE = "pipepair.log"

def log(msg):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    with open(LOG_FILE, "a") as f:
        f.write(f"{timestamp} {msg}\n")
    print(f"{timestamp} {msg}")

def run_command(cmd):
    try:
        result = subprocess.run(cmd, shell=True, check=True)
        return result.returncode == 0
    except subprocess.CalledProcessError:
        return False

def run_step(step):
    name = step.get("name", "unnamed")
    cmd = step.get("command")
    retries = step.get("retry", 0)
    fallback = step.get("on_fail", None)

    log(f"Starting step: {name}")
    for attempt in range(retries + 1):
        if run_command(cmd):
            log(f"Step succeeded: {name}")
            return True
        log(f"Step failed: {name}, attempt {attempt + 1} of {retries + 1}")
        time.sleep(1)

    if fallback:
        log(f"Trying fallback for step: {name}")
        return run_command(fallback)

    log(f"No fallback. Step permanently failed: {name}")
    return False

def run_pipeline(file="pipeline.yaml"):
    if not os.path.exists(file):
        log("pipeline.yaml not found!")
        sys.exit(1)
    with open(file, "r") as f:
        steps = yaml.safe_load(f).get("pipeline", [])
    for step in steps:
        if not run_step(step):
            log("Pipeline halted due to failure.")
            sys.exit(1)
    log("Pipeline completed successfully.")