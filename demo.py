import subprocess
import time
import os
import signal
import json
import sys
import redis

def run_command(command, wait=True, **kwargs):
    """Helper to run a command and print its output."""
    print(f"\n--- Running: {' '.join(command)} ---")
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True, **kwargs)
    if wait:
        stdout, stderr = process.communicate()
        if stdout:
            print(stdout)
        if stderr:
            print(stderr, file=sys.stderr)
    return process

def main():
    """
    Runs a demonstration of the queuectl system, showcasing various job states
    and features, which can be observed on the web UI.
    """
    webui_process = None
    worker_process = None
    
    try:
        # 1. Cleanup and Preparation
        print("--- Preparing for demo: Cleaning up old processes and data ---")
        run_command(["queuectl", "worker", "stop"])
        
        try:
            run_command(["docker", "exec", "fervent_wilson", "redis-cli", "flushall"])
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("Could not flush Redis via Docker. Please ensure Docker is running and the container 'fervent_wilson' exists.", file=sys.stderr)
            pass

        run_command(["queuectl", "config", "set", "max-retries", "2"])
        run_command(["queuectl", "config", "set", "backoff-base", "2"])

        # 2. Start Background Processes
        print("\n--- Starting background processes (Web UI and Worker) ---")
        webui_process = run_command(["queuectl", "webui", "--port", "5001"], wait=False)
        time.sleep(3)
        print("Web UI should be available at http://127.0.0.1:5001")

        worker_process = run_command(["queuectl", "worker", "start"], wait=False)
        time.sleep(2)

        # 3. Demo Sequence
        print("\n--- Starting demo sequence ---")
        print("Watch the web UI to see the changes in real-time.")

        print("\nStep 1: Enqueueing a successful job.")
        job_spec_success = json.dumps({"command": "echo 'This job completed successfully!'"})
        run_command(["queuectl", "enqueue", job_spec_success])
        
        print("\nStep 2: Enqueueing a job that will fail and go to the DLQ.")
        job_spec_fail = json.dumps({"command": "non_existent_command"})
        run_command(["queuectl", "enqueue", job_spec_fail])
        
        print("\nStep 3: Enqueueing a job that will time out.")
        job_spec_timeout = json.dumps({"command": "timeout /t 10", "timeout": 3, "max_retries": 1})
        run_command(["queuectl", "enqueue", job_spec_timeout])
        
        print("\nStep 4: Enqueueing jobs with different priorities to show priority processing.")
        job_spec_low = json.dumps({"id": "job-low", "command": "echo 'Low priority job'", "priority": 10})
        job_spec_high = json.dumps({"id": "job-high", "command": "echo 'High priority job'", "priority": 1})
        job_spec_medium = json.dumps({"id": "job-medium", "command": "echo 'Medium priority job'", "priority": 5})
        run_command(["queuectl", "enqueue", job_spec_low])
        run_command(["queuectl", "enqueue", job_spec_medium])
        run_command(["queuectl", "enqueue", job_spec_high])
        
        print("\n--- Waiting for worker to process all jobs... ---")
        r = redis.Redis(decode_responses=True)
        while r.zcard("queue:priority") > 0 or r.zcard("queue:scheduled") > 0 or r.llen("queue:processing") > 0:
            time.sleep(2)
            print(f"Waiting... (Priority: {r.zcard('queue:priority')}, Scheduled: {r.zcard('queue:scheduled')}, Processing: {r.llen('queue:processing')})")
        
        print("\nAll initial jobs processed.")
        time.sleep(2)

        print("\nStep 5: Retrying a job from the DLQ.")
        dlq_jobs_output = subprocess.check_output(["queuectl", "list", "--state", "dead"], shell=True).decode()
        
        # Write to file for debugging
        with open("dlq_output.json", "w") as f:
            f.write(dlq_jobs_output)
        
        try:
            with open("dlq_output.json", "r") as f:
                dlq_jobs = json.load(f)
            
            print(f"DEBUG: Parsed dlq_jobs type: {type(dlq_jobs)}")
            print(f"DEBUG: Parsed dlq_jobs content: {dlq_jobs}")
            
            if dlq_jobs:
                job_to_retry = dlq_jobs[0].get("id")
                if job_to_retry:
                    run_command(["queuectl", "dlq", "retry", job_to_retry])
                    print(f"Job {job_to_retry} sent for retry. It will fail again and return to the DLQ.")
                    time.sleep(15)
            else:
                print("No jobs found in the DLQ to retry.")
        except (json.JSONDecodeError, IndexError) as e:
            print(f"Could not parse DLQ jobs or DLQ is empty. Error: {e}")

        print("\n--- Demo sequence finished ---")
        time.sleep(10)

    finally:
        # 4. Final Cleanup
        print("\n--- Tearing down demo ---")
        if worker_process:
            print("Stopping worker...")
            run_command(["queuectl", "worker", "stop"])
            worker_process.terminate()
        if webui_process:
            print("Stopping web UI...")
            webui_process.terminate()
        
        if os.path.exists("dlq_output.json"):
            os.remove("dlq_output.json")
            
        print("\nDemo finished. All processes have been terminated.")

if __name__ == "__main__":
    main()