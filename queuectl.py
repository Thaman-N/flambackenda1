import argparse
import json
import redis
import sys
import time
import uuid
from datetime import datetime, timezone
import subprocess
import signal
import os

# --- Web UI Dependencies ---
try:
    from flask import Flask, render_template_string, redirect, url_for
except ImportError:
    Flask = None

# --- Redis Connection ---
def get_redis_connection():
    """Establishes a connection to Redis."""
    try:
        r = redis.Redis(decode_responses=True, db=0)
        r.ping()
        return r
    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis: {e}", file=sys.stderr)
        sys.exit(1)

# --- Job Model ---
def create_job(job_spec):
    """Creates a new job dictionary from a JSON specification."""
    now = datetime.now(timezone.utc).isoformat()
    return {
        "id": job_spec.get("id", str(uuid.uuid4())),
        "command": job_spec["command"],
        "state": "pending",
        "attempts": 0,
        "max_retries": int(job_spec.get("max_retries", 3)),
        "timeout": int(job_spec.get("timeout", 0)),
        "priority": int(job_spec.get("priority", 0)),
        "created_at": now,
        "updated_at": now,
    }

# --- Queue Operations ---
def enqueue_job(r, job):
    """Adds a job to the priority queue."""
    job_id = job["id"]
    r.hset(f"job:{job_id}", mapping=job)
    r.zadd("queue:priority", {job_id: job["priority"]})
    print(f"Enqueued job {job_id} with priority {job['priority']}")

def get_job(r, job_id):
    """Retrieves a job from Redis, including its output."""
    job_data = r.hgetall(f"job:{job_id}")
    if job_data:
        output = r.get(f"job:{job_id}:output")
        if output:
            job_data["output"] = output
    return job_data

def update_job_state(r, job_id, state, output=None):
    """Updates the state of a job and optionally stores its output."""
    pipe = r.pipeline()
    pipe.hset(f"job:{job_id}", "state", state)
    pipe.hset(f"job:{job_id}", "updated_at", datetime.now(timezone.utc).isoformat())
    if output is not None:
        pipe.set(f"job:{job_id}:output", output)
    pipe.execute()

# --- Worker Logic ---
class Worker:
    def __init__(self, r, worker_id):
        self.r = r
        self.worker_id = worker_id
        self.shutdown = False
        self.pid = os.getpid()

    def signal_handler(self, signum, frame):
        print(f"Worker {self.worker_id} (PID: {self.pid}) received OS signal {signum}. Initiating graceful shutdown.")
        self.shutdown = True

    def move_scheduled_jobs(self):
        """Moves jobs from the scheduled set to the priority queue if they are due."""
        now = time.time()
        job_ids_with_scores = self.r.zrangebyscore("queue:scheduled", "-inf", now, withscores=True)
        if not job_ids_with_scores:
            return

        pipe = self.r.pipeline()
        for job_id, score in job_ids_with_scores:
            job_data = self.r.hgetall(f"job:{job_id}")
            if job_data:
                priority = int(job_data.get("priority", 0))
                pipe.zadd("queue:priority", {job_id: priority})
            pipe.zrem("queue:scheduled", job_id)
        results = pipe.execute()
        if any(results):
            print(f"Moved {len(job_ids_with_scores)} scheduled jobs to the priority queue.")

    def start(self):
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        self.r.sadd("workers:active", self.pid)
        print(f"Worker {self.worker_id} (PID: {self.pid}) started.")

        backoff_base = int(self.r.get("config:backoff_base") or 2)

        while not self.shutdown:
            if self.r.exists("worker:shutdown_signal"):
                print(f"Worker {self.worker_id} (PID: {self.pid}) detected Redis shutdown signal. Finishing current job...")
                self.shutdown = True
                break

            self.move_scheduled_jobs()

            job_item = self.r.zpopmin("queue:priority", count=1)
            if not job_item:
                time.sleep(1)
                continue
            
            job_id = job_item[0][0]
            self.r.lpush("queue:processing", job_id)

            job = get_job(self.r, job_id)
            if not job:
                self.r.lrem("queue:processing", 1, job_id)
                continue
            
            update_job_state(self.r, job_id, "processing")
            print(f"Worker {self.worker_id} processing job {job_id}")

            job_output = ""
            try:
                timeout_val = int(job.get("timeout", 0))
                if timeout_val > 0:
                    process = subprocess.run(job["command"], shell=True, check=True, capture_output=True, text=True, timeout=timeout_val)
                else:
                    process = subprocess.run(job["command"], shell=True, check=True, capture_output=True, text=True)
                job_output = process.stdout + process.stderr
                update_job_state(self.r, job_id, "completed", output=job_output)
                self.r.lrem("queue:processing", 1, job_id)
                self.r.lpush("queue:completed", job_id)
                print(f"Job {job_id} completed successfully.")
            except subprocess.TimeoutExpired as e:
                job_output = f"Job timed out after {timeout_val} seconds. {e.stdout}{e.stderr}"
                print(f"Job {job_id} timed out.")
                job["attempts"] = self.r.hincrby(f"job:{job_id}", "attempts", 1)
                if int(job["attempts"]) >= int(job["max_retries"]):
                    update_job_state(self.r, job_id, "dead", output=job_output)
                    self.r.lrem("queue:processing", 1, job_id)
                    self.r.lpush("queue:dead", job_id)
                    print(f"Job {job_id} failed permanently due to timeout. Moved to DLQ.")
                else:
                    update_job_state(self.r, job_id, "failed", output=job_output)
                    delay = backoff_base ** int(job["attempts"])
                    retry_at = time.time() + delay
                    self.r.zadd("queue:scheduled", {job_id: retry_at})
                    self.r.lrem("queue:processing", 1, job_id)
                    print(f"Job {job_id} failed due to timeout. Will retry in {delay} seconds.")
            except subprocess.CalledProcessError as e:
                job_output = e.stdout + e.stderr
                job["attempts"] = self.r.hincrby(f"job:{job_id}", "attempts", 1)
                if int(job["attempts"]) >= int(job["max_retries"]):
                    update_job_state(self.r, job_id, "dead", output=job_output)
                    self.r.lrem("queue:processing", 1, job_id)
                    self.r.lpush("queue:dead", job_id)
                    print(f"Job {job_id} failed permanently. Moved to DLQ.")
                else:
                    update_job_state(self.r, job_id, "failed", output=job_output)
                    delay = backoff_base ** int(job["attempts"])
                    retry_at = time.time() + delay
                    self.r.zadd("queue:scheduled", {job_id: retry_at})
                    self.r.lrem("queue:processing", 1, job_id)
                    print(f"Job {job_id} failed. Will retry in {delay} seconds.")
            except Exception as e:
                job_output = str(e)
                print(f"An unexpected error occurred while processing job {job_id}: {e}")
                update_job_state(self.r, job_id, "failed", output=job_output)
                self.r.lrem("queue:processing", 1, job_id)
                self.r.lpush("queue:failed", job_id)

        self.r.srem("workers:active", self.pid)
        print(f"Worker {self.worker_id} (PID: {self.pid}) shut down.")

# --- Web UI ---
if Flask:
    app = Flask(__name__)
    app.config['r'] = get_redis_connection()

    HTML_TEMPLATE = """
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <meta http-equiv="refresh" content="5">
        <title>QueueCTL Dashboard</title>
        <style>
          body { font-family: sans-serif; margin: 2em; background-color: #f8f9fa; color: #212529; }
          .container { max-width: 1200px; margin: auto; }
          h1, h2 { color: #007bff; }
          .summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 1em; margin-bottom: 2em; }
          .summary-card { background-color: #fff; border: 1px solid #dee2e6; border-radius: .25rem; padding: 1em; text-align: center; }
          .summary-card h3 { margin: 0; font-size: 1.2em; }
          .summary-card .count { font-size: 2.5em; font-weight: bold; color: #007bff; }
          table { width: 100%; border-collapse: collapse; margin-bottom: 2em; background-color: #fff; }
          th, td { border: 1px solid #dee2e6; padding: .75em; text-align: left; }
          th { background-color: #e9ecef; }
          pre { background-color: #e9ecef; padding: 1em; border-radius: .25rem; white-space: pre-wrap; word-wrap: break-word; }
          .job-id { font-family: monospace; font-size: 0.9em; }
          .actions a { text-decoration: none; color: #007bff; margin-right: 1em; }
        </style>
      </head>
      <body>
        <div class="container">
          <h1>QueueCTL Dashboard</h1>
          
          <h2>Summary</h2>
          <div class="summary-grid">
            <div class="summary-card"><h3>Pending</h3><div class="count">{{ stats['priority'] }}</div></div>
            <div class="summary-card"><h3>Scheduled</h3><div class="count">{{ stats['scheduled'] }}</div></div>
            <div class="summary-card"><h3>Processing</h3><div class="count">{{ stats['processing'] }}</div></div>
            <div class="summary-card"><h3>Completed</h3><div class="count">{{ stats['completed'] }}</div></div>
            <div class="summary-card"><h3>Dead</h3><div class="count">{{ stats['dead'] }}</div></div>
            <div class="summary-card"><h3>Active Workers</h3><div class="count">{{ stats['workers'] }}</div></div>
          </div>

          {% for queue_name, jobs in job_lists.items() %}
            <h2>{{ queue_name | capitalize }} Jobs</h2>
            {% if jobs %}
              <table>
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Command</th>
                    <th>Attempts</th>
                    <th>Priority</th>
                    <th>Updated At</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {% for job in jobs %}
                    <tr>
                      <td class="job-id">{{ job.id }}</td>
                      <td><pre>{{ job.command }}</pre></td>
                      <td>{{ job.attempts }}</td>
                      <td>{{ job.priority }}</td>
                      <td>{{ job.updated_at }}</td>
                      <td class="actions">
                        {% if queue_name == 'dead' %}
                          <a href="{{ url_for('retry_job', job_id=job.id) }}">Retry</a>
                        {% endif %}
                      </td>
                    </tr>
                    {% if job.output %}
                      <tr>
                        <td colspan="6">
                          <strong>Output:</strong>
                          <pre>{{ job.output }}</pre>
                        </td>
                      </tr>
                    {% endif %}
                  {% endfor %}
                </tbody>
              </table>
            {% else %}
              <p>No jobs in this queue.</p>
            {% endif %}
          {% endfor %}
        </div>
      </body>
    </html>
    """

    @app.route('/')
    def dashboard():
        r = app.config['r']
        stats = {
            'priority': r.zcard('queue:priority'),
            'scheduled': r.zcard('queue:scheduled'),
            'processing': r.llen('queue:processing'),
            'completed': r.llen('queue:completed'),
            'dead': r.llen('queue:dead'),
            'workers': r.scard('workers:active'),
        }
        
        job_lists = {
            'pending': [get_job(r, jid) for jid in r.zrange('queue:priority', 0, 10)],
            'completed': [get_job(r, jid) for jid in r.lrange('queue:completed', 0, 10)],
            'dead': [get_job(r, jid) for jid in r.lrange('queue:dead', 0, 10)],
        }
        
        return render_template_string(HTML_TEMPLATE, stats=stats, job_lists=job_lists)

    @app.route('/retry/<job_id>')
    def retry_job(job_id):
        r = app.config['r']
        if r.lrem("queue:dead", 1, job_id):
            pipe = r.pipeline()
            pipe.hset(f"job:{job_id}", "state", "pending")
            pipe.hset(f"job:{job_id}", "attempts", 0)
            pipe.delete(f"job:{job_id}:output")
            
            job_data = r.hgetall(f"job:{job_id}")
            priority = int(job_data.get("priority", 0))
            pipe.zadd("queue:priority", {job_id: priority})
            pipe.execute()
        return redirect(url_for('dashboard'))

# --- CLI Commands ---
def handle_enqueue(args):
    """Handles the 'enqueue' command."""
    r = get_redis_connection()
    try:
        job_spec = json.loads(args.job_spec)
        if "command" not in job_spec:
            raise ValueError("Job specification must include a 'command'.")
        
        if "max_retries" not in job_spec:
            job_spec["max_retries"] = r.get("config:max_retries") or 3
        
        if "timeout" not in job_spec:
            job_spec["timeout"] = r.get("config:timeout") or 0
        
        if "priority" not in job_spec:
            job_spec["priority"] = r.get("config:priority") or 0

        job = create_job(job_spec)
        enqueue_job(r, job)
    except (json.JSONDecodeError, ValueError) as e:
        print(f"Error: Invalid job specification. {e}", file=sys.stderr)
        sys.exit(1)

def handle_worker_start(args):
    """Handles the 'worker start' command."""
    if args.count > 1:
        print(f"Starting {args.count} workers...")
        for i in range(args.count):
            subprocess.Popen([sys.executable, __file__, "worker", "start", "--count", "1"])
        print(f"Started {args.count} workers in the background.")
    else:
        r = get_redis_connection()
        worker = Worker(r, 1)
        worker.start()

def handle_worker_stop(args):
    """Handles the 'worker stop' command."""
    r = get_redis_connection()
    active_workers = r.smembers("workers:active")
    if not active_workers:
        print("No active workers to stop.")
        return
    
    print(f"Signaling {len(active_workers)} workers to shut down gracefully...")
    r.set("worker:shutdown_signal", "1")

    timeout = 10
    start_time = time.time()
    while r.smembers("workers:active") and (time.time() - start_time < timeout):
        time.sleep(0.5)
    
    remaining_workers = r.smembers("workers:active")
    if remaining_workers:
        print(f"Warning: {len(remaining_workers)} workers did not shut down gracefully within {timeout} seconds.")
        for pid in remaining_workers:
            try:
                os.kill(int(pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
            r.srem("workers:active", pid)
    
    r.delete("worker:shutdown_signal")
    print("All workers stopped or forcefully terminated. Shutdown signal cleared.")

def handle_status(args):
    """Handles the 'status' command."""
    r = get_redis_connection()
    print("--- Job Status ---")
    print(f"Priority Queue: {r.zcard('queue:priority')}")
    print(f"Scheduled: {r.zcard('queue:scheduled')}")
    print(f"Processing: {r.llen('queue:processing')}")
    print(f"Completed:  {r.llen('queue:completed')}")
    print(f"Failed:     {r.llen('queue:failed')}")
    print(f"Dead:       {r.llen('queue:dead')}")
    print("\n--- Worker Status ---")
    active_workers = r.smembers("workers:active")
    print(f"Active Workers: {len(active_workers)}")
    for pid in active_workers:
        print(f"  - PID: {pid}")

def handle_list(args):
    """Handles the 'list' command."""
    r = get_redis_connection()
    state = args.state
    
    job_ids = []
    if state == "scheduled":
        job_ids = r.zrange("queue:scheduled", 0, -1, withscores=True)
    elif state == "pending":
        job_ids = r.zrange("queue:priority", 0, -1, withscores=True)
    else:
        queue_name = f"queue:{state}"
        job_ids = r.lrange(queue_name, 0, -1)

    if not job_ids:
        print("[]") # Output an empty JSON array
        return

    jobs = []
    if state in ["scheduled", "pending"]:
        for job_id, score in job_ids:
            job = get_job(r, job_id)
            if job:
                if state == "scheduled":
                    job['scheduled_at'] = datetime.fromtimestamp(score).isoformat()
                else:
                    job['priority'] = int(score)
                jobs.append(job)
    else:
        for job_id in job_ids:
            job = get_job(r, job_id)
            if job:
                jobs.append(job)
    
    print(json.dumps(jobs, indent=2))


def handle_dlq_list(args):
    """Handles the 'dlq list' command."""
    args.state = "dead"
    handle_list(args)

def handle_dlq_retry(args):
    """Handles the 'dlq retry' command."""
    r = get_redis_connection()
    job_id = args.job_id
    
    if not r.lrem("queue:dead", 1, job_id):
        print(f"Job {job_id} not found in the DLQ.")
        return

    pipe = r.pipeline()
    pipe.hset(f"job:{job_id}", "state", "pending")
    pipe.hset(f"job:{job_id}", "attempts", 0)
    pipe.delete(f"job:{job_id}:output")
    
    job_data = r.hgetall(f"job:{job_id}")
    priority = int(job_data.get("priority", 0))
    pipe.zadd("queue:priority", {job_id: priority})
    pipe.execute()

    print(f"Retrying job {job_id}.")

def handle_config_set(args):
    """Handles the 'config set' command."""
    r = get_redis_connection()
    r.set(f"config:{args.key}", args.value)
    print(f"Configuration '{args.key}' set to '{args.value}'.")

def handle_config_get(args):
    """Handles the 'config get' command."""
    r = get_redis_connection()
    value = r.get(f"config:{args.key}")
    if value:
        print(f"{args.key}: {value}")
    else:
        print(f"Configuration '{args.key}' not set.")

def handle_webui(args):
    """Handles the 'webui' command."""
    if Flask is None:
        print("Error: Flask is not installed. Please run 'pip install Flask' to use the web UI.", file=sys.stderr)
        sys.exit(1)
    app.run(host=args.host, port=args.port, debug=True)

def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(description="A simple CLI-based background job queue system.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Enqueue command
    enqueue_parser = subparsers.add_parser("enqueue", help="Add a new job to the queue from a JSON string.")
    enqueue_parser.add_argument("job_spec", help='The job specification as a JSON string.')
    enqueue_parser.set_defaults(func=handle_enqueue)

    # Worker command
    worker_parser = subparsers.add_parser("worker", help="Manage workers.")
    worker_subparsers = worker_parser.add_subparsers(dest="subcommand", required=True)
    worker_start_parser = worker_subparsers.add_parser("start", help="Start one or more workers.")
    worker_start_parser.add_argument("--count", type=int, default=1, help="Number of workers to start.")
    worker_start_parser.set_defaults(func=handle_worker_start)
    worker_stop_parser = worker_subparsers.add_parser("stop", help="Stop running workers gracefully.")
    worker_stop_parser.set_defaults(func=handle_worker_stop)

    # Status command
    status_parser = subparsers.add_parser("status", help="Show summary of all job states & active workers.")
    status_parser.set_defaults(func=handle_status)

    # List command
    list_parser = subparsers.add_parser("list", help="List jobs by state.")
    list_parser.add_argument("--state", choices=["pending", "scheduled", "processing", "completed", "failed", "dead"], default="pending", help="The state of the jobs to list.")
    list_parser.set_defaults(func=handle_list)

    # DLQ command
    dlq_parser = subparsers.add_parser("dlq", help="Manage the Dead Letter Queue.")
    dlq_subparsers = dlq_parser.add_subparsers(dest="subcommand", required=True)
    dlq_list_parser = dlq_subparsers.add_parser("list", help="List jobs in the DLQ.")
    dlq_list_parser.set_defaults(func=handle_dlq_list)
    dlq_retry_parser = dlq_subparsers.add_parser("retry", help="Retry a job from the DLQ.")
    dlq_retry_parser.add_argument("job_id", help="The ID of the job to retry.")
    dlq_retry_parser.set_defaults(func=handle_dlq_retry)

    # Config command
    config_parser = subparsers.add_parser("config", help="Manage configuration.")
    config_subparsers = config_parser.add_subparsers(dest="subcommand", required=True)
    config_set_parser = config_subparsers.add_parser("set", help="Set a configuration value.")
    config_set_parser.add_argument("key", help="The configuration key.")
    config_set_parser.add_argument("value", help="The configuration value.")
    config_set_parser.set_defaults(func=handle_config_set)
    config_get_parser = config_subparsers.add_parser("get", help="Get a configuration value.")
    config_get_parser.add_argument("key", help="The configuration key.")
    config_get_parser.set_defaults(func=handle_config_get)

    # Web UI command
    if Flask:
        webui_parser = subparsers.add_parser("webui", help="Launch the web UI dashboard.")
        webui_parser.add_argument("--host", default="127.0.0.1", help="Host to bind the web server to.")
        webui_parser.add_argument("--port", type=int, default=5000, help="Port to run the web server on.")
        webui_parser.set_defaults(func=handle_webui)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
