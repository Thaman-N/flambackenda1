# QueueCTL

QueueCTL is a minimal, production-grade background job queue system built in Python. It is designed to be run from the command line and uses Redis for persistent job storage and state management.

This system manages background jobs with worker processes, handles retries using exponential backoff, and maintains a Dead Letter Queue (DLQ) for permanently failed jobs.

## Features

-   **Persistent Job Queue:** Job data persists across restarts using Redis.
-   **Multiple Worker Support:** Process jobs in parallel with multiple worker processes.
-   **Automatic Retries:** Failed jobs are automatically retried with exponential backoff.
-   **Dead Letter Queue (DLQ):** Jobs that exhaust their retries are moved to a DLQ for manual inspection and retry.
-   **CLI Interface:** All functionality is accessible through a clean and simple command-line interface.
-   **Dynamic Configuration:** Configure settings like max retries on the fly.

## Bonus Features Implemented

-   **Job Timeouts:** Jobs can be configured with a timeout to prevent them from running indefinitely.
-   **Priority Queues:** Jobs can be assigned a priority to be processed before lower-priority jobs.
-   **Job Output Logging:** The `stdout` and `stderr` of every job are captured and stored.
-   **Web Dashboard:** A minimal web UI is included for real-time monitoring.

## Architecture Overview

The system is composed of a single Python script (`queuectl.py`) that acts as the CLI, worker, and web UI entry point. Redis serves as the backbone for all state management.

### Job Lifecycle

1.  **Enqueue:** A new job is created with a `pending` state and added to a `queue:priority` sorted set in Redis, with its priority as the score.
2.  **Processing:** A worker atomically pops the highest-priority job (lowest score) from the priority queue and moves it to a `queue:processing` list.
3.  **Execution:** The worker executes the job's command using `subprocess`.
    -   **On Success:** The job state is set to `completed`, and it is moved to the `queue:completed` list.
    -   **On Failure (or Timeout):** The job's `attempts` count is incremented.
        -   If `attempts` < `max_retries`, the job is added to the `queue:scheduled` sorted set with a future timestamp for its next attempt.
        -   If `attempts` >= `max_retries`, the job state is set to `dead`, and it is moved to the `queue:dead` list (DLQ).

### Data Persistence

-   **Job Data:** Each job is stored as a Redis Hash with a key like `job:<uuid>`.
-   **Queues:** Redis Sorted Sets are used for `queue:priority` and `queue:scheduled`. Redis Lists are used for `queue:processing`, `queue:completed`, and `queue:dead`.
-   **Workers:** A Redis Set (`workers:active`) stores the PIDs of all active worker processes.
-   **Configuration:** System settings are stored in simple Redis keys (e.g., `config:max_retries`).

## Setup Instructions

### Prerequisites

-   Python 3.7+
-   Redis (can be run locally or via Docker)
-   `redis-py` and `Flask` Python libraries

### Installation

1.  **Clone the repository (or download the project files).**

2.  **Install the required Python libraries:**
    ```sh
    pip install redis Flask
    ```

3.  **Start Redis:**
    If you have Docker installed, you can easily start a Redis instance:
    ```sh
    docker run -d --name fervent_wilson -p 6379:6379 redis:latest
    ```
    Otherwise, ensure your local Redis server is running.

4.  **Making `queuectl` a Command (Optional but Recommended)**

    To use `queuectl` as a regular command, you need to make the `queuectl.py` script executable and accessible from your system's PATH.

    **On Windows:**
    A batch file `queuectl.bat` is provided which acts as an alias. To use it, add the project directory to your PATH.
    ```cmd
    set PATH=%PATH%;C:\path\to\flamassignment
    ```
    (Replace `C:\path\to\flamassignment` with the actual path to the project directory).

    **On Linux/macOS:**
    You can create a symbolic link to `queuectl.py` in a directory that is in your PATH, for example `/usr/local/bin`.
    ```sh
    # Make the script executable
    chmod +x queuectl.py
    # Create a symbolic link
    sudo ln -s /path/to/flamassignment/queuectl.py /usr/local/bin/queuectl
    ```
    (Replace `/path/to/flamassignment` with the actual path to the project directory).

## Command Reference

All commands are run through the `queuectl` command.

### `enqueue`
Adds a new job to the queue.

**Usage:**
```sh
queuectl enqueue <json_spec>
```

**Argument:**
-   `<json_spec>`: A single JSON string that defines the job.

**JSON Job Specification Fields:**
-   `command` (required): The shell command to execute.
-   `id` (optional): A unique ID for the job. If not provided, a UUID will be generated.
-   `priority` (optional): An integer representing job priority. Lower numbers are higher priority. Defaults to `0`.
-   `max_retries` (optional): The number of times to retry a failed job. Defaults to the value of `config:max_retries` or `3`.
-   `timeout` (optional): The maximum number of seconds the job can run before being terminated. Defaults to `0` (no timeout).

**Example (Command Prompt - Windows):**
```cmd
# Enqueue a high-priority job that times out
queuectl enqueue "{\"id\":\"job1\", \"command\":\"timeout /t 10\", \"priority\": 1, \"timeout\": 5}"
```

**Example (Bash/Zsh/PowerShell):**
```bash
# Enqueue a high-priority job that times out
queuectl enqueue '{"id":"job1", "command":"sleep 10", "priority": 1, "timeout": 5}'
```

---


### `worker`
Manages worker processes.

**Subcommands:**
-   `start`: Starts one or more worker processes.
-   `stop`: Gracefully stops all running workers.

**Usage:**
```sh
# Start a single worker in the foreground
queuectl worker start

# Start 4 workers in the background
queuectl worker start --count 4

# Signal all workers to stop
queuectl worker stop
```

---


### `status`
Shows a summary of all job states and active workers.

**Usage:**
```sh
queuectl status
```

---


### `list`
Lists jobs and their details for a given state.

**Usage:**
```sh
queuectl list --state <state>
```

**Argument:**
-   `--state <state>`: The state of the jobs to list.
    -   **Choices:** `pending`, `scheduled`, `processing`, `completed`, `failed`, `dead`.
    -   **Default:** `pending`.

**Example:**
```sh
# List all jobs in the Dead Letter Queue
queuectl list --state dead
```

---


### `dlq`
Manages the Dead Letter Queue (DLQ).

**Subcommands:**
-   `list`: A shortcut for `queuectl list --state dead`.
-   `retry <job_id>`: Moves a specific job from the DLQ back to the pending queue for retry.

**Usage:**
```sh
# List all jobs in the DLQ
queuectl dlq list

# Retry a specific job from the DLQ
queuectl dlq retry <job-id>
```

---


### `config`
Manages system configuration.

**Subcommands:**
-   `set <key> <value>`: Sets a configuration value.
-   `get <key>`: Retrieves a configuration value.

**Supported Keys:**
-   `max-retries`: Default max retries for new jobs.
-   `backoff-base`: The base for exponential backoff delay calculation (`delay = base ^ attempts`).
-   `priority`: Default priority for new jobs.
-   `timeout`: Default timeout for new jobs.

**Usage:**
```sh
# Set the default max retries to 5
queuectl config set max-retries 5

# Get the current backoff-base value
queuectl config get backoff-base
```

---


### `webui`
Launches a minimal web dashboard for real-time monitoring.

**Usage:**
```sh
queuectl webui [--host <ip>] [--port <port>]
```

**Arguments:**
-   `--host <ip>` (optional): The host IP to bind to. Defaults to `127.0.0.1`.
-   `--port <port>` (optional): The port to run on. Defaults to `5000`.

**Example:**
```sh
# Run the web UI on all network interfaces on port 8080
queuectl webui --host 0.0.0.0 --port 8080
```

## Testing Instructions

A comprehensive test suite is provided in `test_queuectl.py`. This script uses Python's `unittest` framework to validate all core functionality.

To run the tests:

1.  Ensure your Redis server is running and accessible.
2.  The test suite will automatically flush the Redis database before starting.
3.  Run the test script from your terminal:

    ```sh
    python test_queuectl.py
    ```
The script will execute a series of tests and print the results to the console.