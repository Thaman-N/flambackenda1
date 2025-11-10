# QueueCTL Design Document

This document outlines the design and architecture of the `queuectl` system.

## 1. Core Technology Choices

### 1.1. Language: Python

Python was chosen as it is a versatile, high-level language with a rich ecosystem of libraries, making it ideal for rapid development of a CLI tool and web server. Its standard libraries for `argparse`, `subprocess`, and `json` cover most of the core requirements out of the box.

### 1.2. Data Store: Redis

Redis was selected as the backend for persistence and state management for several key reasons:

-   **In-Memory Performance:** Redis is extremely fast, which is crucial for a job queue system where workers need to enqueue and dequeue jobs with minimal latency.
-   **Atomic Operations:** Redis provides atomic commands that are essential for building a robust, concurrent system. This was the single most important factor in this choice.
    -   `ZPOPMIN` is used to atomically fetch the highest-priority job and remove it from the queue, preventing race conditions where multiple workers could grab the same job.
    -   `HINCRBY` is used to atomically increment a job's `attempts` counter.
-   **Versatile Data Structures:** Redis is not just a key-value store. We leveraged its advanced data structures to model the system efficiently:
    -   **Hashes:** Used to store the metadata for each job (`job:<id>`). This is a natural fit for storing object-like data.
    -   **Sorted Sets:** Used for the `queue:priority` (pending jobs) and `queue:scheduled` (failed jobs waiting for retry) queues. The "score" member of the sorted set is a perfect fit for storing priority and scheduled retry times, allowing for efficient, ordered retrieval.
    -   **Lists:** Used for the `queue:completed` and `queue:dead` queues, where simple, time-ordered insertion (`LPUSH`) is sufficient. A list is also used for `queue:processing` to provide a simple, non-blocking way to track in-flight jobs.
    -   **Sets:** Used for `workers:active` to store the unique PIDs of running workers.

## 2. System Architecture

### 2.1. Job Lifecycle and State Management

The job lifecycle (`pending` -> `processing` -> `completed`/`failed` -> `scheduled` -> `pending` -> `dead`) is managed entirely through Redis operations.

-   **Enqueueing:** A new job is added to the `queue:priority` sorted set. This ensures that when a worker is ready, it will always pick the job with the highest priority (lowest score).
-   **Processing:** A worker uses `ZPOPMIN` to get the next job. This is a critical design choice for ensuring correctness and preventing duplicate processing. The job is then added to a `processing` list for monitoring purposes.
-   **Retries & Backoff:** A failed job is not immediately re-enqueued. Instead, it is added to the `queue:scheduled` sorted set. The score of the item is the Unix timestamp of its next scheduled run, calculated using exponential backoff. This is a **non-blocking** approach. A sleeping worker is an inefficient worker; this design allows the worker to remain active and process other available jobs while waiting for the retry delay to expire. A `move_scheduled_jobs` function within the worker's main loop periodically checks this queue and moves due jobs back to the `queue:priority` queue.

### 2.2. Worker Management

-   **Concurrency:** Multiple workers are supported by running separate `queuectl.py worker` processes using `subprocess.Popen`. This provides true parallelism and process isolation, which is more robust than threading for this use case (and avoids Python's GIL contentions for CPU-bound sub-tasks).
-   **Graceful Shutdown:** A "poison pill" mechanism is used for shutdown. The `worker stop` command sets a `worker:shutdown_signal` key in Redis. Each worker checks for this key in its main loop. If found, it finishes its current job and exits cleanly, removing its PID from the `workers:active` set. This is more reliable and cross-platform compatible than relying on OS-level signals (`SIGTERM`), which can be problematic on Windows.

### 2.3. Web UI

A minimal web dashboard was implemented using **Flask**.

-   **Simplicity:** Flask was chosen for its lightweight nature. To adhere to the "single file" spirit of the assignment, the entire Flask application, including the HTML and CSS, is embedded within the `queuectl.py` script.
-   **Real-time View:** The dashboard provides a near real-time view by automatically refreshing every 5 seconds using a `<meta>` tag. It fetches data directly from Redis on each request to provide an up-to-the-minute summary of the system's state.
-   **Interactivity:** A "Retry" link is provided for jobs in the DLQ, demonstrating how a web UI can trigger actions in the queue system.

## 3. Trade-offs and Simplifications

-   **Single File Implementation:** While the code is organized into functions and classes, a real-world application would be split into multiple modules (e.g., `cli.py`, `worker.py`, `web.py`, `datastore.py`). The single-file approach was a constraint of the assignment.
-   **Blocking Worker on Empty Queue:** When no jobs are available, the worker sleeps for 1 second (`time.sleep(1)`). A more advanced implementation would use Redis's blocking pop commands on a list (like `BRPOP`) to wait for jobs without consuming CPU. However, since we are using a sorted set for the priority queue, a blocking pop is not directly applicable. `BZPOPMIN` is a blocking variant, but using a short sleep is a simple and effective alternative for this project's scale.
-   **Error Handling:** Error handling for Redis connections and subprocess execution is present but could be made more robust with more specific exception handling and logging.
