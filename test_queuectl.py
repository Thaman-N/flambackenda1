
import unittest
import subprocess
import redis
import time
import os
import json
import requests

class TestQueueCTL(unittest.TestCase):
    """
    A comprehensive test suite for the queuectl.py script.
    This suite tests all major functionalities including enqueuing, worker processing,
    retries with non-blocking backoff, DLQ, configuration, graceful shutdown,
    job output logging, job timeout handling, job priority queues, and the web UI.
    """
    
    @classmethod
    def setUpClass(cls):
        """Set up the test environment. This runs once before all tests."""
        cls.redis_conn = redis.Redis(decode_responses=True)
        try:
            containers = subprocess.check_output(["docker", "ps", "--format", "{{.Names}}"])
            redis_container = next((c for c in containers.decode().strip().split('\n') if 'redis' in c), None)
            if redis_container:
                cls.redis_container_name = redis_container
                print(f"Found Redis container: {cls.redis_container_name}")
            else:
                cls.redis_container_name = None
        except (subprocess.CalledProcessError, FileNotFoundError):
            cls.redis_container_name = None
            print("Docker not found or not running. Assuming local Redis.")

    def setUp(self):
        """Set up for each test. This runs before every single test."""
        if self.redis_container_name:
            subprocess.run(["docker", "exec", self.redis_container_name, "redis-cli", "flushall"], check=True)
        else:
            self.redis_conn.flushall()
        self.worker_process = None
        self.webui_process = None

    def tearDown(self):
        """Tear down after each test. This runs after every single test."""
        if self.worker_process and self.worker_process.poll() is None:
            try:
                subprocess.run(["queuectl", "worker", "stop"], timeout=15, shell=True)
                if self.worker_process.poll() is None:
                    self.worker_process.terminate()
                    self.worker_process.wait(timeout=5)
            except (subprocess.TimeoutExpired, ProcessLookupError):
                pass
        if self.webui_process and self.webui_process.poll() is None:
            self.webui_process.terminate()
            self.webui_process.wait(timeout=5)
        for log in ["worker.log", "worker.err.log"]:
            if os.path.exists(log):
                os.remove(log)

    def _run_command(self, command):
        """Helper function to run a queuectl command."""
        return subprocess.run(["queuectl"] + command, capture_output=True, text=True, shell=True)

    def _wait_for_redis_state(self, check_func, timeout=20, interval=0.5):
        """
        Polls Redis until a condition is met or a timeout occurs.
        check_func: A callable that takes the redis_conn and returns True if the condition is met.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            if check_func(self.redis_conn):
                return True
            time.sleep(interval)
        return False

    def test_01_config_set_and_get(self):
        """Test setting and getting configuration values."""
        print("Running test_01_config_set_and_get...")
        result = self._run_command(["config", "set", "max-retries", "5"])
        self.assertEqual(result.returncode, 0)
        self.assertEqual(self.redis_conn.get("config:max-retries"), "5")

    def test_02_enqueue_job(self):
        """Test enqueuing a new job using a JSON string."""
        print("Running test_02_enqueue_job...")
        job_spec = json.dumps({"id": "job-123", "command": "echo 'test'"})
        result = self._run_command(["enqueue", job_spec])
        self.assertEqual(result.returncode, 0)
        self.assertIn("Enqueued job job-123", result.stdout)
        
        pending_jobs = self.redis_conn.zrange("queue:priority", 0, -1)
        self.assertEqual(len(pending_jobs), 1)
        self.assertEqual(pending_jobs[0], "job-123")
        
        job_data = self.redis_conn.hgetall("job:job-123")
        self.assertEqual(job_data["command"], "echo 'test'")
        self.assertEqual(job_data["state"], "pending")

    def test_03_list_jobs(self):
        """Test listing jobs by state."""
        print("Running test_03_list_jobs...")
        job_spec = json.dumps({"command": "echo 'job1'"})
        self._run_command(["enqueue", job_spec])
        
        result = self._run_command(["list", "--state", "pending"])
        self.assertEqual(result.returncode, 0)
        self.assertIn('"command": "echo \'job1\'"', result.stdout)

    def test_04_worker_processes_successful_job_with_output(self):
        """Test a worker successfully processing a job and capturing output."""
        print("Running test_04_worker_processes_successful_job_with_output...")
        job_spec = json.dumps({"command": "echo 'success output'"})
        result = self._run_command(["enqueue", job_spec])
        job_id = result.stdout.strip().split()[2]
        
        self.worker_process = subprocess.Popen(["queuectl", "worker", "start"], shell=True)
        
        self.assertTrue(self._wait_for_redis_state(lambda r: r.llen("queue:completed") == 1), "Job should be completed")
        
        completed_jobs = self.redis_conn.lrange("queue:completed", 0, -1)
        self.assertEqual(len(completed_jobs), 1)
        self.assertEqual(completed_jobs[0], job_id)
        
        job_data = self.redis_conn.hgetall(f"job:{job_id}")
        self.assertEqual(job_data["state"], "completed")
        self.assertEqual(self.redis_conn.zcard("queue:priority"), 0)

        output = self.redis_conn.get(f"job:{job_id}:output")
        self.assertIsNotNone(output)
        self.assertIn("success output", output)

    def test_05_worker_handles_failing_job_and_dlq_non_blocking_with_output(self):
        """Test non-blocking retry and moving a job to the DLQ, capturing output."""
        print("Running test_05_worker_handles_failing_job_and_dlq_non_blocking_with_output...")
        self._run_command(["config", "set", "max-retries", "2"])
        self._run_command(["config", "set", "backoff-base", "1"])
        
        job_spec = json.dumps({"command": "non_existent_command"})
        result = self._run_command(["enqueue", job_spec])
        job_id = result.stdout.strip().split()[2]

        self.worker_process = subprocess.Popen(["queuectl", "worker", "start"], shell=True)
        
        self.assertTrue(self._wait_for_redis_state(lambda r: r.llen("queue:dead") == 1, timeout=30), "Job should be in DLQ after exhausting retries")
        
        self.assertEqual(self.redis_conn.zcard("queue:scheduled"), 0, "Scheduled queue should be empty after moving to DLQ")
        
        dead_job_id = self.redis_conn.lrange("queue:dead", 0, -1)[0]
        self.assertEqual(dead_job_id, job_id)
        job_data = self.redis_conn.hgetall(f"job:{job_id}")
        self.assertEqual(job_data["attempts"], "3")

        output = self.redis_conn.get(f"job:{job_id}:output")
        self.assertIsNotNone(output)
        self.assertIn("'non_existent_command' is not recognized", output)

    def test_06_dlq_list_and_retry_clears_output(self):
        """Test listing and retrying jobs from the DLQ, ensuring output is cleared."""
        print("Running test_06_dlq_list_and_retry_clears_output...")
        job_id = "test-dlq-job-id"
        job = {"id": job_id, "command": "echo 'retried'", "state": "dead", "attempts": "3", "max_retries": "3"}
        self.redis_conn.hset(f"job:{job_id}", mapping=job)
        self.redis_conn.set(f"job:{job_id}:output", "some old error output")
        self.redis_conn.lpush("queue:dead", job_id)
        
        result = self._run_command(["dlq", "list"])
        self.assertEqual(result.returncode, 0)
        self.assertIn(job_id, result.stdout)
        self.assertIn("some old error output", result.stdout)

        result = self._run_command(["dlq", "retry", job_id])
        self.assertEqual(result.returncode, 0)
        self.assertIn(f"Retrying job {job_id}", result.stdout)
        
        self.assertEqual(self.redis_conn.llen("queue:dead"), 0)
        self.assertEqual(self.redis_conn.zrange("queue:priority", 0, -1), [job_id])
        self.assertEqual(self.redis_conn.hget(f"job:{job_id}", "attempts"), "0")
        
        self.assertIsNone(self.redis_conn.get(f"job:{job_id}:output"))

    def test_08_job_timeout_handling(self):
        """Test that a job with a timeout is correctly terminated and handled."""
        print("Running test_08_job_timeout_handling...")
        
        job_spec = json.dumps({"command": "timeout /t 5", "timeout": 2, "max_retries": 0})
        result = self._run_command(["enqueue", job_spec])
        job_id = result.stdout.strip().split()[2]

        self.worker_process = subprocess.Popen(["queuectl", "worker", "start"], shell=True)
        
        self.assertTrue(self._wait_for_redis_state(lambda r: r.llen("queue:dead") == 1), "Job should be in DLQ due to timeout")
        
        dead_job_id = self.redis_conn.lrange("queue:dead", 0, -1)[0]
        self.assertEqual(dead_job_id, job_id)
        
        job_data = self.redis_conn.hgetall(f"job:{job_id}")
        self.assertEqual(job_data["state"], "dead")
        self.assertEqual(job_data["attempts"], "1")

        output = self.redis_conn.get(f"job:{job_id}:output")
        self.assertIsNotNone(output)
        self.assertIn("Job timed out after 2 seconds", output)

    def test_09_job_priority_queues(self):
        """Test that jobs are processed according to their priority."""
        print("Running test_09_job_priority_queues...")
        job_spec_low = json.dumps({"id": "job-low", "command": "echo 'low priority'", "priority": 10})
        job_spec_medium = json.dumps({"id": "job-medium", "command": "echo 'medium priority'", "priority": 5})
        job_spec_high = json.dumps({"id": "job-high", "command": "echo 'high priority'", "priority": 1})

        self._run_command(["enqueue", job_spec_low])
        self._run_command(["enqueue", job_spec_medium])
        self._run_command(["enqueue", job_spec_high])

        expected_order_ids = ["job-high", "job-medium", "job-low"]
        actual_order_ids = self.redis_conn.zrange("queue:priority", 0, -1)
        self.assertEqual(actual_order_ids, expected_order_ids, "Initial priority queue order is incorrect")

        self.worker_process = subprocess.Popen(["queuectl", "worker", "start"], shell=True)

        self.assertTrue(self._wait_for_redis_state(lambda r: r.llen("queue:completed") == 3), "All jobs should be completed")

        completed_jobs_ids = self.redis_conn.lrange("queue:completed", 0, -1)
        expected_completed_order = ["job-low", "job-medium", "job-high"]
        self.assertEqual(completed_jobs_ids, expected_completed_order, "Jobs should be processed by priority (high to low)")

    def test_10_webui_starts(self):
        """Test that the web UI starts without crashing."""
        print("Running test_10_webui_starts...")
        self.webui_process = subprocess.Popen(["queuectl", "webui", "--port", "5001"], shell=True)
        time.sleep(2) # Give the server time to start
        
        # Check if the process is still running
        self.assertIsNone(self.webui_process.poll(), "Web UI process should be running")
        
        # Try to make a request to the dashboard
        try:
            response = requests.get("http://127.0.0.1:5001")
            self.assertEqual(response.status_code, 200)
            self.assertIn("QueueCTL Dashboard", response.text)
        except requests.exceptions.ConnectionError as e:
            self.fail(f"Web UI is not accessible: {e}")

    def test_07_worker_graceful_shutdown(self):
        """Test the graceful shutdown of a worker."""
        print("Running test_07_worker_graceful_shutdown...")
        self.worker_process = subprocess.Popen(["queuectl", "worker", "start"], shell=True)
        time.sleep(2)
        
        active_workers = self.redis_conn.smembers("workers:active")
        self.assertEqual(len(active_workers), 1)
        
        self._run_command(["worker", "stop"])
        
        self.assertTrue(self._wait_for_redis_state(lambda r: r.scard("workers:active") == 0), "Worker should have shut down gracefully")
        
        try:
            self.worker_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.fail("Worker process did not terminate after graceful shutdown signal.")
            
        self.assertEqual(self.redis_conn.scard("workers:active"), 0)

if __name__ == '__main__':
    unittest.main()
