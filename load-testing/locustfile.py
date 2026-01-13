"""
Locust Load Testing for MongoDB Atlas ODL Banking API

This file defines load testing scenarios to verify the SLA targets:
- 500 QPS (queries per second)
- <100ms p95 latency

Run with:
    locust -f locustfile.py --host=http://localhost:8000

Or use the web UI at http://localhost:8089
"""

import random
import os
from locust import HttpUser, task, between, events
from locust.runners import MasterRunner


# Test data
SEARCH_TERMS = [
    "fairprice", "grab", "netflix", "singtel", "starhub",
    "dbs", "ocbc", "uob", "ntuc", "cold storage",
    "mcdonalds", "kfc", "starbucks", "spotify"
]

CATEGORIES = [
    "groceries", "food_delivery", "transport", "utilities",
    "entertainment", "shopping", "dining", "healthcare"
]


class BankingAPIUser(HttpUser):
    """
    Simulated user making requests to the Banking ODL API.

    Request distribution matches typical banking app usage:
    - 50% transaction listing (most common)
    - 25% balance checks
    - 15% search queries
    - 10% statement retrieval
    """

    # Wait time between requests (simulates think time)
    wait_time = between(0.1, 0.5)

    # Account numbers loaded on start
    account_numbers = []

    def on_start(self):
        """Called when a simulated user starts."""
        # Fetch available accounts if not already loaded
        if not BankingAPIUser.account_numbers:
            self._load_accounts()

        # Select a random account for this user
        if BankingAPIUser.account_numbers:
            self.account = random.choice(BankingAPIUser.account_numbers)
        else:
            self.account = None

    def _load_accounts(self):
        """Load available account numbers from the API."""
        try:
            response = self.client.get("/api/v1/accounts?limit=100")
            if response.status_code == 200:
                BankingAPIUser.account_numbers = response.json()
        except Exception as e:
            print(f"Failed to load accounts: {e}")

    @task(5)
    def get_transactions(self):
        """
        Get transactions for an account.
        Weight: 5 (most common operation)
        """
        if not self.account:
            return

        # Vary the query parameters
        limit = random.choice([10, 20, 50])

        with self.client.get(
            f"/api/v1/accounts/{self.account}/transactions",
            params={"limit": limit},
            name="/api/v1/accounts/[account]/transactions",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Got status {response.status_code}")

    @task(3)
    def get_balance(self):
        """
        Get current balance for an account.
        Weight: 3 (second most common)
        """
        if not self.account:
            return

        with self.client.get(
            f"/api/v1/accounts/{self.account}/balance",
            name="/api/v1/accounts/[account]/balance",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Got status {response.status_code}")

    @task(2)
    def search_transactions(self):
        """
        Search transactions using full-text search.
        Weight: 2
        """
        if not self.account:
            return

        query = random.choice(SEARCH_TERMS)

        with self.client.get(
            f"/api/v1/accounts/{self.account}/transactions/search",
            params={"q": query, "limit": 20},
            name="/api/v1/accounts/[account]/transactions/search",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Got status {response.status_code}")

    @task(1)
    def get_latest_statement(self):
        """
        Get the latest statement for an account.
        Weight: 1
        """
        if not self.account:
            return

        with self.client.get(
            f"/api/v1/accounts/{self.account}/statements/latest",
            name="/api/v1/accounts/[account]/statements/latest",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Got status {response.status_code}")

    @task(1)
    def get_transactions_filtered(self):
        """
        Get transactions with category filter.
        Weight: 1
        """
        if not self.account:
            return

        category = random.choice(CATEGORIES)

        with self.client.get(
            f"/api/v1/accounts/{self.account}/transactions",
            params={"category": category, "limit": 20},
            name="/api/v1/accounts/[account]/transactions?category=[cat]",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Got status {response.status_code}")

    @task(1)
    def get_account_summary(self):
        """
        Get account summary.
        Weight: 1
        """
        if not self.account:
            return

        with self.client.get(
            f"/api/v1/accounts/{self.account}/summary",
            name="/api/v1/accounts/[account]/summary",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Got status {response.status_code}")


class HighThroughputUser(HttpUser):
    """
    User profile optimized for high throughput testing.
    Minimal wait time for maximum QPS.
    """

    wait_time = between(0.01, 0.05)  # Very short wait
    weight = 2  # Higher weight for load testing scenarios

    account_numbers = []

    def on_start(self):
        if not HighThroughputUser.account_numbers:
            try:
                response = self.client.get("/api/v1/accounts?limit=100")
                if response.status_code == 200:
                    HighThroughputUser.account_numbers = response.json()
            except Exception:
                pass

        if HighThroughputUser.account_numbers:
            self.account = random.choice(HighThroughputUser.account_numbers)
        else:
            self.account = None

    @task(10)
    def quick_balance_check(self):
        """Fastest operation - balance check."""
        if not self.account:
            return

        with self.client.get(
            f"/api/v1/accounts/{self.account}/balance",
            name="/api/v1/accounts/[account]/balance",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Got status {response.status_code}")

    @task(5)
    def quick_transactions(self):
        """Quick transaction listing with small limit."""
        if not self.account:
            return

        with self.client.get(
            f"/api/v1/accounts/{self.account}/transactions",
            params={"limit": 10},
            name="/api/v1/accounts/[account]/transactions",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Got status {response.status_code}")


# Event hooks for custom reporting
@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Called when load test starts."""
    print("=" * 60)
    print("MongoDB Atlas ODL Load Test Starting")
    print("=" * 60)
    print("Target SLA:")
    print("  - QPS: 500")
    print("  - Latency P95: <100ms")
    print("=" * 60)


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Called when load test ends."""
    stats = environment.stats.total

    print("\n" + "=" * 60)
    print("Load Test Complete - Results Summary")
    print("=" * 60)

    # Calculate metrics
    total_requests = stats.num_requests
    total_failures = stats.num_failures
    avg_response_time = stats.avg_response_time

    # Get percentiles
    response_times = stats.response_times
    p95 = stats.get_response_time_percentile(0.95) or 0
    p99 = stats.get_response_time_percentile(0.99) or 0

    # Calculate QPS
    if stats.last_request_timestamp and stats.start_time:
        duration = stats.last_request_timestamp - stats.start_time
        qps = total_requests / duration if duration > 0 else 0
    else:
        qps = 0

    print(f"Total Requests: {total_requests}")
    print(f"Total Failures: {total_failures}")
    print(f"Failure Rate: {(total_failures/total_requests*100):.2f}%" if total_requests > 0 else "N/A")
    print(f"Average Response Time: {avg_response_time:.2f}ms")
    print(f"P95 Response Time: {p95:.2f}ms")
    print(f"P99 Response Time: {p99:.2f}ms")
    print(f"Actual QPS: {qps:.2f}")

    print("\n" + "-" * 60)
    print("SLA Verification:")

    qps_passed = qps >= 450  # Allow 10% tolerance
    latency_passed = p95 < 100

    print(f"  QPS Target (500): {'PASS' if qps_passed else 'FAIL'} ({qps:.2f})")
    print(f"  P95 Latency (<100ms): {'PASS' if latency_passed else 'FAIL'} ({p95:.2f}ms)")
    print(f"  Overall: {'PASS' if (qps_passed and latency_passed) else 'FAIL'}")
    print("=" * 60)
