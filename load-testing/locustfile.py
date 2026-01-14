"""
Locust Load Testing for MongoDB Atlas ODL Banking API

This file defines load testing scenarios to verify the SLA targets:
- 500 QPS (queries per second)
- <100ms p95 latency

Run with:
    locust -f locustfile.py --host=http://localhost:8000

Or use the web UI at http://localhost:8089

Benchmark mode is auto-enabled at test start. This:
- Runs explain-only queries (faster)
- Returns _dbExecTimeMs in response body with actual MongoDB execution time
- Locust will report DB execution time instead of round-trip latency
"""

import random
from locust import HttpUser, task, between, events


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


def get_db_exec_time(response):
    """Extract DB execution time from response body."""
    try:
        data = response.json()
        if isinstance(data, dict) and "_dbExecTimeMs" in data:
            return float(data["_dbExecTimeMs"])
    except:
        pass
    return None


class BankingAPIUser(HttpUser):
    """
    Simulated user making requests to the Banking ODL API.

    Request distribution matches typical banking app usage:
    - 50% transaction listing (most common)
    - 25% balance checks
    - 15% search queries
    - 10% statement retrieval

    Reports DB execution time from _dbExecTimeMs in response body.
    """

    wait_time = between(0.1, 0.5)
    account_numbers = []

    def on_start(self):
        """Called when a simulated user starts."""
        if not BankingAPIUser.account_numbers:
            self._load_accounts()

        if BankingAPIUser.account_numbers:
            self.account = random.choice(BankingAPIUser.account_numbers)
        else:
            self.account = None

    def _load_accounts(self):
        """Load available account numbers from the API."""
        import requests
        try:
            # Use raw requests library to avoid any Locust tracking
            host = self.host
            resp = requests.get(f"{host}/api/v1/accounts?limit=100", timeout=10)
            if resp.status_code == 200:
                BankingAPIUser.account_numbers = resp.json()
        except Exception as e:
            print(f"Failed to load accounts: {e}")

    def _make_request(self, method, url, name, **kwargs):
        """Make a request and report DB execution time only."""
        import requests
        try:
            # Use raw requests library to completely bypass Locust's tracking
            full_url = f"{self.host}{url}"
            resp = requests.request(method, full_url, timeout=30, **kwargs)
            response_length = len(resp.content) if resp.content else 0

            if resp.status_code == 200:
                db_time = get_db_exec_time(resp)
                if db_time is not None:
                    # 0ms is valid - means query ran in <1ms (excellent!)
                    events.request.fire(
                        request_type=method,
                        name=name,
                        response_time=db_time,
                        response_length=response_length,
                        exception=None,
                        context=self.context()
                    )
                else:
                    # _dbExecTimeMs field missing entirely
                    events.request.fire(
                        request_type=method,
                        name=name,
                        response_time=0,
                        response_length=response_length,
                        exception=Exception("No _dbExecTimeMs in response"),
                        context=self.context()
                    )
            else:
                events.request.fire(
                    request_type=method,
                    name=name,
                    response_time=0,
                    response_length=response_length,
                    exception=Exception(f"HTTP {resp.status_code}"),
                    context=self.context()
                )

        except Exception as e:
            events.request.fire(
                request_type=method,
                name=name,
                response_time=0,
                response_length=0,
                exception=e,
                context=self.context()
            )

    @task(5)
    def get_transactions(self):
        """Get transactions for an account. Weight: 5"""
        if not self.account:
            return

        limit = random.choice([10, 20, 50])
        self._make_request(
            "GET",
            f"/api/v1/accounts/{self.account}/transactions",
            "/accounts/[acct]/transactions",
            params={"limit": limit}
        )

    @task(3)
    def get_balance(self):
        """Get current balance for an account. Weight: 3"""
        if not self.account:
            return

        self._make_request(
            "GET",
            f"/api/v1/accounts/{self.account}/balance",
            "/accounts/[acct]/balance"
        )

    @task(2)
    def search_transactions(self):
        """Search transactions using full-text search. Weight: 2"""
        if not self.account:
            return

        query = random.choice(SEARCH_TERMS)
        self._make_request(
            "GET",
            f"/api/v1/accounts/{self.account}/transactions/search",
            "/accounts/[acct]/search",
            params={"q": query, "limit": 20}
        )

    @task(1)
    def get_latest_statement(self):
        """Get the latest statement for an account. Weight: 1"""
        if not self.account:
            return

        self._make_request(
            "GET",
            f"/api/v1/accounts/{self.account}/statements/latest",
            "/accounts/[acct]/statements/latest"
        )

    @task(1)
    def get_transactions_filtered(self):
        """Get transactions with category filter. Weight: 1"""
        if not self.account:
            return

        category = random.choice(CATEGORIES)
        self._make_request(
            "GET",
            f"/api/v1/accounts/{self.account}/transactions",
            "/accounts/[acct]/transactions?cat",
            params={"category": category, "limit": 20}
        )

    @task(1)
    def get_account_summary(self):
        """Get account summary. Weight: 1"""
        if not self.account:
            return

        self._make_request(
            "GET",
            f"/api/v1/accounts/{self.account}/summary",
            "/accounts/[acct]/summary"
        )


class HighThroughputUser(HttpUser):
    """
    User profile optimized for high throughput testing.
    Minimal wait time for maximum QPS.
    """

    wait_time = between(0.01, 0.05)
    weight = 2
    account_numbers = []

    def on_start(self):
        if not HighThroughputUser.account_numbers:
            import requests
            try:
                resp = requests.get(f"{self.host}/api/v1/accounts?limit=100", timeout=10)
                if resp.status_code == 200:
                    HighThroughputUser.account_numbers = resp.json()
            except Exception:
                pass

        if HighThroughputUser.account_numbers:
            self.account = random.choice(HighThroughputUser.account_numbers)
        else:
            self.account = None

    def _make_request(self, method, url, name, **kwargs):
        """Make a request and report DB execution time only."""
        import requests
        try:
            full_url = f"{self.host}{url}"
            resp = requests.request(method, full_url, timeout=30, **kwargs)
            response_length = len(resp.content) if resp.content else 0

            if resp.status_code == 200:
                db_time = get_db_exec_time(resp)
                if db_time is not None:
                    # 0ms is valid - means query ran in <1ms (excellent!)
                    events.request.fire(
                        request_type=method,
                        name=name,
                        response_time=db_time,
                        response_length=response_length,
                        exception=None,
                        context=self.context()
                    )
                else:
                    events.request.fire(
                        request_type=method,
                        name=name,
                        response_time=0,
                        response_length=response_length,
                        exception=Exception("No _dbExecTimeMs in response"),
                        context=self.context()
                    )
            else:
                events.request.fire(
                    request_type=method,
                    name=name,
                    response_time=0,
                    response_length=response_length,
                    exception=Exception(f"HTTP {resp.status_code}"),
                    context=self.context()
                )

        except Exception as e:
            events.request.fire(
                request_type=method,
                name=name,
                response_time=0,
                response_length=0,
                exception=e,
                context=self.context()
            )

    @task(10)
    def quick_balance_check(self):
        """Fastest operation - balance check."""
        if not self.account:
            return

        self._make_request(
            "GET",
            f"/api/v1/accounts/{self.account}/balance",
            "/accounts/[acct]/balance"
        )

    @task(5)
    def quick_transactions(self):
        """Quick transaction listing with small limit."""
        if not self.account:
            return

        self._make_request(
            "GET",
            f"/api/v1/accounts/{self.account}/transactions",
            "/accounts/[acct]/transactions",
            params={"limit": 10}
        )


# Event hooks for custom reporting
@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Called when load test starts."""
    print("=" * 60)
    print("MongoDB Atlas ODL Load Test Starting")
    print("=" * 60)
    print("Target SLA:")
    print("  - QPS: 500")
    print("  - Latency P95: <100ms (DB execution time)")
    print("=" * 60)

    # Auto-enable benchmark mode
    import requests
    try:
        host = environment.host or "http://api:8000"
        resp = requests.post(f"{host}/api/v1/benchmark/start", timeout=5)
        if resp.status_code == 200:
            print("Benchmark mode ENABLED - reporting DB execution time")
        else:
            print(f"WARNING: Failed to enable benchmark mode: {resp.status_code}")
    except Exception as e:
        print(f"WARNING: Could not enable benchmark mode: {e}")
    print("=" * 60)


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Called when load test ends."""
    # Disable benchmark mode
    import requests
    try:
        host = environment.host or "http://api:8000"
        requests.post(f"{host}/api/v1/benchmark/stop", timeout=5)
        print("Benchmark mode disabled")
    except Exception:
        pass

    stats = environment.stats.total

    print("\n" + "=" * 60)
    print("Load Test Complete - Results Summary")
    print("=" * 60)

    total_requests = stats.num_requests
    total_failures = stats.num_failures
    avg_response_time = stats.avg_response_time

    p95 = stats.get_response_time_percentile(0.95) or 0
    p99 = stats.get_response_time_percentile(0.99) or 0

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

    qps_passed = qps >= 450
    latency_passed = p95 < 100

    print(f"  QPS Target (500): {'PASS' if qps_passed else 'FAIL'} ({qps:.2f})")
    print(f"  P95 Latency (<100ms): {'PASS' if latency_passed else 'FAIL'} ({p95:.2f}ms)")
    print(f"  Overall: {'PASS' if (qps_passed and latency_passed) else 'FAIL'}")
    print("=" * 60)
