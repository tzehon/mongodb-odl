#!/bin/bash
#
# Run load test for MongoDB Atlas ODL Banking API
#
# Usage:
#   ./run_load_test.sh [options]
#
# Options:
#   -u, --users        Number of concurrent users (default: 100)
#   -r, --spawn-rate   Users to spawn per second (default: 10)
#   -t, --run-time     Test duration (default: 60s)
#   -h, --host         Target host (default: http://localhost:8000)
#   --headless         Run without web UI
#   --html             Generate HTML report

set -e

# Default values
USERS=100
SPAWN_RATE=10
RUN_TIME="60s"
HOST="${LOCUST_HOST:-http://localhost:8000}"
HEADLESS=false
HTML_REPORT=false
REPORT_FILE="locust_report_$(date +%Y%m%d_%H%M%S).html"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -u|--users)
            USERS="$2"
            shift 2
            ;;
        -r|--spawn-rate)
            SPAWN_RATE="$2"
            shift 2
            ;;
        -t|--run-time)
            RUN_TIME="$2"
            shift 2
            ;;
        -h|--host)
            HOST="$2"
            shift 2
            ;;
        --headless)
            HEADLESS=true
            shift
            ;;
        --html)
            HTML_REPORT=true
            shift
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  -u, --users        Number of concurrent users (default: 100)"
            echo "  -r, --spawn-rate   Users to spawn per second (default: 10)"
            echo "  -t, --run-time     Test duration (default: 60s)"
            echo "  -h, --host         Target host (default: http://localhost:8000)"
            echo "  --headless         Run without web UI"
            echo "  --html             Generate HTML report"
            echo ""
            echo "Example:"
            echo "  $0 -u 50 -r 5 -t 120s --headless --html"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "=================================================="
echo "MongoDB Atlas ODL Load Test"
echo "=================================================="
echo "Target Host: $HOST"
echo "Users: $USERS"
echo "Spawn Rate: $SPAWN_RATE users/second"
echo "Duration: $RUN_TIME"
echo "=================================================="

# Build command
CMD="locust -f locustfile.py --host=$HOST"

if [ "$HEADLESS" = true ]; then
    CMD="$CMD --headless -u $USERS -r $SPAWN_RATE -t $RUN_TIME"

    if [ "$HTML_REPORT" = true ]; then
        CMD="$CMD --html $REPORT_FILE"
    fi
else
    CMD="$CMD --web-host 0.0.0.0"
    echo ""
    echo "Starting Locust web UI at http://localhost:8089"
    echo "Configure and start the test from the web interface."
    echo ""
fi

# Run Locust
echo "Running: $CMD"
echo ""

eval $CMD

if [ "$HTML_REPORT" = true ] && [ -f "$REPORT_FILE" ]; then
    echo ""
    echo "HTML report generated: $REPORT_FILE"
fi
