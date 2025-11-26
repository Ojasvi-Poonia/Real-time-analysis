#!/usr/bin/env bash
# ==============================================================================
# CASSANDRA DEMO STARTUP SCRIPT
# ==============================================================================
# Starts both the transaction producer and the Streamlit dashboard
# ==============================================================================

set -e

echo "=============================================="
echo "  🗄️ CASSANDRA REAL-TIME ANALYTICS DEMO"
echo "=============================================="

cd /app || exit 1

# Wait for Cassandra to be fully ready
echo ""
echo "⏳ Waiting for Cassandra to initialize..."
sleep 15

# Start the transaction producer in background
if [ -f ./backend/demo.py ]; then
    echo ""
    echo "🚀 Starting transaction stream producer..."
    python ./backend/demo.py &
    PRODUCER_PID=$!
    echo "   Producer started (PID: $PRODUCER_PID)"
else
    echo "❌ ERROR: backend/demo.py not found!"
    exit 1
fi

# Wait for schema to be created
sleep 5

# Start Streamlit dashboard in foreground
if [ -f ./backend/dashboard.py ]; then
    echo ""
    echo "🖥️  Starting Streamlit dashboard on port 8501..."
    echo "   Open http://localhost:8501 in your browser"
    echo ""
    python -m streamlit run ./backend/dashboard.py \
        --server.port=8501 \
        --server.address=0.0.0.0 \
        --server.headless=true \
        --browser.gatherUsageStats=false
else
    echo "❌ ERROR: backend/dashboard.py not found!"
    tail -f /dev/null
fi
