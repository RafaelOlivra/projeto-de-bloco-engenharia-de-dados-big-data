#!/bin/bash
set -e

# Start Spark Master in the background
echo "Starting Spark Master..."
# The start-master.sh script uses the configurations set in spark-defaults.conf
# and binds to the container's hostname.
${SPARK_HOME}/sbin/start-master.sh &

# Give Spark Master a moment to initialize
sleep 5

# Start Jupyter Lab in the foreground.
# This command will keep the container running.
echo "Starting Jupyter Lab..."
jupyter lab --port-retries=0 --ip 0.0.0.0 --allow-root --IdentityProvider.token="" --ServerApp.password=""
