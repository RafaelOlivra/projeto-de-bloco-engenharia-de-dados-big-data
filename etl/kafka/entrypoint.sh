#!/bin/bash

echo "Starting Kafka entrypoint script..."

# Verifica se a variável SCRIPT está definida
if [ -n "$SCRIPT" ]; then
  echo "Running custom script: $SCRIPT"
  exec python -u "$SCRIPT"
else
  echo "No SCRIPT environment variable set. Exiting."
  exit 1
fi