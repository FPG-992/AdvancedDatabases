set -euo pipefail

#is docker running?
if ! docker info > /dev/null 2>&1; then
  echo "Docker is not running. Starting Docker Desktop..."
  open -a Docker
  echo "Waiting for Docker to start..."
  while ! docker info > /dev/null 2>&1; do
    echo -n "."
    sleep 2
  done
  echo -e "\nDocker started!"
fi

#start service
echo "Starting Spark cluster..."
docker compose up -d --build

#poll for spaark master to be ready
echo "Waiting for Spark master..."
for i in {1..30}; do
  if docker compose exec spark-master curl -s http://localhost:8080 > /dev/null 2>&1; then
    echo "Spark master is ready!"
    break
  fi
  echo -n "."
  sleep 2
done
echo

# Run the workload
echo "Running workload in spark-master..."
docker compose exec spark-master bash -lc "cd /app && chmod +x ./run_all.sh && ./run_all.sh"
