#!/bin/bash
# =============================================================================
# run_git.sh - Main entry point for running the Advanced Databases project
# =============================================================================
# This script:
# 1. Ensures Docker is running
# 2. Starts the Spark + HDFS cluster
# 3. Waits for all services to be healthy
# 4. Uploads data to HDFS (if not already present)
# 5. Runs the Spark workload (all queries Q1-Q5)
# =============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${CYAN}========================================${NC}"
echo -e "${CYAN}  Advanced Databases - Spark + Hadoop  ${NC}"
echo -e "${CYAN}========================================${NC}"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
  echo -e "${YELLOW}Docker is not running. Starting Docker Desktop...${NC}"
  open -a Docker
  echo "Waiting for Docker to start..."
  while ! docker info > /dev/null 2>&1; do
    echo -n "."
    sleep 2
  done
  echo -e "\n${GREEN}Docker started!${NC}"
fi

# Start all services (HDFS + Spark)
echo -e "\n${CYAN}Starting Spark + HDFS cluster...${NC}"
docker compose up -d --build

# Wait for HDFS NameNode to be healthy
echo -e "\n${CYAN}Waiting for HDFS NameNode to be healthy...${NC}"
for i in {1..60}; do
  if docker compose exec -T namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; then
    echo -e "${GREEN}HDFS is ready!${NC}"
    break
  fi
  if [ $i -eq 60 ]; then
    echo -e "${RED}HDFS failed to start within timeout${NC}"
    exit 1
  fi
  echo -n "."
  sleep 2
done

# Wait for Spark master to be ready
echo -e "\n${CYAN}Waiting for Spark master...${NC}"
for i in {1..30}; do
  if docker compose exec -T spark-master curl -s http://localhost:8080 > /dev/null 2>&1; then
    echo -e "${GREEN}Spark master is ready!${NC}"
    break
  fi
  if [ $i -eq 30 ]; then
    echo -e "${RED}Spark master failed to start within timeout${NC}"
    exit 1
  fi
  echo -n "."
  sleep 2
done

# Check if data exists in HDFS, if not upload it
echo -e "\n${CYAN}Checking HDFS for data...${NC}"
if ! docker compose exec -T namenode hdfs dfs -test -e /data/LA_Crime_Data 2>/dev/null; then
  echo -e "${YELLOW}Data not found in HDFS. Uploading...${NC}"
  
  # Create directory structure
  docker compose exec -T namenode hdfs dfs -mkdir -p /data/LA_Crime_Data
  
  # Upload all data files
  echo "  → Uploading LA_Crime_Data_2010_2019.csv..."
  docker compose exec -T namenode hdfs dfs -put -f /project_data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv /data/LA_Crime_Data/
  
  echo "  → Uploading LA_Crime_Data_2020_2025.csv..."
  docker compose exec -T namenode hdfs dfs -put -f /project_data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv /data/LA_Crime_Data/
  
  echo "  → Uploading LA_Census_Blocks_2020.geojson..."
  docker compose exec -T namenode hdfs dfs -put -f /project_data/LA_Census_Blocks_2020.geojson /data/
  
  echo "  → Uploading LA_Census_Blocks_2020_fields.csv..."
  docker compose exec -T namenode hdfs dfs -put -f /project_data/LA_Census_Blocks_2020_fields.csv /data/
  
  echo "  → Uploading LA_income_2021.csv..."
  docker compose exec -T namenode hdfs dfs -put -f /project_data/LA_income_2021.csv /data/
  
  echo "  → Uploading LA_Police_Stations.csv..."
  docker compose exec -T namenode hdfs dfs -put -f /project_data/LA_Police_Stations.csv /data/
  
  echo "  → Uploading RE_codes.csv..."
  docker compose exec -T namenode hdfs dfs -put -f /project_data/RE_codes.csv /data/
  
  echo "  → Uploading MO_codes.txt..."
  docker compose exec -T namenode hdfs dfs -put -f /project_data/MO_codes.txt /data/
  
  echo -e "${GREEN}Data upload complete!${NC}"
else
  echo -e "${GREEN}Data already exists in HDFS.${NC}"
fi

# Show HDFS contents
echo -e "\n${CYAN}HDFS Contents:${NC}"
docker compose exec -T namenode hdfs dfs -ls -R /data 2>/dev/null | head -20

# Print service URLs
echo -e "\n${CYAN}========================================${NC}"
echo -e "${CYAN}  Service URLs                         ${NC}"
echo -e "${CYAN}========================================${NC}"
echo -e "  HDFS NameNode UI: ${GREEN}http://localhost:9870${NC}"
echo -e "  Spark Master UI:  ${GREEN}http://localhost:8080${NC}"
echo -e "  Spark Worker UI:  ${GREEN}http://localhost:8081${NC}"
echo -e "${CYAN}========================================${NC}"

# Run the workload
echo -e "\n${CYAN}Running Spark workload (Q1-Q5)...${NC}"
docker compose exec spark-master bash -lc "cd /app && chmod +x ./run_all.sh && ./run_all.sh"
