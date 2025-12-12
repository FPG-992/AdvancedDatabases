#!/bin/bash
# =============================================================================
# upload_to_hdfs.sh - Upload project data to HDFS
# =============================================================================
# This script uploads all project datasets to HDFS for distributed processing.
# Run this AFTER docker compose up and HDFS is healthy.
# =============================================================================

set -e

HDFS_PATH="/data"
LOCAL_PATH="/project_data"

echo "========================================"
echo "  Uploading Data to HDFS"
echo "========================================"

# Wait for HDFS to be ready
echo "Waiting for HDFS to be ready..."
until docker compose exec -T namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; do
    echo "  HDFS still in safe mode, waiting..."
    sleep 5
done
echo "✓ HDFS is ready!"

# Create directory structure
echo ""
echo "Creating HDFS directories..."
docker compose exec -T namenode hdfs dfs -mkdir -p ${HDFS_PATH}/LA_Crime_Data

# Upload files
echo ""
echo "Uploading files to HDFS..."

echo "  → LA_Crime_Data_2010_2019.csv"
docker compose exec -T namenode hdfs dfs -put -f ${LOCAL_PATH}/LA_Crime_Data/LA_Crime_Data_2010_2019.csv ${HDFS_PATH}/LA_Crime_Data/

echo "  → LA_Crime_Data_2020_2025.csv"
docker compose exec -T namenode hdfs dfs -put -f ${LOCAL_PATH}/LA_Crime_Data/LA_Crime_Data_2020_2025.csv ${HDFS_PATH}/LA_Crime_Data/

echo "  → LA_Census_Blocks_2020.geojson"
docker compose exec -T namenode hdfs dfs -put -f ${LOCAL_PATH}/LA_Census_Blocks_2020.geojson ${HDFS_PATH}/

echo "  → LA_Census_Blocks_2020_fields.csv"
docker compose exec -T namenode hdfs dfs -put -f ${LOCAL_PATH}/LA_Census_Blocks_2020_fields.csv ${HDFS_PATH}/

echo "  → LA_income_2021.csv"
docker compose exec -T namenode hdfs dfs -put -f ${LOCAL_PATH}/LA_income_2021.csv ${HDFS_PATH}/

echo "  → LA_Police_Stations.csv"
docker compose exec -T namenode hdfs dfs -put -f ${LOCAL_PATH}/LA_Police_Stations.csv ${HDFS_PATH}/

echo "  → RE_codes.csv"
docker compose exec -T namenode hdfs dfs -put -f ${LOCAL_PATH}/RE_codes.csv ${HDFS_PATH}/

echo "  → MO_codes.txt"
docker compose exec -T namenode hdfs dfs -put -f ${LOCAL_PATH}/MO_codes.txt ${HDFS_PATH}/

# Verify upload
echo ""
echo "========================================"
echo "  HDFS Contents"
echo "========================================"
docker compose exec -T namenode hdfs dfs -ls -R ${HDFS_PATH}

echo ""
echo "========================================"
echo "  ✓ Upload Complete!"
echo "========================================"
echo ""
echo "HDFS Web UI: http://localhost:9870"
echo "Data path:   hdfs://namenode:9000${HDFS_PATH}"
