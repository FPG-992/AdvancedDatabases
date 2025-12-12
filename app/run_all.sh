set -u

SPARK_SUBMIT="/opt/spark/bin/spark-submit"
SPARK_MASTER="spark://spark-master:7077"
APP_DIR="/app/src"
LOG_BASE_DIR="/app/logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
CURRENT_LOG_DIR="$LOG_BASE_DIR/run_$TIMESTAMP"

SEDONA_PACKAGES="org.apache.sedona:sedona-spark-3.5_2.12:1.8.0,org.datasyslab:geotools-wrapper:1.8.0-33.1"

SPARK_COMMON_ARGS=()

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m' 

mkdir -p "$CURRENT_LOG_DIR"

echo -e "${GREEN}=== Storage Mode: HDFS (hdfs://namenode:9000/data) ===${NC}"

print_header() {
    echo -e "\n${CYAN}======================================================${NC}"
    echo -e "${CYAN}  $1${NC}"
    echo -e "${CYAN}======================================================${NC}"
}

run_spark_job() {
    local script_name=$1
    local log_name=$2
    shift 2
    local args=("$@")

    echo -e "Running: ${YELLOW}$script_name${NC} -> Log: $log_name.log"
    
    touch "$CURRENT_LOG_DIR/$log_name.log"

    "$SPARK_SUBMIT" \
        --master "$SPARK_MASTER" \
        "${SPARK_COMMON_ARGS[@]}" \
        "${args[@]}" \
        "$APP_DIR/$script_name" \
        2>&1 | tee "$CURRENT_LOG_DIR/$log_name.log"
}

run_q1() {
    print_header "Running Query 1 (Victim Age Groups)"
    local res_args=("--instances" "4" "--cores" "1" "--memory" "2g")

    # 1. DataFrame No UDF
    "$SPARK_SUBMIT" --master "$SPARK_MASTER" "${SPARK_COMMON_ARGS[@]}" "$APP_DIR/q1.py" --impl create_age_groups_no_udf 2>&1 | tee "$CURRENT_LOG_DIR/q1_df_no_udf.log"
    
    # 2. DataFrame UDF
    "$SPARK_SUBMIT" --master "$SPARK_MASTER" "${SPARK_COMMON_ARGS[@]}" "$APP_DIR/q1.py" --impl create_age_groups_with_udf 2>&1 | tee "$CURRENT_LOG_DIR/q1_df_udf.log"

    # 3. RDD
    "$SPARK_SUBMIT" --master "$SPARK_MASTER" "${SPARK_COMMON_ARGS[@]}" "$APP_DIR/q1.py" --impl create_age_groups_rdd_api 2>&1 | tee "$CURRENT_LOG_DIR/q1_rdd.log"
}

run_q2() {
    print_header "Running Query 2 (Victim Descent)"
    # Specs: 4 executors, 1 core, 2GB
    
    # 1. DataFrame
    "$SPARK_SUBMIT" --master "$SPARK_MASTER" "${SPARK_COMMON_ARGS[@]}" "$APP_DIR/q2.py" --impl df 2>&1 | tee "$CURRENT_LOG_DIR/q2_dataframe.log"

    # 2. SQL
    "$SPARK_SUBMIT" --master "$SPARK_MASTER" "${SPARK_COMMON_ARGS[@]}" "$APP_DIR/q2.py" --impl sql 2>&1 | tee "$CURRENT_LOG_DIR/q2_sql.log"
}

run_q3() {
    print_header "Running Query 3 (MO Codes & Join Strategies)"
    # Specs: 4 executors, 1 core, 2GB

    # 1. RDD
    "$SPARK_SUBMIT" --master "$SPARK_MASTER" "${SPARK_COMMON_ARGS[@]}" "$APP_DIR/q3.py" --impl rdd 2>&1 | tee "$CURRENT_LOG_DIR/q3_rdd.log"

    # 2. DataFrame (Default/Broadcast)
    "$SPARK_SUBMIT" --master "$SPARK_MASTER" "${SPARK_COMMON_ARGS[@]}" "$APP_DIR/q3.py" --impl df --join_hint broadcast 2>&1 | tee "$CURRENT_LOG_DIR/q3_df_broadcast.log"

    # 3. DataFrame (Merge)
    "$SPARK_SUBMIT" --master "$SPARK_MASTER" "${SPARK_COMMON_ARGS[@]}" "$APP_DIR/q3.py" --impl df --join_hint merge 2>&1 | tee "$CURRENT_LOG_DIR/q3_df_merge.log"

    # 4. DataFrame (Shuffle Hash)
    "$SPARK_SUBMIT" --master "$SPARK_MASTER" "${SPARK_COMMON_ARGS[@]}" "$APP_DIR/q3.py" --impl df --join_hint shuffle_hash 2>&1 | tee "$CURRENT_LOG_DIR/q3_df_shuffle_hash.log"

    # 5. DataFrame (Shuffle Replicate NL - Cartesian)
    "$SPARK_SUBMIT" --master "$SPARK_MASTER" "${SPARK_COMMON_ARGS[@]}" "$APP_DIR/q3.py" --impl df --join_hint shuffle_replicate_nl 2>&1 | tee "$CURRENT_LOG_DIR/q3_df_cartesian.log"
}

run_q4() {
    print_header "Running Query 4 (Spatial: Nearest Station - Scaling)"

    # Config 1: 2 executors, 1 core, 2GB
    echo -e "${YELLOW}Config 1: 2 Exec, 1 Core, 2GB${NC}"
    "$SPARK_SUBMIT" --master "$SPARK_MASTER" --packages "$SEDONA_PACKAGES" \
        "${SPARK_COMMON_ARGS[@]}" \
        "$APP_DIR/q4.py" --instances 2 --cores 1 --memory 2g 2>&1 | tee "$CURRENT_LOG_DIR/q4_cfg1.log"

    # Config 2: 2 executors, 2 cores, 4GB
    echo -e "${YELLOW}Config 2: 2 Exec, 2 Cores, 4GB${NC}"
    "$SPARK_SUBMIT" --master "$SPARK_MASTER" --packages "$SEDONA_PACKAGES" \
        "${SPARK_COMMON_ARGS[@]}" \
        "$APP_DIR/q4.py" --instances 2 --cores 2 --memory 4g 2>&1 | tee "$CURRENT_LOG_DIR/q4_cfg2.log"

    # Config 3: 2 executors, 4 cores, 8GB
    echo -e "${YELLOW}Config 3: 2 Exec, 4 Cores, 8GB${NC}"
    "$SPARK_SUBMIT" --master "$SPARK_MASTER" --packages "$SEDONA_PACKAGES" \
        "${SPARK_COMMON_ARGS[@]}" \
        "$APP_DIR/q4.py" --instances 2 --cores 4 --memory 8g 2>&1 | tee "$CURRENT_LOG_DIR/q4_cfg3.log"
}

run_q5() {
    print_header "Running Query 5 (Spatial: Income Correlation - Resources)"

    # Config 1: 2 executors, 4 cores, 8GB
    echo -e "${YELLOW}Config 1: 2 Exec, 4 Cores, 8GB${NC}"
    "$SPARK_SUBMIT" --master "$SPARK_MASTER" --packages "$SEDONA_PACKAGES" \
        "${SPARK_COMMON_ARGS[@]}" \
        "$APP_DIR/q5.py" --instances 2 --cores 4 --memory 8g 2>&1 | tee "$CURRENT_LOG_DIR/q5_cfg1.log"

    # Config 2: 4 executors, 2 cores, 4GB
    echo -e "${YELLOW}Config 2: 4 Exec, 2 Cores, 4GB${NC}"
    "$SPARK_SUBMIT" --master "$SPARK_MASTER" --packages "$SEDONA_PACKAGES" \
        "${SPARK_COMMON_ARGS[@]}" \
        "$APP_DIR/q5.py" --instances 4 --cores 2 --memory 4g 2>&1 | tee "$CURRENT_LOG_DIR/q5_cfg2.log"

    # Config 3: 8 executors, 1 core, 2GB
    echo -e "${YELLOW}Config 3: 8 Exec, 1 Core, 2GB${NC}"
    "$SPARK_SUBMIT" --master "$SPARK_MASTER" --packages "$SEDONA_PACKAGES" \
        "${SPARK_COMMON_ARGS[@]}" \
        "$APP_DIR/q5.py" --instances 8 --cores 1 --memory 2g 2>&1 | tee "$CURRENT_LOG_DIR/q5_cfg3.log"
}

generate_summary() {
    print_header "Execution Time Summary"
    echo "Summary saved to: $CURRENT_LOG_DIR/summary.txt"
    
    echo "==================================================" > "$CURRENT_LOG_DIR/summary.txt"
    echo "  EXECUTION TIMES (Extracted from Logs)           " >> "$CURRENT_LOG_DIR/summary.txt"
    echo "==================================================" >> "$CURRENT_LOG_DIR/summary.txt"
    
    grep "Execution time" "$CURRENT_LOG_DIR"/*.log | awk -F/ '{print $NF}' | sort >> "$CURRENT_LOG_DIR/summary.txt"
    
    cat "$CURRENT_LOG_DIR/summary.txt"
}

# Menu

while true; do
    echo -e "\n${GREEN}=== Spark Project Interactive Runner ===${NC}"
    echo -e "Logs will be saved to: ${YELLOW}$CURRENT_LOG_DIR${NC}"
    echo "1. Run Q1 (Age Groups - All Impls)"
    echo "2. Run Q2 (Victim Descent - All Impls)"
    echo "3. Run Q3 (MO Codes - All Join Hints)"
    echo "4. Run Q4 (Nearest Station - Scaling Experiment)"
    echo "5. Run Q5 (Income Correlation - Resource Experiment)"
    echo "A. RUN EVERYTHING (Q1 - Q5 sequentially)"
    echo "S. Show Summary of Current Run"
    echo "E. Exit"
    read -p "Select an option: " option

    case $option in
        1) run_q1 ;;
        2) run_q2 ;;
        3) run_q3 ;;
        4) run_q4 ;;
        5) run_q5 ;;
        [aA]) 
            run_q1
            run_q2
            run_q3
            run_q4
            run_q5
            generate_summary
            ;;
        [sS]) generate_summary ;;
        [eE]) echo "Exiting."; break ;;
        *) echo -e "${RED}Invalid option.${NC}" ;;
    esac
done