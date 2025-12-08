docker compose up -d   
docker compose exec -it spark-master bash -c "cd /app && chmod +x ./run_all.sh && ./run_all.sh"