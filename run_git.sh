docker compose up -d   
docker compose exec -it spark-master bash -c "cd /app && ./run_all.sh"