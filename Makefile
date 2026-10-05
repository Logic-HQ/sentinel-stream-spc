infra-up:
	docker-compose -f deployments/docker-compose.yml up -d

stream-start:
	spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.x.x src/streaming/main.py

run-all: infra-up stream-start
