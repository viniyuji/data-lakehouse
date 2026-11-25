up:
	docker compose up -d --build

down:
	docker compose down -v

listen-output:
	docker exec -i kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:29092 \
		--topic sales_enriched \
		--from-beginning | grep -v '^null$' | jq .

generate-data:
	uv run generate_data.py