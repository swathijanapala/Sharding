run:
	docker compose up -d
remove:
	docker rm server1
	docker rmi fserver

stop:
	docker compose down

	@CONTAINERS=$$(docker ps -aq); \
	if [ -n "$$CONTAINERS" ]; then \
		docker rm -f $$CONTAINERS; \
	else \
		echo "No containers to stop."; \
	fi

	docker rmi -f loadbalancer
	docker rmi -f flaskserver1
