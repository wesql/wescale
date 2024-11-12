.DEFAULT_GOAL := build

.PHONY: build
build:
	@echo "Building connection-pool-tester..."
	go build -o bin/connection-pool-tester .
	@echo "Build complete"

.PHONY: clean
clean:
	@echo "Cleaning..."
	rm -rf bin/
	@echo "Clean complete"

.PHONY: build-image
build-image:
	@echo "Building Docker image..."
	docker build -t connection-pool-tester:latest .
	@echo "Docker image built"

.PHONY: push-image
push-image:
	@echo "Pushing Docker image..."
	docker tag connection-pool-tester:latest connection-pool-tester:latest
	docker push connection-pool-tester:latest
	@echo "Docker image pushed"

.PHONY: run
run:
	./bin/connection-pool-tester \
		--mysql-host=localhost \
		--mysql-port=15306 \
		--mysql-user=root \
		--mysql-password=passwd \
		--mysql-database=information_schema \
		--connection-count=100 \
		--duration=1m \
		--query="SELECT 1" \
		--exit-on-failure=true

.PHONY: docker-run
docker-run:
	docker run --rm \
		-e MYSQL_HOST=host.docker.internal \
		-e MYSQL_PORT=15306 \
		-e MYSQL_USER=root \
		-e MYSQL_PASSWORD=passwd \
		-e MYSQL_DATABASE=information_schema \
		-e CONNECTION_COUNT=100 \
		-e DURATION=1m \
		-e QUERY="SELECT 1" \
		-e EXIT_ON_FAILURE=true \
		connection-pool-tester:latest