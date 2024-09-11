.PHONY: clean build run test

clean:
	rm -f main/rediscdc
	rm -f wasm/rediscdc.wasm

build:
	go build -o main/rediscdc main/main.go
	GOOS=wasip1 GOARCH=wasm go build -o wasm/rediscdc.wasm wasm/main.go


runwasm:
	export TABLE_SCHEMA=d1; \
	export SOURCE_TABLE_NAME=redis_test; \
	export WESCALE_HOST=127.0.0.1; \
	export WESCALE_GRPC_PORT=15991; \
	export REDIS_ADDRESS=127.0.0.1:6379;\
	export REDIS_PASSWORD=""; \
	export REDIS_DB=0;\
	wasirun --dir / --env-inherit ./wasm/rediscdc.wasm

test:
	go test test/redis_test.go