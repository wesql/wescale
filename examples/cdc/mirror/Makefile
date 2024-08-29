.PHONY: clean build run

clean:
	rm -f main/mirror
	rm -f wasm/mirror.wasm

build:
	go build -o main/mirror main/main.go
	GOOS=wasip1 GOARCH=wasm go build -o wasm/mirror.wasm wasm/main.go


runwasm:
	export TABLE_SCHEMA=d1; \
	export SOURCE_TABLE_NAME=t1; \
	export TARGET_TABLE_NAME=t2; \
	export TARGET_META_TABLE_NAME=t2_meta; \
	export WESCALE_HOST=127.0.0.1; \
	export WESCALE_GRPC_PORT=15991; \
	wasirun --dir / --env-inherit ./wasm/mirror.wasm

