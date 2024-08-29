.PHONY: build
build:
	GOOS=wasip1 GOARCH=wasm go build -o mirror.wasm main.go

run:
	export TABLE_SCHEMA=d1
    export SOURCE_TABLE_NAME=t1
    export TARGET_TABLE_NAME=t2
    export TARGET_META_TABLE_NAME=t2_meta
    export WESCALE_HOST=127.0.0.1
    export WESCALE_GRPC_PORT=15991
	wasirun --dir / --env-inherit ./mirror.wasm

clean:
	rm -f mirror.wasm