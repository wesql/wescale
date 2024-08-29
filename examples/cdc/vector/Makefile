.PHONY: clean build run

clean:
	rm -f main/vector
	rm -f wasm/vector.wasm

build:
	go build -o main/vector main/main.go
	GOOS=wasip1 GOARCH=wasm go build -o wasm/vector.wasm wasm/main.go


runwasm:
	export TABLE_SCHEMA=d1; \
	export SOURCE_TABLE_NAME=t1; \
	export WESCALE_HOST=127.0.0.1; \
	export WESCALE_GRPC_PORT=15991; \
	export EMBEDDING_MODEL=text-embedding-3-large; \
	export EMBEDDING_URL=https://api.gptsapi.net/v1; \
	export VECTOR_STORE_TYPE=qdrant; \
	export VECTOR_STORE_URL=http://127.0.0.1:6333/; \
	export VECTOR_STORE_COLLECTION_NAME=t1_vector; \
	wasirun --dir / --env-inherit ./wasm/vector.wasm
