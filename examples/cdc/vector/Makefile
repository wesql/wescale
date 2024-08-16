.PHONY: build
build:
	GOOS=wasip1 GOARCH=wasm go build -o main.wasm main.go

run-wasm:
	wasirun --dir / --env-inherit ./main.wasm

clean:
	rm -f main.wasm