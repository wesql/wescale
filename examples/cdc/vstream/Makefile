.PHONY: build
build:
	GOOS=wasip1 GOARCH=wasm go build -o demo.wasm demo.go

run-wasm:
	wasirun --dir / --env-inherit ./demo.wasm

clean:
	rm -f demo.wasm