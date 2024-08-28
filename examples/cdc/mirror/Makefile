.PHONY: build
build:
	GOOS=wasip1 GOARCH=wasm go build -o mirror.wasm main.go

run-wasm:
	wasirun --dir / --env-inherit ./mirror.wasm

clean:
	rm -f mirror.wasm