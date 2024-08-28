.PHONY: build
build:
	GOOS=wasip1 GOARCH=wasm go build -o vector.wasm main.go

run:
	wasirun --dir / --env-inherit ./vector.wasm

clean:
	rm -f vector.wasm