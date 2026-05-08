build:
	@go build -o bin/fs

run: build
	@./bin/fs

upload:
	@if [ -z "$(FILE)" ]; then \
		echo "Usage: make upload FILE=<filename>"; \
		exit 1; \
	fi
	@go run . $(FILE)

test:
	@go test ./... -v

clean:
	@rm -rf bin/
	@rm -rf :3000_network/ :4000_network/ :5000_network/
	@echo "Cleaned build artifacts and network directories"
