.PHONY: build
build:
	@echo 'Building...'
	GOOS=linux GOARCH=amd64 go build -ldflags='-s -w' -o=1brc .