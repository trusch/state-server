run-server: run-postgres
	go run cmd/state-server/main.go

run-postgres:
	-@podman kill postgres >/dev/null
	-@podman rm postgres >/dev/null
	-@podman run --rm -d --name postgres -p 5432:5432 postgres >/dev/null
