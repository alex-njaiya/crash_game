include .env
export

DB_URL=postgres://$(POSTGRES_USER):$(POSTGRES_PASS)@localhost:5432/$(POSTGRES_DB)?sslmode=disable

migrate-up:
	docker run -v $(PWD)/migrations:/migrations --network host migrate/migrate \
		-path=/migrations -database "$(DB_URL)" up

migrate-down:
	docker run -v $(PWD)/migrations:/migrations --network host migrate/migrate \
		-path=/migrations -database "$(DB_URL)" down 1

migrate-version:
	docker run -v $(PWD)/migrations:/migrations --network host migrate/migrate \
		-path=/migrations -database "$(DB_URL)" version