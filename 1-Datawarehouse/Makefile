all:
	docker compose up -d

down:
	docker compose down

exec:
	docker exec -it python bash

postgres:
	docker exec -it postgres psql -U imontero -d piscineds -h localhost -W

clean: down
	yes | docker system prune -a
