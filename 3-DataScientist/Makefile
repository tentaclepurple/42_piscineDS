all:
	docker compose up --build

down:
	docker compose down

exec:
	docker exec -it jupyter_lab bash

clean: down
	yes | docker system prune -a
