all:
	docker compose up -d

down:
	docker compose down

exec_python:
	docker exec -it python bash

exec_jupiter:
	docker exec -it jupyter_lab bash

clean: down
	yes | docker system prune -a
