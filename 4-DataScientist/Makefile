all:
	docker compose up -d

run:
	docker logs jupyter_lab
	
down:
	docker compose down

python:
	docker exec -it python bash

jupyter:
	docker exec -it jupyter_lab bash

clean: down
	yes | docker system prune -a
