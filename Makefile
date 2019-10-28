default: build

build:
	docker build -t log-collection:latest .
	docker tag log-collection:latest 831196587420.dkr.ecr.us-east-1.amazonaws.com/log-collection:latest
  
run:
	docker run -d --name log-collection -v /usr1/volumes/logs:/logs -p 1010:80 $(SD_DOCKER_OPTS) log-collection:latest

term:
	docker run -it --rm -v /usr1/volumes/logs:/logs -p 1010:80 $(SD_DOCKER_OPTS) log-collection:latest bash
