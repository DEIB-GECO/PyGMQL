#!/bin/bash

pygmql_docker_name="pygmql"

build_docker() {
	echo "Building GMQL docker"
	docker build . -t $DOCKERHUB_USERNAME/$pygmql_docker_name -f docker/Dockerfile
}


docker_login() {
	echo "Logging in into DockerHub"
	docker login --username=$DOCKERHUB_USERNAME --password=$DOCKERHUB_PASSWORD
}

push_docker() {
	echo "Pushing docker image to DockerHub"
	docker push $DOCKERHUB_USERNAME/$pygmql_docker_name
}

build_docker
docker_login
push_docker