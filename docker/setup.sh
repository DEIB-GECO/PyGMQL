#!/bin/bash

pygmql_docker_name="pygmql"

build_docker() {
	echo "Building GMQL docker"
	docker build . -t $DOCKERHUB_USERNAME/$pygmql_docker_name -f docker/Dockerfile
}

download_data() {
    echo "Downloading data"
    wget https://s3.us-east-2.amazonaws.com/geco-repository/HG19_ENCODE_BROAD_acetylation.tar.gz
    tar xvfs HG19_ENCODE_BROAD_acetylation.tar.gz
    rm HG19_ENCODE_BROAD_acetylation.tar.gz
    mv HG19_ENCODE_BROAD/ examples/data/
}

docker_login() {
	echo "Logging in into DockerHub"
	docker login --username=$DOCKERHUB_USERNAME --password=$DOCKERHUB_PASSWORD
}

push_docker() {
	echo "Pushing docker image to DockerHub"
	docker push $DOCKERHUB_USERNAME/$pygmql_docker_name
}

download_data
build_docker
docker_login
push_docker