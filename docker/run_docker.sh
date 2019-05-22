#!/bin/bash

port=${1:-10000}

pygmql_docker_name="pygmql"

run_docker() {
	echo "Running PyGMQL docker"
	docker run --rm \
	           --name ${pygmql_docker_name}_instance \
	           -p $port:8888 \
	           $DOCKERHUB_USERNAME/$pygmql_docker_name
}

run_docker
