#!/bin/bash

if [ -z "$KUBERNETES_TOKEN" ]
then
	>&2 echo "KUBERNETES_TOKEN not set"
	exit 22
fi

if [ -z "$TRAVIS_COMMIT" ]
then
	>&2 echo "TRAVIS_COMMIT not set"
	exit 22
fi


docker run \
	--env KUBERNETES_TOKEN \
	--env DOCKER_IMAGE_SLUG=dm874/jolie-exec-migrations \
	--env DOCKER_IMAGE_TAG=$TRAVIS_COMMIT \
	--env SERVICE=jolie-exec \
	--env CONTAINER=migrations-container \
	dm874/deploy 
