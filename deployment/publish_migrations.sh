#!/bin/bash
if [ -z "$DOCKER_USERNAME" ]
then
	>&2 echo "DOCKER_USERNAME not provided"
	exit 22
fi

if [ -z "$DOCKER_PASSWORD" ]
then
	>&2 echo "DOCKER_PASSWORD not provided"
	exit 22
fi

if [ -z "$TRAVIS_COMMIT" ]
then
	>&2 echo "TRAVIS_COMMIT not set, did you run outside of Travis CI?"
	exit 22
fi

echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker tag migrations dm874/jolie-exec-migrations:$TRAVIS_COMMIT
docker push dm874/jolie-exec-migrations:$TRAVIS_COMMIT
