#!/bin/bash

if [ $# -lt 3 ]; then
    echo
    echo "Usage: ./build_docker_image.sh METABASE_VERSION CLICKHOUSE_DRIVER_VERSION DOCKER_IMAGE_TAG"
    echo
    echo "This script builds and tags a Metabase Docker image with ClickHouse driver built-in"
    echo
    echo "Example:"
    echo
    echo "./build_docker_image.sh v0.44.6 0.8.3 my-metabase-with-clickhouse:v0.0.1"
    exit 1
fi

export DOWNLOAD_URL="https://github.com/ClickHouse/metabase-clickhouse-driver/releases/download/$2/clickhouse.metabase-driver.jar"
echo "Downloading the driver from $DOWNLOAD_URL"

cd .build
curl -L -o clickhouse.metabase-driver.jar $DOWNLOAD_URL
docker build --build-arg METABASE_VERSION=$1 --tag $3 .
rm clickhouse.metabase-driver.jar
