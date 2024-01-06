#!/usr/bin/env bash   
set -euo pipefail

LOCALSTACK_HOST=localhost
QUEUE_COORINATES_NAME=coordinates
QUEUE_PROCESS_NAME=process

awslocal --endpoint-url=http://${LOCALSTACK_HOST}:4566 sqs create-queue --queue-name ${QUEUE_COORINATES_NAME}
awslocal --endpoint-url=http://${LOCALSTACK_HOST}:4566 sqs create-queue --queue-name ${QUEUE_PROCESS_NAME}
