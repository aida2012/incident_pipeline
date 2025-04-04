#!/bin/bash

aws --endpoint-url=http://localhost:4566 --profile localstack iam create-role \
    --role-name lambda-execution-role \
    --assume-role-policy-document file://../config/lambdaExecutionRolePolicy.json
