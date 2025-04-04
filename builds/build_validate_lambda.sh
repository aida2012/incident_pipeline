#!/bin/bash

LAMBDA_PATH=$(realpath ../lambdas/validate_lambda.py)
7z a validate_lambda.zip "$LAMBDA_PATH"


aws --endpoint-url=http://localhost:4566 --profile localstack lambda create-function \
    --function-name validate_lambda \
    --zip-file fileb://validate_lambda.zip \
    --handler validate_lambda.lambda_handler \
    --runtime python3.8 \
    --role "arn:aws:iam::000000000000:role/lambda-execution-role"

aws --endpoint-url=http://localhost:4566 --profile localstack lambda add-permission \
  --function-name validate_lambda \
  --principal s3.amazonaws.com \
  --statement-id 1244 \
  --action "lambda:InvokeFunction" \
  --source-arn arn:aws:s3:::incidents-pipeline