LAMBDA_PATH=$(realpath ../lambdas/validate_lambda.py)
7z a -tzip validate_lambda.zip "$LAMBDA_PATH"

aws --endpoint-url=http://localhost:4566 --profile localstack lambda update-function-code \
    --function-name validate_lambda \
    --zip-file fileb://validate_lambda.zip



