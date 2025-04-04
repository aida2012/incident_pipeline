
# extend Lambda Function timeout
aws --endpoint-url=http://localhost:4566 --profile localstack lambda update-function-configuration \
  --function-name validate_lambda \
  --timeout 300

# add trigger configuration to S3 bucket
./build_s3trigger.sh