#!/bin/bash

# set profile
export AWS_PROFILE=localstack

# create incidents-pipeline bucket
./build_s3bucket.sh


# create iam role
./build_lambdaExecutionRole.sh 


# create lambda function
./build_validate_lambda.sh





