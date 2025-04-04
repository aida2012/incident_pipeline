#!/bin/bash
aws --endpoint-url=http://localhost:4566 --profile localstack s3api put-bucket-notification-configuration \
    --bucket incidents-pipeline \
    --notification-configuration '{
        "LambdaFunctionConfigurations": [
            {
                "LambdaFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:validate_lambda",
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {
                                "Name": "Prefix",
                                "Value": "datasets/raw/"
                            }
                        ]
                    }
                }
            }
        ]
    }'