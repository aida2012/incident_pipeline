#!/bin/bash
aws --profile localstack --endpoint-url=http://localhost:4566 s3 mb s3://incidents-pipeline