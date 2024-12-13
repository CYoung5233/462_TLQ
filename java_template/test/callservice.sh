#!/bin/bash
# JSON object to pass to Lambda Function
json={"\"row\"":50,"\"col\"":10,"\"bucketname\"":\"project.bucket462.tlq\"","\"filename\"":\"test.csv\""}
echo "Invoking Lambda function using API Gateway"
time output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://p76vzlkcughwlrsz7nujrg4ymy0kepan.lambda-url.us-east-1.on.aws/`
echo “”
echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""
echo "Invoking Lambda function using AWS CLI (Boto3)"
time output=`aws lambda invoke --invocation-type RequestResponse --cli-binaryformat raw-in-base64-out --function-name 462_SAAF_java --region us-east-1 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`
echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""