<h1>TODO Notes</h1>
Need to implement the packaging of python code for lambda and uploading to s3.  TeamCity can pick up the package and do further build steps as needed.

https://github.com/immobilienscout24/pybuilder_aws_plugin

Add coverage back in.  It was failing the build at less than 70% code coverage.  Which is cool but not while daddy's working.

check out cfn-sphere for cloudformation templates in YAML in upload to S3.

#create
	aws lambda create-function --region $(REGION) --function-name $(FUNCTION_NAME) --zip-file fileb://$(ZIPFILE_NAME) --role arn:aws:iam::$(ACCOUNT_ID):role/$(ROLE_NAME)  --handler $(HANDLER) --runtime python2.7 --timeout $(TIMEOUT) --memory-size $(MEMORY_SIZE)
    
#update
	aws lambda update-function-code --region $(REGION) --function-name $(FUNCTION_NAME) --zip-file fileb://$(ZIPFILE_NAME) --publish
