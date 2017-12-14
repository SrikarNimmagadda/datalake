from __future__ import print_function
import json
import boto3

# bucket names landing, raw, refine, discover, deliver

def handler(event, context):
  client = boto3.client('lambda')
  resp = client.invoke(
    FunctionName='s3Waiter',
    InvocationType='RequestResponse',
    Payload='{"value":"VALUE_PATH"}'
  )
  print(resp)
  url_title = resp['Payload'].read()
  return url_title

if __name__ == '__main__':
  pt = handler('event', 'handler')
  print("pt: "+pt)