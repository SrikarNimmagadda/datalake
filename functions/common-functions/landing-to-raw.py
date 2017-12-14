from __future__ import print_function
import io
import boto3
import pandas as pd
import yaml
s3 = boto3.resource('s3')

#l = Landing('gs-test-results', 'City-Parish_Employee_Annual_Salaries.csv', 'JOB CODE', 5020, 'gs-test-results', 'pii.csv')
#l.s3_reader()
#l.filter_writer()
class Landing: 

  def __init__(self):    
    self.data = ''
    self.cfg = []

  def config_reader(self):
    print("config reader")
    with open("./config.yml", 'r') as config:
        self.cfg = yaml.load(config)

  def filter_writer(self):
    print("Filtering and writing the files to raw buckets")
    cfg = self.cfg["buckets"]
    for k, v in cfg.items():
      s3_object = s3.Object(cfg[k]["source"], cfg[k]["in_file"])
      # apply data frame to the data
      self.data = pd.read_csv(io.BytesIO(s3_object.get()["Body"].read()), low_memory=False, index_col=False, encoding='utf-8')
      df = pd.DataFrame(self.data)
      # Apply filter and write to target
      print(s3.Object(cfg[k]["target"], cfg[k]["out_file"]).put(df[(df.xs(cfg[k]["query_key"], axis=1) == cfg[k]["query_value"])].to_csv(cfg[k]["out_file"], sep=',', encoding='utf-8')))

def callMethod(o, name):
      getattr(o, name)()

l = Landing()
callMethod(l, "config_reader")
callMethod(l, "filter_writer")
