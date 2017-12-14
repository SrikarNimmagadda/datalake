from __future__ import print_function
import boto3
import yaml
s3 = boto3.resource('s3')

# Usage
# cr = ConfigReader()
# cfg = cr.yaml_config_reader("../config.yml")
# buckets = cfg["buckets"]
# for k, v in buckets.items():
#    s3_object = s3.Object(buckets[k]["source"], buckets[k]["in_file"])
#    print(s3_object)

class ConfigReader():

    def __init__(self, cfg_file):
        self.cfg = []
        self.cfg_file = cfg_file

    # configuration reader from a YAML file.
    # YAML will provide organizational structure to your properties and connection string definitions
    # Exception handling defined for "File not Found", additional exceptions may be defined here as well
    def yaml_config_reader(self):
        try:
            # config is defined here for this solution, however, it can be abstracted to the class scope for using other 
            # configuration files parsed with the same method, utilizing the same exception handling
            with open(self.cfg_file, 'r') as config:
                if self.cfg_file.endswith('.yml'):
                    self.cfg = yaml.load(config)
                return self.cfg
        except IOError, (ErrorNumber, ErrorMessage):
            if ErrorNumber == 2: # file not found
                print("Cannot find the configuration, please validate your YAML config exists")
            elif ErrorNumber != 2:
                print % ErrorNumber
                print % ErrorMessage