""" Contains the class StepFactory"""

class StepFactory(object):
    """Creates new EMR job flow steps"""

    def __init__(self, code_bucket):
        """Constructor

        Arguments:
        code_bucket: the name of the bucket containng your emr scripts
        """
        self.bucket = code_bucket

    def create(self, step_name, script_name, script_args):
        """Return a new job flow step

        Arguments
        step_name: the name of the new step
        script_name: the name of the python file to execute. Do not
                     include the '/EMRJars/' prefix, though if your
                     script is in a subfolder of EMRJars, you should
                     include that: e.g. 'Customer/CustomerDelivery.py'
        script_args: a list of arguments to pass to your script.
                     Do not include the EMR Step arguments--this method
                     will create those arguments.
        """
        jar = 's3://elasticmapreduce/libs/script-runner/script-runner.jar'
        args = self._build_step_args(script_name, script_args)

        step = {
            'Name': step_name,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': jar,
                'Args': args
            }
        }

        return step

    def _build_step_args(self, script_name, script_args):
        csv_jar = 's3://' + self.bucket + '/EMRJars/spark-csv_2.11-1.5.0.jar'
        excel_jar = 's3://' + self.bucket + '/EMRJars/spark-excel_2.11-0.8.6.jar'

        jars_arg = csv_jar + ',' + excel_jar

        args = [
            '/usr/bin/spark-submit',
            '--jars', jars_arg,
            's3://' + self.bucket + '/EMRScripts/' + script_name
        ]

        return args + script_args
