import unittest
from functions.add_jobflow_steps.step_factory import StepFactory


class StepFactoryHappyPathTest(unittest.TestCase):

    def setUp(self):
        # arrange
        factory = StepFactory('code_bucket')
        script_args = ['arg1', 'arg2', 'arg3']

        # act
        self.step = factory.create('teststep', 'test.py', script_args)

    def test_create_name(self):
        self.assertEqual(self.step['Name'], 'teststep')

    def test_create_fail_action(self):
        self.assertEqual(self.step['ActionOnFailure'], 'CONTINUE')

    def test_create_jar(self):
        self.assertEqual(
            self.step['HadoopJarStep']['Jar'],
            's3://elasticmapreduce/libs/script-runner/script-runner.jar')

    def test_create_args_length(self):
        self.assertEqual(len(self.step['HadoopJarStep']['Args']), 8)

    def test_create_args_command(self):
        self.assertEqual(
            self.step['HadoopJarStep']['Args'][0],
            '/usr/bin/spark-submit')

    def test_create_args_deploymode_opt(self):
        self.assertEqual(self.step['HadoopJarStep']
                         ['Args'][1], '--deploy-mode cluster')

    def test_create_args_jars_opt(self):
        self.assertEqual(self.step['HadoopJarStep']['Args'][2], '--jars')

    def test_create_args_jars_opt_val(self):
        self.assertEqual(
            self.step['HadoopJarStep']['Args'][3],
            's3://code_bucket/EMRJars/spark-csv_2.11-1.5.0.jar,' +
            's3://code_bucket/EMRJars/spark-excel_2.11-0.8.6.jar')

    def test_create_args_script(self):
        self.assertEqual(
            self.step['HadoopJarStep']['Args'][4],
            's3://code_bucket/EMRScripts/test.py')

    def test_create_args_script_args(self):
        self.assertEqual(self.step['HadoopJarStep']['Args'][5], 'arg1')
        self.assertEqual(self.step['HadoopJarStep']['Args'][6], 'arg2')
        self.assertEqual(self.step['HadoopJarStep']['Args'][7], 'arg3')


if __name__ == '__main__':
    unittest.main()
