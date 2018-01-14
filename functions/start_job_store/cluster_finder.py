class ClusterFinder(object):
    def __init__(self, cloudFormation):
        self.cloudformation = cloudFormation

    def find_cluster(self, emrStackName):
        response = self.cloudformation.describe_stacks(
            StackName=emrStackName)

        outputs = response['Stacks'][0]['Outputs']

        results = filter(
            lambda output_item: output_item['OutputKey'] == 'ClusterId',
            outputs)

        # assumes the output exists
        cluster_id = results[0]['OutputValue']

        return cluster_id
