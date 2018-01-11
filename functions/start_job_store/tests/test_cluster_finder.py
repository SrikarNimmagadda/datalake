import os
import unittest
from mock import Mock, MagicMock
from cluster_finder import cluster_finder

CWD = os.path.split(os.path.abspath(__file__))[0]

with open(CWD + '/describe_stack_output.json', 'r') as describe_stack_file:
    DESCRIBE_STACK_RESPONSE = describe_stack_file.read()

class SampleTest(unittest.TestCase):

    def test_find_cluster_happypath(self):
        # arrange
        cloudformation = Mock()
        cloudformation.describe_stacks = MagicMock(return_value=DESCRIBE_STACK_RESPONSE)

        finder = cluster_finder(cloudformation)

        # act
        cluster_id = finder.find_cluster('does_not_matter')

        # assert
        self.assertEqual(cluster_id, 'j-3I9MFY6R3ETM0')


if __name__ == '__main__':
    unittest.main()
