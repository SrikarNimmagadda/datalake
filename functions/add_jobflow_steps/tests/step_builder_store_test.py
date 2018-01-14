from datetime import datetime
import unittest
from functions.add_jobflow_steps.step_builder_store import StepBuilderStore


class StepBuilderStoreTest(unittest.TestCase):

    def test_build_path(self):
        # arrange
        stamp = datetime(1999, 1, 7, 12, 55, 0, 0)
        buckets = {
            'discovery_regular': 'tb-app-datalake-discovery-regular',
            'refined_regular': 'tb-app-datalake-discovery-regular'
        }

        builder = StepBuilderStore({}, {}, buckets, stamp)

        # act
        path = builder._build_path('testbucket', 'store', 'testfile')

        # assert
        self.assertEqual(
            path,
            's3://testbucket/store/1999/01/testfile199901071255/*.parquet')


if __name__ == '__main__':
    unittest.main()
