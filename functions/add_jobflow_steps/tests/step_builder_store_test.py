from datetime import datetime
import unittest
from mock import Mock

from functions.add_jobflow_steps.step_factory import StepFactory
from functions.add_jobflow_steps.step_builder_store import StepBuilderStore


class StepBuilderStoreTest(unittest.TestCase):

    def test_build_path(self):
        # arrange
        stamp = datetime(1999, 1, 7, 12, 55, 0, 0)
        buckets = {
            'discovery_regular': 'tb-app-datalake-discovery-regular',
            'refined_regular': 'tb-app-datalake-refined-regular'
        }

        builder = StepBuilderStore({}, {}, buckets, stamp)

        # act
        path = builder._build_path('testbucket', 'store', 'testfile')

        # assert
        self.assertEqual(
            path,
            's3://testbucket/store/1999/01/testfile199901071255/*.parquet')

    def test_build_step_tech_brand_hierarchy_happy(self):
        # arrange
        stamp = datetime(1999, 1, 7, 12, 55, 0, 0)
        buckets = {
            'refined_regular': 'tb-app-datalake-refined-regular',
            'delivery_regular': 'tb-app-datalake-delivery-regular',
            'code': 'tb-app-datalake-code'
        }

        factory = Mock(StepFactory)
        builder = StepBuilderStore(factory, {}, buckets, stamp)
        # paths = {
        # 'store_refine': 'sr',
        # 'att_dealer': 'dlr'
        # }

        # act
        # self.step = builder._build_step_store_hier_delivery(paths)
        self.step = builder._build_step_store_hier_delivery()

        # assert
        factory.create.assert_called_with(
            'StoreHierarchyDelivery',
            'Dimensions/DimStoreHierDelivery.py',
            # ['s3://tb-app-datalake-discovery-regular', 's3://tb-app-datalake-delivery']
            ['s3://tb-app-datalake-refined-regular/Store/Working', 's3://tb-app-datalake-delivery-regular/WT_STORE_HIER/Current']
        )


if __name__ == '__main__':
    unittest.main()
