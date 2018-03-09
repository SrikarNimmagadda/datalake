"""Contains the class StepBuilderProduct.
Builds EMR Steps for Product files.
"""


class StepBuilderProduct(object):
    """Build the steps that will be sent to the EMR cluster."""

    def __init__(self, step_factory, s3, buckets, now):
        """Construct the StepBuilder

        Arguments:
        step_factory: an instance of the StepFactory
        s3: the boto3 s3 client
        buckets: a dictionary of the bucket names we can use
        now: a datetime object
        """
        self.step_factory = step_factory
        self.s3_client = s3
        self.buckets = buckets

        self.date_parts = {
            'time': now.strftime('%Y%m%d%H%M'),
            'year': now.strftime('%Y'),
            'month': now.strftime('%m')
        }

    def build_steps(self):
        """Return list of steps that will be sent to the EMR cluster."""

        steps = [
            self._build_step_csv_to_parquet_productcategory(),
            self._build_step_productcategory_refinery(),
            self._build_step_productcategory_delivery(),
            self._build_step_csv_to_parquet_product(),
            self._build_step_product_refinery(),
            self._build_step_product_delivery()
        ]

        return steps

    # ============================================
    # Step Definitions
    # ============================================

    def _build_step_csv_to_parquet_productcategory(self):
        step_name = 'CSVToParquetProductCategory'
        script_name = 'Dimensions/ProductCategoryCSVToParquet.py'
        input_bucket = self.buckets['raw_regular']
        output_bucket = self.buckets['discovery_regular']

        script_args = [

            's3://' + output_bucket,
            input_bucket

        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_productcategory_refinery(self):
        step_name = 'ProductCategoryRefinery'
        script_name = 'Dimensions/ProductCategoryDiscoveryToRefined.py'
        input_bucket = self.buckets['discovery_regular']
        output_bucket = self.buckets['refined_regular']
        error_bucket = self.buckets['data_processing_errors']

        script_args = [
            's3://' + output_bucket,
            output_bucket,
            error_bucket,
            's3://' + input_bucket + '/ProductCateogry/Working'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_productcategory_delivery(self):
        step_name = 'ProductCategoryDelivery'
        script_name = 'Dimensions/ProductCategoryRefinedToDelivery.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['delivery_regular']

        script_args = [
            input_bucket,
            's3://' + output_bucket + '/WT_PROD_CAT'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_csv_to_parquet_product(self):
        step_name = 'CSVToParquetProduct'
        script_name = 'Dimensions/ProductCSVToParquet.py'
        input_bucket = self.buckets['raw_regular']
        output_bucket = self.buckets['discovery_regular']

        script_args = [
            's3://' + output_bucket,
            input_bucket
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_product_refinery(self):
        step_name = 'ProductRefinery'
        script_name = 'Dimensions/ProductDiscoveryToRefined.py'
        input_bucket = self.buckets['discovery_regular']
        output_bucket = self.buckets['refined_regular']
        error_bucket = self.buckets['data_processing_errors']

        script_args = [
            's3://' + output_bucket,
            output_bucket,
            error_bucket,
            's3://' + input_bucket
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_product_delivery(self):
        step_name = 'ProductDelivery'
        script_name = 'Dimensions/ProductRefinedToDelivery.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['delivery_regular']

        script_args = [
            input_bucket,
            's3://' + output_bucket + '/WT_PROD'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    # ============================================
    # Support Methods
    # ============================================

    def _find_source_file(self, bucketname, filter_prefix):
        bucket = self.s3_client.Bucket(bucketname)
        data = [obj for obj in list(bucket.objects.filter(
            Prefix=filter_prefix)) if obj.key != filter_prefix]

        length = len(data)

        # appends the last matched item to the list
        # Are we assuming that all previous files have been processed?
        # That seems like a dangerous assumption
        # I think this is where the dynamo table must come in
        # to track what has been processed.
        i = 0
        for obj in data:
            i = i + 1
            if i == length:
                return 's3://' + bucketname + '/' + obj.key

        # takes care of the case when no files match the filter.
        # will probably cause problems further downstream unless
        # the case for an empty filename is checked for
        return None

    def _build_raw_file_list(self, filters):
        file_list = []

        for file_filter in filters:
            file_name = self._find_source_file(
                self.buckets['raw_regular'],
                file_filter)

            if file_name is None:
                # since passing as an argument to emr,
                # can't be empty string.
                # Need to use a proxy null value.
                file_list.append('nofile')
            else:
                file_list.append(file_name)

                return file_list
