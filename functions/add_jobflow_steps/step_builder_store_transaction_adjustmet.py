""" Contains the class StepBuilderStoreTransAdjustment.
Builds EMR Steps for storetransadjustment files.
"""


class StepBuilderStoreTransAdjustment(object):
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
            self._build_step_csv_to_parquet_storetransactionadjustment(),
            self._build_step_storetransactionadjustment_refinery(),
            self._build_step_storetransactionadjustment_delivery()
        ]

        return steps

    # ============================================
    # Step Definitions
    # ============================================

    def _build_step_csv_to_parquet_storetransactionadjustment(self):
        step_name = 'CSVToParquetStoreTransactionAdjustment'
        script_name = 'Facts/StoreTransactionAdjustmentsCSVToParquet.py'
        input_bucket = self.buckets['raw_regular']
        output_bucket = self.buckets['discovery_regular']

        script_args = [
            's3://' + output_bucket + '/StoreTransactionAdjustment',
            's3://' + input_bucket + '/StoreTransAdjustments/StoreTrans/Working',
            's3://' + input_bucket + '/StoreTransAdjustments/MISC_input/Working'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_storetransactionadjustment_refinery(self):
        step_name = 'StoreTransactionAdjustmentRefinery'
        script_name = 'Facts/StoreTransactionAdjustmentsRefined.py'
        input_bucket = self.buckets['discovery_regular']
        output_bucket = self.buckets['refined_regular']

        script_args = [
            's3://' + output_bucket + '/StoreTransactionAdj',
            's3://' + input_bucket + '/StoreTransactionAdjustment/Working1/',
            's3://' + input_bucket + '/StoreTransactionAdjustment/Working2/'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_storetransactionadjustment_delivery(self):
        step_name = 'StoreTransactionAdjustmentDelivery'
        script_name = 'Facts/StoreTransactionAdjustmentsDelivery.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['delivery_regular']

        script_args = [
            's3://' + output_bucket + '/WT_STORE_TRANS_ADJMNTS',
            's3://' + input_bucket + '/StoreTransactionAdj/Working/'
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
