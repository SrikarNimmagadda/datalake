""" Contains the class StepBuilderSalesOthers.
Builds EMR Steps for sales files.
"""


class StepBuilderSalesOthers(object):
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
            self._build_step_csv_to_parquet_salesleads(),
            self._build_step_salesleads_refinery(),
            self._build_step_salesleads_delivery(),
            self._build_step_csv_to_parquet_storetraffic(),
            self._build_step_storetraffic_refinery(),
            self._build_step_storetraffic_delivery()
        ]

        return steps

    # ============================================
    # Step Definitions
    # ============================================

    def _build_step_csv_to_parquet_salesleads(self):
        step_name = 'CSVToParquetSalesLeads'
        script_name = 'SalesLeadCSVToParquet_Latest.py'
        input_bucket = self.buckets['raw_regular']
        output_bucket = self.buckets['discovery_regular']

        script_args = [

            's3://' + input_bucket + '/SalesLeads',
            's3://' + output_bucket + '/SalesLeads'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_salesleads_refinery(self):
        step_name = 'SalesLeadsRefinery'
        script_name = 'SalesLeadsRefined_Latest.py'
        input_bucket = self.buckets['discovery_regular']
        output_bucket = self.buckets['refined_regular']
        raw_bucket = self.buckets['raw_regular']

        script_args = [

            's3://' + output_bucket + '/Employee/Working',
            's3://' + output_bucket + '/Store/Working',
            's3://' + output_bucket + '/StoreDealerAssociation/Working',
            's3://' + input_bucket + '/ATTDealerCode/Working',
            's3://' + input_bucket + '/SalesLeads/Working',
            's3://' + raw_bucket + '/Company',
            's3://' + output_bucket + '/SalesLeads'

        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_salesleads_delivery(self):
        step_name = 'SalesLeadsDelivery'
        script_name = 'SalesLeadDelivery_Latest.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['delivery']

        script_args = [
            's3://' + input_bucket + 'SalesLeads/Working',
            's3://' + output_bucket + '/WT_SALES_LEADS'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_csv_to_parquet_storetraffic(self):
        step_name = 'CSVToParquetStoreTraffic'
        script_name = 'StoreTrafficCSVToParquet.py'
        input_bucket = self.buckets['raw_regular']
        output_bucket = self.buckets['discovery_regular']

        script_args = [

            's3://' + input_bucket + '/StoreTraffic',
            's3://' + output_bucket + '/StoreTraffic'

        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_storetraffic_refinery(self):
        step_name = 'StoreTrafficRefinery'
        script_name = 'StoreTrafficRefined.py'
        input_bucket = self.buckets['discovery_regular']
        output_bucket = self.buckets['refined_regular']

        script_args = [

            's3://' + input_bucket + '/StoreTraffic/Working',
            's3://' + output_bucket + '/Store/Working',
            's3://' + output_bucket + '/StoreTraffic'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_storetraffic_delivery(self):
        step_name = 'StoreTrafficDelivery'
        script_name = 'StoreTrafficDelivery.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['delivery']

        script_args = [
            's3://' + input_bucket + '/StoreTraffic/working',
            's3://' + output_bucket + '/WT_STORE_TRAFFIC'
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
