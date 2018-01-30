"""Contains the class StepBuilderStore.
Builds EMR Steps for store files.
"""


class StepBuilderStore(object):
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
        discovery_paths = self._build_discovery_paths(
            self.buckets['discovery_regular'])

        refined_paths = self._build_refined_paths(
            self.buckets['refined_regular'])

        steps = [
            self._build_step_csv_to_parquet(),
            self._build_step_store_refinery(discovery_paths),
            self._build_step_att_dealer_refinery(discovery_paths),
            self._build_step_store_dealer_assn_refinery(discovery_paths),
            self._build_step_tech_brand_hierarchy(refined_paths),
            self._build_step_dealer_code_delivery(refined_paths),
            self._build_step_store_dealer_association_delivery(refined_paths),
            self._build_step_dim_store_delivery(refined_paths)
        ]

        return steps

    # ============================================
    # Step Definitions
    # ============================================

    def _build_step_csv_to_parquet(self):
        step_name = 'CSVToParquet'
        script_name = 'LocationMasterRQ4Parquet.py'
        bucket = self.buckets['discovery_regular']

        filters = [
            'store/Location',
            'store/BAE',
            'store/DealerCodes',
            'store/Multi',
            'store/SpringMobile',
            'store/DTV'
        ]
        raw_file_list = self._build_raw_file_list(filters)

        script_args = [
            raw_file_list[0],
            raw_file_list[1],
            raw_file_list[2],
            raw_file_list[3],
            raw_file_list[4],
            raw_file_list[5],
            's3://' + bucket
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_store_refinery(self, discovery_paths):
        step_name = 'StoreRefinery'
        script_name = 'DimStoreRefined.py'
        input_bucket = self.buckets['discovery_regular']
        output_bucket = self.buckets['refined_regular']

        script_args = [
            's3://' + input_bucket,
            's3://' + output_bucket
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_att_dealer_refinery(self, discovery_paths):
        step_name = 'ATTDealerRefinery'
        script_name = 'ATTDealerCodeRefined.py'
        input_bucket = self.buckets['discovery_regular']
        output_bucket = self.buckets['refined_regular']
        script_args = [
            's3://' + input_bucket,
            's3://' + output_bucket
        ]
        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_store_dealer_assn_refinery(self, discovery_paths):
        step_name = 'StoreDealerAssociationRefinery'
        script_name = 'StoreDealerCodeAssociationRefine.py'
        input_bucket = self.buckets['discovery_regular']
        output_bucket = self.buckets['refined_regular']

        script_args = [
            's3://' + input_bucket,
            's3://' + output_bucket
        ]
        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_tech_brand_hierarchy(self, refined_paths):
        step_name = 'TechBrandHierarchy'
        script_name = 'DimStoreHierDelivery.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['delivery']

        script_args = [
            's3://' + input_bucket,
            's3://' + output_bucket
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_dealer_code_delivery(self, refined_paths):
        step_name = 'DealerCodeDelivery'
        script_name = 'ATTDealerCodeDelivery.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['delivery']

        script_args = [
            's3://' + input_bucket,
            's3://' + output_bucket + '/WT_ATT_DELR_CDS/Current'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_store_dealer_association_delivery(self, refined_paths):
        step_name = 'StoreDealerAssociationDelivery'
        script_name = 'StoreDealerCodeAssociationDelivery.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['delivery']
        script_args = [
            's3://' + input_bucket,
            's3://' + output_bucket
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_dim_store_delivery(self, refined_paths):
        step_name = 'DimStoreDelivery'
        script_name = 'DimStoreDelivery.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['delivery']

        # tech_brand_op_name = 's3://' + bucket + '/Store/Store_Hier/Current/'

        script_args = [
            's3://' + input_bucket,
            's3://' + output_bucket
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    # ============================================
    # Support Methods
    # ============================================

    def _build_discovery_paths(self, bucket):
        return {
            'location': self._build_path(bucket, 'store', 'location'),
            'dealer': self._build_path(bucket, 'store', 'dealer'),
            'multi': self._build_path(bucket, 'store', 'multi_tracker'),
            'spring': self._build_path(bucket, 'store', 'spring_mobile'),
            'bae': self._build_path(bucket, 'store', 'bae'),
            'dtv_location': self._build_path(bucket, 'store', 'dtv_location')
        }

    def _build_refined_paths(self, bucket):
        return {
            'att_dealer': self._build_path(
                bucket, 'Store', 'ATTDealerCodeRefine'),
            'association': self._build_path(
                bucket, 'Store', 'StoreDealerAssociationRefine'),
            'store_refine': self._build_path(
                bucket, 'Store', 'StoreRefined')
        }

    def _build_path(self, bucket, domain, name):
        path_parts = [
            's3://' + bucket,
            domain,
            self.date_parts['year'],
            self.date_parts['month'],
            name + self.date_parts['time'],
            '*.parquet'
        ]

        return '/'.join(path_parts)

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
    
