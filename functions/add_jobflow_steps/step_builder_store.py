"""Contains the class StepBuilderStore.
Builds EMR Steps for Store files.
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

        steps = [
            self._build_step_csv_to_parquet(),
            self._build_step_employee_refinery(),
            self._build_step_employee_delivery(),
            self._build_step_csv_to_parquet_store(),
            self._build_step_csv_to_parquet_ATT_Dealer_code(),
            self._build_step_ATT_Dealer_code_refinery(),
            self._build_step_ATT_Dealer_code_delivery(),
            self._build_step_store_refinery(),
            self._build_step_store_dlcode_assoc_refinery(),
            self._build_step_store_dlcode_assoc_delivery(),
            self._build_step_store_delivery(),
            self._build_step_store_hier_delivery(),
            self._build_step_store_management_hier_delivery(),
            self._build_step_emp_store_assoc_refinery(),
            self._build_step_emp_store_assoc_delivery()
        ]

        return steps

    # ============================================
    # Step Definitions
    # ============================================

    def _build_step_csv_to_parquet(self):
        step_name = 'CSVToParquetEmployee'
        script_name = 'Dimensions/EmployeeCSVToParquet.py'
        input_bucket = self.buckets['raw_hr_pii']
        output_bucket = self.buckets['discovery_hr_pii']

        script_args = [
            's3://' + input_bucket + '/Employee/Working',
            's3://' + output_bucket + '/Employee/'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_employee_refinery(self):
        step_name = 'EmployeeRefinery'
        script_name = 'Dimensions/EmployeeDiscoveryToRefined.py'
        input_bucket = self.buckets['discovery_hr_pii']
        output_bucket = self.buckets['refined_regular']

        script_args = [
            's3://' + input_bucket + '/Employee/Working',
            's3://' + output_bucket + '/Employee/Working'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_employee_delivery(self):
        step_name = 'EmployeeDelivery'
        script_name = 'Dimensions/EmployeeRefinedToDelivery.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['delivery_regular']

        script_args = [
            's3://' + input_bucket + '/Employee/Working',
            's3://' + output_bucket + '/WT_EMP/'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_csv_to_parquet_store(self):
        step_name = 'CSVToParquetStore'
        script_name = 'Dimensions/DimStoreCSVToParquet.py'
        input_bucket = self.buckets['raw_regular']
        output_bucket = self.buckets['discovery_regular']
        error_bucket = self.buckets['data_processing_errors']

        script_args = [
            's3://' + input_bucket + '/Location/Working',
            's3://' + input_bucket + '/BAE/Working',
            's3://' + input_bucket + '/ATTDealerCodes/Working',
            's3://' + input_bucket + '/MultiTracker/Working',
            's3://' + input_bucket + '/SpringMobileStore/Working',
            's3://' + input_bucket + '/DTV/Working',
            's3://' + output_bucket + '/Store/Working',
            's3://' + error_bucket + '/Store'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_csv_to_parquet_ATT_Dealer_code(self):
        step_name = 'CSVToParquetATTDealerCode'
        script_name = 'Dimensions/ATTDealerCodeCSVToParquet.py'
        input_bucket = self.buckets['raw_regular']
        output_bucket = self.buckets['discovery_regular']

        script_args = [
            's3://' + output_bucket + '/ATTDealerCode',
            's3://' + input_bucket + '/ATTDealerCodes/Working/'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_ATT_Dealer_code_refinery(self):
        step_name = 'ATTDealerCodeRefinery'
        script_name = 'Dimensions/ATTDealerCodeRefined.py'
        input_bucket = self.buckets['discovery_regular']
        output_bucket = self.buckets['refined_regular']

        script_args = [
            's3://' + output_bucket + '/ATTDealerCode',
            's3://' + input_bucket + '/ATTDealerCode/Working/'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_ATT_Dealer_code_delivery(self):
        step_name = 'ATTDealerCodeDelivery'
        script_name = 'Dimensions/ATTDealerCodeDelivery.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['delivery_regular']

        script_args = [
            's3://' + output_bucket + '/WT_ATT_DELR_CDS',
            's3://' + input_bucket + '/ATTDealerCode/Working/'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_store_refinery(self):
        step_name = 'StoreRefinery'
        script_name = 'Dimensions/DimStoreRefined.py'
        input_bucket = self.buckets['discovery_regular']
        output_bucket = self.buckets['refined_regular']
        error_bucket = self.buckets['data_processing_errors']

        script_args = [
            's3://' + input_bucket + '/Store/Working',
            's3://' + output_bucket + '/Store/Working',
            's3://' + error_bucket + '/Store'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_store_dlcode_assoc_refinery(self):
        step_name = 'StoreDelaerCodeAssociationRefinery'
        script_name = 'Associations/StoreDealerCodeAssociationRefine.py'
        input_bucket = self.buckets['discovery_regular']
        output_bucket = self.buckets['refined_regular']
        error_bucket = self.buckets['data_processing_errors']

        script_args = [
            's3://' + input_bucket + '/StoreDealerAssociation/Working',
            's3://' + output_bucket + '/StoreDealerAssociation/Working',
            's3://' + error_bucket + '/StoreDealerAssociation'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_store_dlcode_assoc_delivery(self):
        step_name = 'StoreDealerCodeAssociationDelivery'
        script_name = 'Associations/StoreDealerCodeAssociationDelivery.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['delivery_regular']

        script_args = [
            's3://' + input_bucket + '/StoreDealerAssociation/Working',
            's3://' + output_bucket + '/WT_STORE_DELR_CD_ASSOC/Current'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_store_delivery(self):
        step_name = 'StoreDelivery'
        script_name = 'Dimensions/DimStoreDelivery.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['delivery_regular']

        script_args = [
            's3://' + input_bucket + '/Store/Working',
            's3://' + output_bucket + '/WT_STORE/Current'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_store_hier_delivery(self):
        step_name = 'StoreHierarchyDelivery'
        script_name = 'Dimensions/DimStoreHierDelivery.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['delivery_regular']

        script_args = [
            's3://' + input_bucket + '/Store/Working',
            's3://' + output_bucket + '/WT_STORE_HIER/Current'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_store_management_hier_delivery(self):
        step_name = 'StoreManagementHierarchyDelivery'
        script_name = 'Dimensions/DimStoreManagementHierDelivery.py'
        input_bucket_refined = self.buckets['refined_regular']
        input_bucket_discovery = self.buckets['discovery_regular']
        output_bucket = self.buckets['delivery_regular']
        error_bucket = self.buckets['data_processing_errors']

        script_args = [
            's3://' + input_bucket_refined + '/Store/Working',
            's3://' + input_bucket_discovery + '/Store/SpringMobileStore/Working',
            's3://' + output_bucket + '/WT_STORE_MGMT_HIER/Current',
            's3://' + error_bucket + '/Store'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_emp_store_assoc_refinery(self):
        step_name = 'EmpStoreAssocRefinery'
        script_name = 'Associations/EmpStoreAssociationDiscoveryToRefined.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['refined_regular']

        script_args = [
            's3://' + input_bucket + '/Employee/Working',
            's3://' + output_bucket + '/EmpStoreAssociation/',
            's3://' + output_bucket + '/EmpStoreAssociation/'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_emp_store_assoc_delivery(self):
        step_name = 'EmpStoreAssocDelivery'
        script_name = 'Associations/EmpStoreAsscociationRefinedToDelivery.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['delivery_regular']

        script_args = [
            's3://' + input_bucket + '/EmpStoreAssociation/Working/',
            's3://' + output_bucket + '/WT_EMP_STORE_ASSOC/'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    # ============================================
    # Support Methods
    # ============================================

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
