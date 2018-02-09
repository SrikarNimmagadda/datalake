"""Contains the class StepBuilderStoreCustomerExperience.
Builds EMR Steps for Store Customer Experience files.
"""


class StepBuilderPayrollandCompliance(object):
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
            self._build_step_csv_to_parquet_store_customer_experience(),
            self._build_step_store_customer_experience_refinery(),
            self._build_step_store_customer_experience_delivery(),
            self._build_step_csv_to_parquet_store_recruiting_headcount(),
            self._build_step_store_recruiting_headcount_refined(),
            self._build_step_store_recruiting_headcount_delivery(),
            self._build_step_csv_to_parquet_emp_cnc_training(),
            self._build_step_emp_cnc_training_refined(),
            self._build_step_emp_cnc_training_delivery()
        ]

        return steps

    # ============================================
    # Step Definitions
    # ============================================

    def _build_step_csv_to_parquet_store_customer_experience(self):
        step_name = 'CSVToParquetStoreCustomerExperience'
        script_name = 'StoreCustExpCSVToParquet.py'
        input_bucket = self.buckets['raw_regular']
        output_bucket = self.buckets['discovery_regular']

        script_args = [

            's3://' + output_bucket,
            's3://' + input_bucket + '/StoreCustomerExperience'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_store_customer_experience_refinery(self):
        step_name = 'StoreCustomerExperienceRefined'
        script_name = 'StoreCustExpDiscoveryToRefined.py'
        input_bucket = self.buckets['discovery_regular']
        output_bucket = self.buckets['refined_regular']

        script_args = [
            's3://' + output_bucket,
            's3://' + input_bucket + 'StoreCustomerExperience/Working'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_store_customer_experience_delivery(self):
        step_name = 'StoreCustomerExperienceDelivery'
        script_name = 'StoreCustExpRefinedToDelivery.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['delivery']

        script_args = [
            's3://' + output_bucket + '/WT_STORE_CUST_EXPRC',
            's3://' + input_bucket + '/StoreCustomerExperience/Working'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_csv_to_parquet_store_recruiting_headcount(self):
        step_name = 'CSVToParquetStoreRecruitingHeadcount'
        script_name = 'StoreRecHeadcountCSVToParquet.py'
        input_bucket = self.buckets['raw_regular']
        output_bucket = self.buckets['discovery_regular']

        script_args = [
            's3://' + output_bucket,
            's3://' + input_bucket + '/StoreRecruitingHeadcount'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_store_recruiting_headcount_refined(self):
        step_name = 'StoreRecruitingHeadcountRefined'
        script_name = 'StoreRecHeadcountDiscoveryToRefined.py'
        input_bucket = self.buckets['discovery_regular']
        output_bucket = self.buckets['refined_regular']

        script_args = [
            's3://' + output_bucket,
            's3://' + input_bucket + '/StoreRecruitingHeadcount/Working'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_store_recruiting_headcount_delivery(self):
        step_name = 'StoreRecruitingHeadcountDelivery'
        script_name = 'StoreRecHeadcountRefinedToDelivery.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['delivery']

        script_args = [
            's3://' + output_bucket + '/WT_STORE_RCRTING_HDCT',
            's3://' + input_bucket + '/StoreRecruitingHeadcount/Working'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_csv_to_parquet_emp_cnc_training(self):
        step_name = 'CSVToParquetEMPCNCTraining'
        script_name = 'EmpCNCRawToDiscoveryt.py'
        input_bucket = self.buckets['raw_regular']
        output_bucket = self.buckets['discovery_regular']

        script_args = [
            's3://' + input_bucket + '/EmpCNCTraining/',
            's3://' + output_bucket + '/EmployeeCnCTraining/'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_emp_cnc_training_refined(self):
        step_name = 'EmployeeCnCTrainingRefined'
        script_name = 'EmpCNCDiscoverytoRefined.py'
        input_bucket = self.buckets['discovery_regular']
        output_bucket = self.buckets['refined_regular']

        script_args = [
            's3://' + input_bucket + '/EmployeeCnCTraining/Working/',
            's3://' + output_bucket + '/StoreDealerAssociation/Working/',
            's3://' + output_bucket + '/Employee/working/',
            's3://' + output_bucket + '/ATTDealerCode/Working/',
            's3://' + output_bucket + '/Store/Working/',
            's3://' + output_bucket + '/EmployeeCNCTrng/'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_emp_cnc_training_delivery(self):
        step_name = 'EMPCNCTrainingDelivery'
        script_name = 'EmployeeCNCTrainingRefinedToDelivery.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['delivery']

        script_args = [
            's3://' + input_bucket + '/EmployeeCNCTrng/Working/',
            's3://' + output_bucket + '/WT_EMP_C_C_TRAING/Current',
            's3://' + output_bucket + '/WT_EMP_C_C_TRAING/Previous'
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
