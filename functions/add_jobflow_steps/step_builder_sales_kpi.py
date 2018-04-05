"""Contains the class StepBuildersalesKPI and details.
Builds EMR Steps for salesKPI files.
"""


class StepBuilderSalesKPI(object):
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
        # discovery_paths = self._build_discovery_paths(
        #    self.buckets['discovery_regular'])

        # refined_paths = self._build_refined_paths(
        #    self.buckets['refined_regular'])

        steps = [
            self._build_step_salesKPIlist(),
            self._build_step_salesDetails_Refinery(),
            self._build_step_salesDetails_Delivery()
        ]

        return steps

    # ============================================
    # Step Definitions
    # ============================================

    def _build_step_salesKPIlist(self):
        step_name = 'SalesKPIList'
        script_name = 'Facts/TB_KPI_LIST.py'
        input_bucket = self.buckets['raw_regular']
        output_bucket = self.buckets['delivery_regular']

        script_args = [
            's3://' + input_bucket + '/KPI_Testing/Working/TB_KPI_LIST.xlsx',
            's3://' + output_bucket + '/WT_TB_KPI_LIST'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_salesDetails_Refinery(self):
        step_name = 'SalesKPIRefined'
        script_name = 'Facts/SalesKPIRefined.py'
        input_bucket = self.buckets['refined_regular']
        raw_bucket = self.buckets['raw_regular']
        KPI_FILE = '/KPI_Testing/Working/TB_KPI_LIST.xlsx'

        script_args = [
            's3://' + input_bucket + '/SalesDetails/Working/',
            's3://' + input_bucket + '/Product/Working/',
            's3://' + input_bucket + '/ProductCategory/Working/',
            's3://' + input_bucket + '/StoreTransactionAdjustment/Working/',
            's3://' + input_bucket + '/Store/Working/',
            's3://' + input_bucket + '/ATTSalesActual/Working/',
            's3://' + input_bucket + '/StoreTraffic/Working/',
            's3://' + input_bucket + '/StoreRecruitingHeadcount/Working',
            's3://' + input_bucket + '/EmployeeGoal/Working/',
            's3://' + input_bucket + '/Employee/Working/',
            's3://' + input_bucket + '/EmpStoreAssociation/Working/',
            's3://' + input_bucket + '/SalesLeads/Working/',
            's3://' + input_bucket + '/StoreCustomerExperience/Working/',
            's3://' + raw_bucket + KPI_FILE,
            's3://' + input_bucket + '/SalesKPI/'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_salesDetails_Delivery(self):
        step_name = 'SalesKPIDelivery'
        script_name = 'Facts/SalesKPIDelivery.py'
        input_bucket = self.buckets['refined_regular']
        output_bucket = self.buckets['delivery_regular']

        script_args = [
            's3://' + output_bucket + '/WT_TB_SALES_KPIS',
            's3://' + input_bucket + '/SalesKPI/Working'
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
