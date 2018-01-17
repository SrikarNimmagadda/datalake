""" Contains the class StepBuilderStore.
Builds EMR Steps for employee files.
"""


class StepBuilderEmployee(object):
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
            self._build_step_employee_refinery(discovery_paths),
            self._build_step_employee_delivery(refined_paths)
        ]

        return steps

    # ============================================
    # Step Definitions
    # ============================================

    def _build_step_csv_to_parquet(self):
        step_name = 'CSVToParquet'
        script_name = 'EmployeeCSVToParquet.py'
        bucket = self.buckets['discovery_regular']

              script_args = [
            's3://tb-us-east-1-dev-raw-regular/Employee/' ,
            's3://' + bucket + '/Employee/working/',
            self.date_parts['time']
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    def _build_step_employee_refinery(self, discovery_paths):
        step_name = 'EmployeeRefinery'
        script_name = 'EmployeeDiscoveryToRefined.py'
        bucket = self.buckets['refined_regular']

        script_args = [
            discovery_paths['Employee'],
            's3://' + bucket + '/Employee/working/',
            self.date_parts['time']
        ]

        return self.step_factory.create(step_name, script_name, script_args)

   

    def _build_step_employee_delivery(self, refined_paths):
        step_name = 'EmployeeDelivery'
        script_name = 'EmployeeRefinedToDelivery.py '
        bucket = self.buckets['delivery_regular']

       
        script_args = [
            refined_paths['Employee'],
            's3://' + bucket + '/WT_EMP/Current/'
        ]

        return self.step_factory.create(step_name, script_name, script_args)

    # ============================================
    # Support Methods
    # ============================================

    def _build_discovery_paths(self, bucket):
        return {
            'employee': self._build_path(bucket, 'Employee', 'working')
            
        }

    def _build_refined_paths(self, bucket):
        return {
            'employee': self._build_path(
                bucket, 'Employee', 'working')
           
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
                file_list.append(file)

        return file_list