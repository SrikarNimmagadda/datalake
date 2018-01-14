class step_builder(object):

    def __init__(self, s3, buckets, now):
        self.s3_client = s3
        self.buckets = buckets

        self.date_parts = {
            'time': now.strftime('%Y%m%d%H%M'),
            'year': now.strftime('%Y'),
            'month': now.strftime('%m')
        }

        self.discovery_paths = self.build_discovery_paths(
            self.buckets['discovery_regular'])

        self.refined_paths = self.build_refined_paths(
            self.buckets['refined_regular'])

    def BuildSteps(self):
        steps = [
            self.BuildStepLocationMasterRQ4ToParquet(),
            self.BuildStepDimStoreRefined(),
            self.BuildStepATTDealerCodeRefined(),
            self.BuildStepStoreDealerCodeAssociationRefine(),
            self.BuildStepDimTechBrandHierarchy(),
            self.BuildStepAttDealerCodeDelivery(),
            self.BuildStepStoreDealerCodeAssociationDelivery(),
            self.BuildStepDimStoreDelivery()
        ]

        return steps

    def build_discovery_paths(self, bucket):
        return {
            'location': self.build_path(bucket, 'Store', 'location'),
            'dealer': self.build_path(bucket, 'Store', 'Dealer'),
            'multi': self.build_path(bucket, 'Store', 'multiTracker'),
            'spring': self.build_path(bucket, 'Store', 'springMobile'),
            'bae': self.build_path(bucket, 'Store', 'BAE')
        }

    def build_refined_paths(self, bucket):
        return {
            'att_dealer': self.build_path(bucket, 'Store', 'ATTDealerCodeRefine'),
            'association': self.build_path(bucket, 'Store', 'StoreDealerAssociationRefine'),
            'store_refine': self.build_path(bucket, 'Store', 'StoreRefined')
        }

    def build_path(self, bucket, domain, name):
        return 's3://' + bucket + '/' + domain + '/' + \
            self.date_parts['year'] + '/' + self.date_parts['month'] + '/' + \
            name + self.date_parts['time'] + '/*.parquet'

    def find_source_file(self, bucketname, filter_prefix):
        bucket = self.s3_client.Bucket(bucketname)
        data = [obj for obj in list(bucket.objects.filter(
            Prefix=filter_prefix)) if obj.key != filter_prefix]

        length = len(data)

        # takes care of the case when no files match the filter.
        # will probably cause problems further downstream unless
        # the case for an empty filename is checked for
        if length == 0:
            return None

        # appends the last matched item to the list
        # Are we assuming that all previous files have been processed?
        # That seems like a dangerous assumption
        # I think this is where the dynamo table must come in to track what has been processed.
        i = 0
        for obj in data:
            i = i + 1
            if i == length:
                return "s3://" + bucketname + '/' + obj.key

    def build_raw_file_list(self):
        file_list = []

        filters = [
            'Store/locationMasterList',
            'Store/BAE',
            'Store/dealer',
            'Store/multiTracker',
            'Store/springMobile'
        ]

        for filter in filters:
            file = self.find_source_file(self.buckets['raw_regular'], filter)
            if file == None:
                # since passing as an argument to emr, can't be empty string. Need to use a proxy null value.
                file_list.append('nofile')
            else:
                file_list.append(file)

        return file_list

    def BuildStepLocationMasterRQ4ToParquet(self):
        raw_file_list = self.build_raw_file_list()

        args = [
            raw_file_list[0],
            raw_file_list[1],
            raw_file_list[2],
            raw_file_list[3],
            raw_file_list[4],
            's3://' + self.buckets['discovery_regular'] + '/Store',
            self.date_parts['time']
        ]

        step = self.CreateStep('CSVToParquet', 'LocationMasterRQ4Parquet.py', args)

        return step

    def BuildStepDimStoreRefined(self):
        script_args = [
            self.discovery_paths['location'],
            self.discovery_paths['bae'],
            self.discovery_paths['dealer'],
            self.discovery_paths['spring'],
            self.discovery_paths['multi'],
            's3://' + self.buckets['refined_regular'] + '/Store',
            self.date_parts['time']
        ]

        step = self.CreateStep('StoreRefinery', 'DimStoreRefined.py', script_args)

        return step

    def BuildStepATTDealerCodeRefined(self):
        script_args = [
            self.discovery_paths['dealer'],
            's3://' + self.buckets['refined_regular'] + '/Store',
            self.date_parts['time']
        ]

        step = self.CreateStep('ATTDealerRefinery', 'ATTDealerCodeRefine.py', script_args)

        return step

    def BuildStepStoreDealerCodeAssociationRefine(self):
        script_args = [
            self.discovery_paths['dealer'],
            's3://' + self.buckets['refined_regular'] + '/Store',
            self.date_parts['time']
        ]

        step = self.CreateStep('StoreDealerAssociationRefinery', 'StoreDealerCodeAssociationRefine.py', script_args)

        return step

    def BuildStepDimTechBrandHierarchy(self):
        script_args = [
            self.refined_paths['store_refine'],
            self.refined_paths['att_dealer'],
            's3://' + self.buckets['delivery_regular'] + '/Store/Store_Hier/Current/'

        step = self.CreateStep('TechBrandHierarchy', 'DimTechBrandHierarchy.py', script_args)

        return step

    def BuildStepAttDealerCodeDelivery(self):
        script_args = [
            self.refined_paths['att_dealer'],
            's3://' + self.buckets['delivery_regular'] + '/WT_ATT_DELR_CDS/Current'
        ]

        step = self.CreateStep('DealerCodeDelivery', 'ATTDealerCodeDelivery.py', script_args)

        return step

    def BuildStepStoreDealerCodeAssociationDelivery(self):
        script_args = [
            self.refined_paths['association'],
            's3://' + self.buckets['delivery_regular'] + '/WT_STORE_DELR_CD_ASSOC/Current/'

        step = self.CreateStep('StoreDealerAssociationDelivery', 'StoreDealerCodeAssociationDelivery.py', script_args)

        return step

    def BuildStepDimStoreDelivery(self):
        tech_brand_op_name = 's3://' + self.buckets['delivery-regular'] + '/Store/Store_Hier/Current/'

        script_args = [
            self.refined_paths['att_dealer'],
            self.refined_paths['association'],
            self.refined_paths['store_refine'],
            tech_brand_op_name,
            's3://' + self.buckets['delivery_regular'] + '/WT_STORE/Current/'
        ]

        step = self.CreateStep('DimStoreDelivery', 'DimStoreDelivery.py', script_args)

        return step

    def CreateStep(self, stepName, scriptName, scriptArgs):
        args = BuildStepArgs(scriptName, scriptArgs)
        
        step = {
            'Name': stepName,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 's3://elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': args
            }
        }

        return step

    def BuildStepArgs(self, scriptName, scriptArgs):
        args = [
            '/usr/bin/spark-submit',
            '--jars',
            's3://' + self.buckets['code'] + '/EMRJars/spark-csv_2.11-1.5.0.jar,s3://' + self.buckets['code'] + '/EMRJars/spark-excel_2.11-0.8.6.jar',
            's3://' + self.buckets['code'] + '/EMRScripts/' + scriptName

        return args + scriptArgs;

