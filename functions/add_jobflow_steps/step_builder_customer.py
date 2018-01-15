""" Contains the class StepBuilderCustomer.

Builds EMR Steps for customer files.
"""


class StepBuilderCustomer(object):
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
            self._build_dummy_step()
        ]

        return steps

    # ============================================
    # Step Definitions
    # ============================================

    def _build_dummy_step(self):
        step_name = 'Dummy'
        script_name = 'Dummy.py'

        script_args = []

        return self.step_factory.create(step_name, script_name, script_args)
