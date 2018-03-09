"""Run all tests."""

# Need this file to execute tests because if we just run unittest from
# the command line, the exit status is always 0, even when tests fail.abs
# To get around that limitation, we are setting the exit code in this
# script.

import sys
import unittest


if __name__ == '__main__':
    test_suite = unittest.defaultTestLoader.discover('.', '*_test.py')
    test_runner = unittest.TextTestRunner(resultclass=unittest.TextTestResult)
    result = test_runner.run(test_suite)
    sys.exit(not result.wasSuccessful())
