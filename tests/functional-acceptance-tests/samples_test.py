import unittest

class SamplesTest(unittest.TestCase):

    def test_self(self):
        """
        TESTED!
        :return:
        """
        self.assertEqual('testme'.capitalize(), 'Testme')

if __name__ == '__main__':
    unittest.main()