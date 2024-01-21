import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")
print('sys.path={}'.format(sys.path))

import unittest

from pytaskflow.models.Task import *

running_path = os.getcwd()
print('Current Working Path: {}'.format(running_path))

class TestFunctionKeysToLower(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print('-'*80)

    def test_keys_to_lower_1(self):
        d = {
            'a': 'AA',
            'Bb': {
                'c': 123,
                'dD': True
            },
            'ccC': [ 1,2,3, ]
        }
        df = keys_to_lower(data=d)
        print('df={}'.format(df))
        self.assertIsNotNone(df)
        self.assertIsInstance(df, dict)
        self.assertEqual(len(df), len(d))
        self.assertTrue('a' in df)
        self.assertTrue('bb' in df)
        self.assertTrue('ccc' in df)
        bb = df['bb']
        self.assertTrue('c' in bb)
        self.assertTrue('dd' in bb)


if __name__ == '__main__':
    unittest.main()
