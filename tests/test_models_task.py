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


class TestObjectInstanceGlobalKeyValueStore(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print('-'*80)

    def test_global_key_value_store_basic(self):
        self.assertIsInstance(global_key_value_store, KeyValueStore)
        global_key_value_store.save(key='test_key_1', value='test_value')
        global_key_value_store.save(key='test_key_2', value=123)
        global_key_value_store.save(key='test_key_3', value=True)
        self.assertEqual(len(global_key_value_store.store), 3)
        self.assertTrue('test_key_1' in global_key_value_store.store)
        self.assertTrue('test_key_2' in global_key_value_store.store)
        self.assertTrue('test_key_3' in global_key_value_store.store)
        self.assertIsInstance(global_key_value_store.store['test_key_1'], str)
        self.assertIsInstance(global_key_value_store.store['test_key_2'], int)
        self.assertIsInstance(global_key_value_store.store['test_key_3'], bool)


class TestClassTask(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print('-'*80)

    def test_task_basic_init_minimal_1(self):
        t = Task(kind='TestKind', version='v1', spec={'field1': 'value1'}, metadata=dict())
        self.assertIsNotNone(t)
        self.assertIsInstance(t, Task)
        self.assertEqual(t.kind, 'TestKind')


if __name__ == '__main__':
    unittest.main()
