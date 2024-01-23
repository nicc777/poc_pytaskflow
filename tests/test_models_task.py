import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")
print('sys.path={}'.format(sys.path))

import unittest

from pytaskflow.models.Task import *

running_path = os.getcwd()
print('Current Working Path: {}'.format(running_path))


class TestLogger(LoggerWrapper):

    def __init__(self):
        super().__init__()
        self.info_lines = list()
        self.warn_lines = list()
        self.debug_lines = list()
        self.critical_lines = list()
        self.error_lines = list()

    def info(self, message: str):
        self.info_lines.append('[LOG] INFO: {}'.format(message))

    def warn(self, message: str):
        self.warn_lines.append('[LOG] WARNING: {}'.format(message))

    def warning(self, message: str):
        self.warn_lines.append('[LOG] WARNING: {}'.format(message))

    def debug(self, message: str):
        self.debug_lines.append('[LOG] DEBUG: {}'.format(message))

    def critical(self, message: str):
        self.critical_lines.append('[LOG] CRITICAL: {}'.format(message))

    def error(self, message: str):
        self.error_lines.append('[LOG] ERROR: {}'.format(message))

    def reset(self):
        self.info_lines = list()
        self.warn_lines = list()
        self.debug_lines = list()
        self.critical_lines = list()
        self.error_lines = list()


class TestFunctionKeysToLower(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
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
        print()
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
        print()
        print('-'*80)
        self.logger = TestLogger()

    def tearDown(self):
        for line in self.logger.info_lines:
            print(line)
        for line in self.logger.warn_lines:
            print(line)
        for line in self.logger.debug_lines:
            print(line)
        for line in self.logger.critical_lines:
            print(line)
        for line in self.logger.error_lines:
            print(line)

    def test_task_basic_init_minimal_1(self):
        t = Task(kind='TestKind', version='v1', spec={'field1': 'value1'}, metadata=dict(), logger=self.logger)
        self.assertIsNotNone(t)
        self.assertIsInstance(t, Task)
        self.assertEqual(t.kind, 'TestKind')

        match_found = False
        for line in self.logger.info_lines:
            if 'registered. Task checksum:' in line:
                match_found = True
        self.assertTrue(match_found)

    def test_task_basic_init_minimal_with_name_1(self):
        t = Task(kind='TestKind', version='v1', spec={'field1': 'value1'}, metadata={'name': 'test1'}, logger=self.logger)
        self.assertIsNotNone(t)
        self.assertIsInstance(t, Task)
        self.assertEqual(t.kind, 'TestKind')

        match_found = False
        for line in self.logger.info_lines:
            if 'registered. Task checksum:' in line:
                match_found = True
        self.assertTrue(match_found)

        match_1 = t.task_match_name(name='test1')
        match_2 = t.task_match_name(name='test2')

        self.assertTrue(match_1)
        self.assertFalse(match_2)


if __name__ == '__main__':
    unittest.main()
