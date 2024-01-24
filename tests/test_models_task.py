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
        spec_test_field_name = 'TestField1'
        t = Task(kind='TestKind', version='v1', spec={spec_test_field_name: 'value1'}, metadata=dict(), logger=self.logger)
        self.assertIsNotNone(t)
        self.assertIsInstance(t, Task)
        self.assertEqual(t.kind, 'TestKind')

        match_found = False
        for line in self.logger.info_lines:
            if 'registered. Task checksum:' in line:
                match_found = True
        self.assertTrue(match_found)

        task_contexts = t.task_contexts
        self.assertIsNotNone(task_contexts)
        self.assertIsInstance(task_contexts, list)
        self.assertEqual(len(task_contexts), 1)
        self.assertTrue('default' in task_contexts)

        task_metadata = t.metadata
        self.assertIsNotNone(task_metadata)
        self.assertIsInstance(task_metadata, dict)
        self.assertEqual(len(task_metadata), 0)

        task_spec = t.spec
        self.assertIsNotNone(task_spec)
        self.assertIsInstance(task_spec, dict)
        self.assertEqual(len(task_spec), 1)
        self.assertTrue(spec_test_field_name.lower() in task_spec)

        task_selector_register = t.selector_register
        self.assertIsNotNone(task_selector_register)
        self.assertIsInstance(task_selector_register, dict)
        self.assertEqual(len(task_selector_register), 0)

        task_annotations = t.annotations
        self.assertIsNotNone(task_annotations)
        self.assertIsInstance(task_annotations, dict)
        self.assertEqual(len(task_annotations), 0)

        task_dependencies = t.task_dependencies
        self.assertIsNotNone(task_dependencies)
        self.assertIsInstance(task_dependencies, dict)
        self.assertEqual(len(task_dependencies), 2)
        self.assertTrue('NamedTasks' in task_dependencies)
        self.assertTrue('Labels' in task_dependencies)
        task_dependencies_named_tasks = task_dependencies['NamedTasks']
        self.assertIsNotNone(task_dependencies_named_tasks)
        self.assertIsInstance(task_dependencies_named_tasks, list)
        self.assertEqual(len(task_dependencies_named_tasks), 0)
        task_dependencies_labels = task_dependencies['Labels']
        self.assertIsNotNone(task_dependencies_labels)
        self.assertIsInstance(task_dependencies_labels, list)
        self.assertEqual(len(task_dependencies_labels), 0)

        data = dict(t)
        self.assertIsNotNone(data)
        self.assertIsInstance(data, dict)
        self.assertEqual(len(data), 3)
        self.assertTrue('kind' in data)
        self.assertTrue('version' in data)
        self.assertTrue('spec' in data)

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

    def test_task_basic_init_minimal_with_no_name_produces_debug_message_when_lookup_by_name_is_done(self):
        t = Task(kind='TestKind', version='v1', spec={'field1': 'value1'}, metadata=dict(), logger=self.logger)
        match_1 = t.task_match_name(name='test1')
        self.assertFalse(match_1)
        log_message_match_found = False
        for line in self.logger.debug_lines:
            if 'This task has no name defined and a match can therefore not be made' in line:
                log_message_match_found = True
        self.assertTrue(log_message_match_found)

    def test_task_basic_init_minimal_with_name_and_labels_1(self):
        t = Task(kind='TestKind', version='v1', spec={'field1': 'value1'}, metadata={'name': 'test1', 'labels': {'label1': 'labelvalue1', 'label2': 'labelvalue2'}}, logger=self.logger)
        self.assertIsNotNone(t)
        self.assertIsInstance(t, Task)
        self.assertEqual(t.kind, 'TestKind')

        match_1a = t.task_match_name(name='test1')
        match_1b = t.task_match_label(key='label1', value='labelvalue1')
        match_1c = t.task_match_label(key='label2', value='labelvalue2')
        match_2a = t.task_match_name(name='test2')
        match_2b = t.task_match_label(key='label1', value='labelvalue2')
        match_2c = t.task_match_label(key='label2', value='labelvalue1')
        match_2d = t.task_match_label(key='label3', value='labelvalue3')
        
        self.assertTrue(match_1a)
        self.assertTrue(match_1b)
        self.assertTrue(match_1c)
        self.assertFalse(match_2a)
        self.assertFalse(match_2b)
        self.assertFalse(match_2c)
        self.assertFalse(match_2d)

    def test_task_basic_init_minimal_with_annotations_1(self):
        custom_annotation_value = 'customvalue1'
        custom_annotation_name = 'thirdparty/annotation/name1'
        t = Task(
            kind='TestKind',
            version='v1',
            spec={'field1': 'value1'},
            metadata={
                'name': 'test1',
                'annotations': {
                    'contexts': 'c1,c2',
                    'dependency/name': 'name1,name2',
                    'dependency/label/labelname1': 'labelvalue1',
                    custom_annotation_name: custom_annotation_value,
                }
            },
            logger=self.logger
        )

        custom_annotations = t.annotations
        self.assertIsNotNone(custom_annotations)
        self.assertIsInstance(custom_annotations, dict)
        self.assertEqual(len(custom_annotations), 1, 'custom_annotations: {}'.format(custom_annotations))
        self.assertTrue(custom_annotation_name in custom_annotations)
        self.assertEqual(custom_annotations[custom_annotation_name], custom_annotation_value)

        contexts = t.task_contexts
        self.assertIsNotNone(contexts)
        self.assertIsInstance(contexts, list)
        self.assertEqual(len(contexts), 2, 'contexts: {}'.format(contexts))
        self.assertTrue('c1' in contexts)
        self.assertTrue('c2' in contexts)

        dependencies = t.task_dependencies
        self.assertIsNotNone(dependencies)
        self.assertIsInstance(dependencies, dict)
        self.assertEqual(len(dependencies), 2, 'dependencies: {}'.format(dependencies))
        self.assertTrue('NamedTasks' in dependencies)
        self.assertTrue('Labels' in dependencies)
        
        dependencies_labels = dependencies['Labels']
        self.assertIsNotNone(dependencies_labels)
        self.assertIsInstance(dependencies_labels, list)
        self.assertEqual(len(dependencies_labels), 1, 'dependencies_labels: {}'.format(dependencies_labels))
        dependency_label_1 = dependencies_labels[0]
        self.assertIsNotNone(dependency_label_1)
        self.assertIsInstance(dependency_label_1, dict)
        self.assertEqual(len(dependency_label_1), 1, 'dependency_label_1: {}'.format(dependency_label_1))
        self.assertTrue('dependency/label/labelname1' in dependency_label_1, 'dependency_label_1: {}'.format(dependency_label_1))
        self.assertEqual(dependency_label_1['dependency/label/labelname1'], 'labelvalue1')

        dependencies_names_of_tasks = dependencies['NamedTasks']
        self.assertIsNotNone(dependencies_names_of_tasks)
        self.assertIsInstance(dependencies_names_of_tasks, list)
        self.assertEqual(len(dependencies_names_of_tasks), 2, 'dependencies_names_of_tasks: {}'.format(dependencies_names_of_tasks))
        self.assertTrue('name1' in dependencies_names_of_tasks)
        self.assertTrue('name2' in dependencies_names_of_tasks)


class Processor1(TaskProcessor):

    def __init__(self):
        super().__init__(kind='Processor1', kind_versions=['v1'], supported_commands=['command1', 'command2'], logger=TestLogger())

    def process_task(self, task: Task, command: str, context: str='default', key_value_store: KeyValueStore=KeyValueStore())->KeyValueStore:
        self.logger.info('[Processor1]: {}'.format('-'*80))
        self.logger.info('[Processor1]: Processing task_id "{}"'.format(task.task_id))
        self.logger.info('[Processor1]:    Task Contexts "{}"'.format(task.task_contexts))
        self.logger.info('[Processor1]: command="{}"'.format(command))
        self.logger.info('[Processor1]: context="{}"'.format(context))
        can_process = True
        if task.kind != 'Processor1':
            self.logger.error('[Processor1]: Task kind "{}" mismatched and the task will NOT be processed'.format(task.kind))
            can_process = False
        if task.version not in self.versions:
            self.logger.error('[Processor1]: Task version "{}" is not supported and the task will NOT be processed'.format(task.version))
            can_process = False
        self.logger.info('[Processor1]: can_process={}'.format(can_process))
        key_value_store.save(key='Processor1:Processed:Success', value=can_process)
        self.logger.info('[Processor1]: {}'.format('='*80))
        return key_value_store


class Processor2(TaskProcessor):

    def __init__(self):
        super().__init__(kind='Processor2', kind_versions=['v1'], supported_commands=['command2'], logger=TestLogger())
    
    def process_task(self, task: Task, command: str, context: str='default', key_value_store: KeyValueStore=KeyValueStore())->KeyValueStore:
        self.logger.info('[Processor2]: {}'.format('-'*80))
        self.logger.info('[Processor2]: Processing task_id "{}"'.format(task.task_id))
        self.logger.info('[Processor2]:    Task Contexts "{}"'.format(task.task_contexts))
        self.logger.info('[Processor2]: command="{}"'.format(command))
        self.logger.info('[Processor2]: context="{}"'.format(context))
        can_process = True
        if task.kind != 'Processor2':
            self.logger.error('[Processor2]: Task kind "{}" mismatched and the task will NOT be processed'.format(task.kind))
            can_process = False
        if task.version not in self.versions:
            self.logger.error('[Processor2]: Task version "{}" is not supported and the task will NOT be processed'.format(task.version))
            can_process = False
        self.logger.info('[Processor2]: can_process={}'.format(can_process))
        key_value_store.save(key='Processor2:Processed:Success', value=can_process)
        self.logger.info('[Processor2]: {}'.format('='*80))
        return key_value_store


class TestClassTaskProcessor(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)
        self.key_value_store = KeyValueStore()

    def test_processor_1_init_with_successful_exec_of_a_task(self):
        p1 = Processor1()
        t1 = Task(
            kind='Processor1',
            version='v1',
            spec={'field1': 'value1'},
            metadata={
                'name': 'test1',
                'annotations': {
                    'contexts': 'c1,c2',
                }
            },
            logger=TestLogger()
        )
        self.key_value_store = p1.process_task(task=t1, command='command1', context='c1', key_value_store=self.key_value_store)
        self.assertIsNotNone(self.key_value_store)
        self.assertIsInstance(self.key_value_store, KeyValueStore)
        self.assertIsNotNone(self.key_value_store.store)
        self.assertIsInstance(self.key_value_store.store, dict)
        self.assertEqual(len(self.key_value_store.store), 1)
        self.assertTrue('Processor1:Processed:Success' in self.key_value_store.store)
        self.assertTrue(self.key_value_store.store['Processor1:Processed:Success'])

        p1_logger = p1.logger
        self.assertIsNotNone(p1_logger)
        self.assertTrue('[LOG] INFO: [Processor1]: can_process=True' in p1.logger.info_lines, 'info_lines={}'.format(p1.logger.info_lines))

    def test_processor_1_init_with_none_matching_task(self):
        p1 = Processor1()
        t1 = Task(
            kind='Processor2',  # !!!
            version='v1',
            spec={'field1': 'value1'},
            metadata={
                'name': 'test1',
                'annotations': {
                    'contexts': 'c1,c2',
                }
            },
            logger=TestLogger()
        )
        self.key_value_store = p1.process_task(task=t1, command='command1', context='c1', key_value_store=self.key_value_store)
        self.assertIsNotNone(self.key_value_store)
        self.assertIsInstance(self.key_value_store, KeyValueStore)
        self.assertIsNotNone(self.key_value_store.store)
        self.assertIsInstance(self.key_value_store.store, dict)
        self.assertEqual(len(self.key_value_store.store), 1)
        self.assertTrue('Processor1:Processed:Success' in self.key_value_store.store)
        self.assertFalse(self.key_value_store.store['Processor1:Processed:Success'])

        p1_logger = p1.logger
        self.assertIsNotNone(p1_logger)
        self.assertTrue('[LOG] INFO: [Processor1]: can_process=False' in p1.logger.info_lines, 'info_lines={}'.format(p1.logger.info_lines))

    def test_method_task_pre_processing_registration_check_with_valid_task_1(self):
        p1 = Processor1()
        t1 = Task(
            kind='Processor2',  # !!!
            version='v1',
            spec={'field1': 'value1'},
            metadata={
                'name': 'test1',
                'annotations': {
                    'contexts': 'c1,c2',
                }
            },
            logger=TestLogger()
        )
        expected_key = 'PROCESSING_TASK:{}:command1:c1'.format(t1.task_id)
        self.key_value_store = p1.task_pre_processing_registration_check(task=t1, command='command1', context='c1', key_value_store=self.key_value_store)
        self.assertIsNotNone(self.key_value_store)
        self.assertIsInstance(self.key_value_store, KeyValueStore)
        self.assertIsNotNone(self.key_value_store.store)
        self.assertIsInstance(self.key_value_store.store, dict)
        self.assertEqual(len(self.key_value_store.store), 1)
        self.assertTrue(expected_key in self.key_value_store.store)
        self.assertEqual(self.key_value_store.store[expected_key], 1)

    def test_method_task_pre_processing_registration_check_with_valid_task_and_execute_1(self):
        p1 = Processor1()
        t1 = Task(
            kind='Processor1',
            version='v1',
            spec={'field1': 'value1'},
            metadata={
                'name': 'test1',
                'annotations': {
                    'contexts': 'c1,c2',
                }
            },
            logger=TestLogger()
        )
        expected_key = 'PROCESSING_TASK:{}:command1:c1'.format(t1.task_id)
        self.key_value_store = p1.task_pre_processing_registration_check(task=t1, command='command1', context='c1', key_value_store=self.key_value_store, call_process_task_if_check_pass=True)
        self.assertIsNotNone(self.key_value_store)
        self.assertIsInstance(self.key_value_store, KeyValueStore)
        self.assertIsNotNone(self.key_value_store.store)
        self.assertIsInstance(self.key_value_store.store, dict)
        self.assertEqual(len(self.key_value_store.store), 2)
        self.assertTrue(expected_key in self.key_value_store.store)
        self.assertEqual(self.key_value_store.store[expected_key], 2, 'key_value_store={}'.format(self.key_value_store.store))
        self.assertTrue('Processor1:Processed:Success' in self.key_value_store.store)
        self.assertTrue(self.key_value_store.store['Processor1:Processed:Success'], 'key_value_store={}'.format(self.key_value_store.store))


if __name__ == '__main__':
    unittest.main()
