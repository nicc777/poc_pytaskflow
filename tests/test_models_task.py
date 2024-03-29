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
        self.all_lines_in_sequence = list()

    def info(self, message: str):
        self.info_lines.append('[LOG] INFO: {}'.format(message))
        self.all_lines_in_sequence.append(
            copy.deepcopy(self.info_lines[-1])
        )

    def warn(self, message: str):
        self.warn_lines.append('[LOG] WARNING: {}'.format(message))
        self.all_lines_in_sequence.append(
            copy.deepcopy(self.warn_lines[-1])
        )

    def warning(self, message: str):
        self.warn_lines.append('[LOG] WARNING: {}'.format(message))
        self.all_lines_in_sequence.append(
            copy.deepcopy(self.warn_lines[-1])
        )

    def debug(self, message: str):
        self.debug_lines.append('[LOG] DEBUG: {}'.format(message))
        self.all_lines_in_sequence.append(
            copy.deepcopy(self.debug_lines[-1])
        )

    def critical(self, message: str):
        self.critical_lines.append('[LOG] CRITICAL: {}'.format(message))
        self.all_lines_in_sequence.append(
            copy.deepcopy(self.critical_lines[-1])
        )

    def error(self, message: str):
        self.error_lines.append('[LOG] ERROR: {}'.format(message))
        self.all_lines_in_sequence.append(
            copy.deepcopy(self.error_lines[-1])
        )

    def reset(self):
        self.info_lines = list()
        self.warn_lines = list()
        self.debug_lines = list()
        self.critical_lines = list()
        self.error_lines = list()


def print_logger_lines(logger:LoggerWrapper):
    for line in logger.all_lines_in_sequence:
        print(line)


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
        global_key_value_store = KeyValueStore()
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

        task_metadata = t.metadata
        self.assertIsNotNone(task_metadata)
        self.assertIsInstance(task_metadata, dict)
        self.assertEqual(len(task_metadata), 0)

        task_spec = t.spec
        self.assertIsNotNone(task_spec)
        self.assertIsInstance(task_spec, dict)
        self.assertEqual(len(task_spec), 1)
        self.assertTrue(spec_test_field_name.lower() in task_spec)

        task_annotations = t.annotations
        self.assertIsNotNone(task_annotations)
        self.assertIsInstance(task_annotations, dict)
        self.assertEqual(len(task_annotations), 0)

        task_dependencies = t.task_dependencies
        self.assertIsNotNone(task_dependencies)
        self.assertIsInstance(task_dependencies, list)
        self.assertEqual(len(task_dependencies), 0)
        
        data = dict(t)
        self.assertIsNotNone(data)
        self.assertIsInstance(data, dict)
        self.assertEqual(len(data), 3)
        self.assertTrue('kind' in data)
        self.assertTrue('version' in data)
        self.assertTrue('spec' in data)

    def test_task_basic_init_minimal_with_name_1(self):
        metadata = {
            "identifiers": [
                {
                    "type": "ManifestName",
                    "key": "test1"
                }
            ]
        }
        t = Task(kind='TestKind', version='v1', spec={'field1': 'value1'}, metadata=metadata, logger=self.logger)
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

    def test_task_basic_init_minimal_with_name_and_value_1(self):
        identifier_type = 'Label'
        identifier_key = 'test1'
        identifier_val = 'val2'
        metadata = {
            "identifiers": [
                {
                    "type": identifier_type,
                    "key": identifier_key,
                    "val": identifier_val,
                }
            ]
        }
        t = Task(kind='TestKind', version='v1', spec={'field1': 'value1'}, metadata=metadata, logger=self.logger)
        self.assertIsNotNone(t)
        self.assertIsInstance(t, Task)
        self.assertEqual(t.kind, 'TestKind')

        matching_identifier = Identifier(identifier_type=identifier_type, key=identifier_key, val=identifier_val)
        none_matching_identifier_1 = Identifier(identifier_type=identifier_type, key=identifier_key, val='wrong')
        none_matching_identifier_2 = Identifier(identifier_type=identifier_type, key='wrong', val=identifier_val)
        none_matching_identifier_3 = Identifier(identifier_type='wrong', key=identifier_key, val=identifier_val)
        self.assertTrue(t.match_name_or_label_identifier(identifier=matching_identifier))
        self.assertFalse(t.match_name_or_label_identifier(identifier=none_matching_identifier_1))
        self.assertFalse(t.match_name_or_label_identifier(identifier=none_matching_identifier_2))
        self.assertFalse(t.match_name_or_label_identifier(identifier=none_matching_identifier_3))

    def test_task_basic_init_minimal_with_name_and_value_2(self):
        identifier_type = 'Label'
        identifier_key = 'test1'
        identifier_val = 'val2'
        metadata = {
            "identifiers": [
                {
                    "type": identifier_type,
                    "key": identifier_key,
                    "value": identifier_val,
                }
            ]
        }
        t = Task(kind='TestKind', version='v1', spec={'field1': 'value1'}, metadata=metadata, logger=self.logger)
        self.assertIsNotNone(t)
        self.assertIsInstance(t, Task)
        self.assertEqual(t.kind, 'TestKind')

        matching_identifier = Identifier(identifier_type=identifier_type, key=identifier_key, val=identifier_val)
        none_matching_identifier_1 = Identifier(identifier_type=identifier_type, key=identifier_key, val='wrong')
        none_matching_identifier_2 = Identifier(identifier_type=identifier_type, key='wrong', val=identifier_val)
        none_matching_identifier_3 = Identifier(identifier_type='wrong', key=identifier_key, val=identifier_val)
        self.assertTrue(t.match_name_or_label_identifier(identifier=matching_identifier))
        self.assertFalse(t.match_name_or_label_identifier(identifier=none_matching_identifier_1))
        self.assertFalse(t.match_name_or_label_identifier(identifier=none_matching_identifier_2))
        self.assertFalse(t.match_name_or_label_identifier(identifier=none_matching_identifier_3))

    def test_task_basic_init_minimal_with_name_and_value_3(self):
        identifier_type = 'Label'
        identifier_key = 'test1'
        metadata = {
            "contextualIdentifiers": [
                {
                    "type": identifier_type,
                    "key": identifier_key,
                    "contexts": [
                        {
                            "type": "Environment",
                            "names": [
                                "c1",
                                "c2"
                            ]
                        }
                    ]
                }
            ]
        }
        t = Task(kind='TestKind', version='v1', spec={'field1': 'value1'}, metadata=metadata, logger=self.logger)
        self.assertIsNotNone(t)
        self.assertIsInstance(t, Task)
        self.assertEqual(t.kind, 'TestKind')

        matching_contexts_1 = IdentifierContexts()
        matching_contexts_1.add_identifier_context(
            identifier_context=IdentifierContext(context_type='Environment', context_name='c1')
        )
        matching_contexts_1.add_identifier_context(
            identifier_context=IdentifierContext(context_type='Environment', context_name='c2')
        )
        matching_contexts_2 = IdentifierContexts()
        matching_contexts_2.add_identifier_context(
            identifier_context=IdentifierContext(context_type='Environment', context_name='c1')
        )
        matching_contexts_3 = IdentifierContexts()
        matching_contexts_3.add_identifier_context(
            identifier_context=IdentifierContext(context_type='Environment', context_name='c2')
        )

        none_matching_contexts_1 = IdentifierContexts()
        none_matching_contexts_1.add_identifier_context(
            identifier_context=IdentifierContext(context_type='Environment', context_name='c3')
        )

        matching_identifier_1 = Identifier(identifier_type=identifier_type, key=identifier_key, identifier_contexts=matching_contexts_1)
        matching_identifier_2 = Identifier(identifier_type=identifier_type, key=identifier_key, identifier_contexts=matching_contexts_2)
        matching_identifier_3 = Identifier(identifier_type=identifier_type, key=identifier_key, identifier_contexts=matching_contexts_3)
        none_matching_identifier_1 = Identifier(identifier_type=identifier_type, key=identifier_key, val='wrong')
        none_matching_identifier_2 = Identifier(identifier_type=identifier_type, key='wrong')
        none_matching_identifier_3 = Identifier(identifier_type='wrong', key=identifier_key)
        none_matching_identifier_4 = Identifier(identifier_type=identifier_type, key=identifier_key, identifier_contexts=none_matching_contexts_1)
        self.assertTrue(t.match_name_or_label_identifier(identifier=matching_identifier_1))
        self.assertTrue(t.match_name_or_label_identifier(identifier=matching_identifier_2))
        self.assertTrue(t.match_name_or_label_identifier(identifier=matching_identifier_3))
        self.assertFalse(t.match_name_or_label_identifier(identifier=none_matching_identifier_1))
        self.assertFalse(t.match_name_or_label_identifier(identifier=none_matching_identifier_2))
        self.assertFalse(t.match_name_or_label_identifier(identifier=none_matching_identifier_3))
        self.assertFalse(t.match_name_or_label_identifier(identifier=none_matching_identifier_4))

    def test_task_basic_init_minimal_with_no_name_produces_debug_message_when_lookup_by_name_is_done(self):
        t = Task(kind='TestKind', version='v1', spec={'field1': 'value1'}, metadata=dict(), logger=self.logger)
        match_1 = t.task_match_name(name='test1')
        self.assertFalse(match_1)

    def test_task_basic_init_minimal_with_name_and_labels_1(self):
        metadata = {
            "identifiers": [
                {
                    "type": "ManifestName",
                    "key": "test1"
                },
                {
                    "type": "Label",
                    "key": "label1",
                    "value": "labelvalue1"
                },
                {
                    "type": "Label",
                    "key": "label2",
                    "value": "labelvalue2"
                },
            ]
        }
        t = Task(kind='TestKind', version='v1', spec={'field1': 'value1'}, metadata=metadata, logger=self.logger)
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
            metadata = {
                "identifiers": [
                    {
                        "type": "ManifestName",
                        "key": "test1"
                    },
                ],
                "contextualIdentifiers": [
                    {
                        "type": "ExecutionScope",
                        "key": "INCLUDE",
                        "contexts": [
                            {
                                "type": "Environment",
                                "names": [
                                    "c1",
                                    "c2"
                                ]
                            }
                        ]
                    }
                ],
                "dependencies": [
                    {
                        "identifierType": "ManifestName",
                        "identifiers": [
                            { "key": "name1" },
                            { "key": "name2" },
                        ]
                    },
                    {
                        "identifierType": "Label",
                        "identifiers": [
                            { "key": "labelname1", "value": "labelvalue1" },
                        ]
                    }
                ],
                "annotations": {
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

        dependencies = t.task_dependencies
        self.assertIsNotNone(dependencies)
        self.assertIsInstance(dependencies, list)
        self.assertEqual(len(dependencies), 3, 'dependencies: {}'.format(dependencies))
        for dependency in dependencies:
            self.assertIsInstance(dependency, Identifier)


class Processor1(TaskProcessor):

    def __init__(self):
        super().__init__(kind='Processor1', kind_versions=['v1'], supported_commands=['command1', 'command2'], logger=TestLogger())

    def process_task(self, task: Task, command: str, context: str='default', key_value_store: KeyValueStore=KeyValueStore(), state_persistence: StatePersistence=StatePersistence())->KeyValueStore:
        self.logger.info('[Processor1]: {}'.format('-'*80))
        self.logger.info('[Processor1]: Processing task_id "{}"'.format(task.task_id))
        self.logger.info('[Processor1]: command="{}"'.format(command))
        self.logger.info('[Processor1]: context="{}"'.format(context))
        current_state = state_persistence.get_object_state(object_identifier=task.task_id)
        can_process = True
        if task.kind != 'Processor1':
            self.logger.error('[Processor1]: Task kind "{}" mismatched and the task will NOT be processed'.format(task.kind))
            can_process = False
        if task.version not in self.versions:
            self.logger.error('[Processor1]: Task version "{}" is not supported and the task will NOT be processed'.format(task.version))
            can_process = False
        if len(current_state) > 0:
            self.logger.error('[Processor1]: Task version "{}" is already in the correct state'.format(task.version))
            can_process = False
        self.logger.info('[Processor1]: can_process={}'.format(can_process))
        if can_process is True:
            # Emulate processing....
            state_persistence.save_object_state(object_identifier=task.task_id, data={'ResourcesCreated': True})
        key_value_store.save(key='Processor1:Processed:{}:Success'.format(task.task_id), value=can_process)
        self.logger.info('[Processor1]: {}'.format('='*80))
        return key_value_store


class Processor2(TaskProcessor):

    def __init__(self):
        super().__init__(kind='Processor2', kind_versions=['v1'], supported_commands=['command2'], logger=TestLogger())
    
    def process_task(self, task: Task, command: str, context: str='default', key_value_store: KeyValueStore=KeyValueStore(), state_persistence: StatePersistence=StatePersistence())->KeyValueStore:
        self.logger.info('[Processor2]: {}'.format('-'*80))
        self.logger.info('[Processor2]: Processing task_id "{}"'.format(task.task_id))
        self.logger.info('[Processor2]: command="{}"'.format(command))
        self.logger.info('[Processor2]: context="{}"'.format(context))
        current_state = state_persistence.get_object_state(object_identifier=task.task_id)
        can_process = True
        if task.kind != 'Processor2':
            self.logger.error('[Processor2]: Task kind "{}" mismatched and the task will NOT be processed'.format(task.kind))
            can_process = False
        if task.version not in self.versions:
            self.logger.error('[Processor2]: Task version "{}" is not supported and the task will NOT be processed'.format(task.version))
            can_process = False
        if len(current_state) > 0:
            self.logger.error('[Processor2]: Task version "{}" is already in the correct state'.format(task.version))
            can_process = False
        self.logger.info('[Processor2]: can_process={}'.format(can_process))
        if can_process is True:
            # Emulate processing....
            state_persistence.save_object_state(object_identifier=task.task_id, data={'ResourcesCreated': True})
        key_value_store.save(key='Processor2:Processed:{}:Success'.format(task.task_id), value=can_process)
        self.logger.info('[Processor2]: {}'.format('='*80))
        return key_value_store


class TestClassTaskProcessor(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)

    def test_processor_1_init_with_successful_exec_of_a_task(self):
        p1 = Processor1()
        t1 = Task(
            kind='Processor1',
            version='v1',
            spec={'field1': 'value1'},
            metadata = {
                "identifiers": [
                    {
                        "type": "ManifestName",
                        "key": "test1"
                    },
                ],
                "contextualIdentifiers": [
                    {
                        "type": "ExecutionScope",
                        "key": "INCLUDE",
                        "contexts": [
                            {
                                "type": "Environment",
                                "names": [
                                    "c1",
                                    "c2"
                                ]
                            }
                        ]
                    }
                ],
            },
            logger=p1.logger
        )
        key_value_store = p1.process_task(task=t1, command='command1', context='c1', key_value_store=KeyValueStore(), state_persistence=StatePersistence())
        self.assertIsNotNone(key_value_store)
        self.assertIsInstance(key_value_store, KeyValueStore)
        self.assertIsNotNone(key_value_store.store)
        self.assertIsInstance(key_value_store.store, dict)
        self.assertEqual(len(key_value_store.store), 1)
        self.assertTrue('Processor1:Processed:{}:Success'.format(t1.task_id) in key_value_store.store)
        self.assertTrue(key_value_store.store['Processor1:Processed:{}:Success'.format(t1.task_id)], 'key_value_store.store={}'.format(key_value_store.store))

        p1_logger = p1.logger
        self.assertIsNotNone(p1_logger)
        self.assertTrue('[LOG] INFO: [Processor1]: can_process=True' in p1.logger.info_lines, 'info_lines={}'.format(p1.logger.info_lines))

    def test_processor_1_init_with_none_matching_task(self):
        p1 = Processor1()
        t1 = Task(
            kind='Processor2',  # !!!
            version='v1',
            spec={'field1': 'value1'},
            metadata = {
                "identifiers": [
                    {
                        "type": "ManifestName",
                        "key": "test1"
                    },
                ],
                "contextualIdentifiers": [
                    {
                        "type": "ExecutionScope",
                        "key": "INCLUDE",
                        "contexts": [
                            {
                                "type": "Environment",
                                "names": [
                                    "c1",
                                    "c2"
                                ]
                            }
                        ]
                    }
                ],
            },
            logger=TestLogger()
        )
        key_value_store = p1.process_task(task=t1, command='command1', context='c1', key_value_store=KeyValueStore(), state_persistence=StatePersistence())
        self.assertIsNotNone(key_value_store)
        self.assertIsInstance(key_value_store, KeyValueStore)
        self.assertIsNotNone(key_value_store.store)
        self.assertIsInstance(key_value_store.store, dict)
        self.assertEqual(len(key_value_store.store), 1)
        self.assertTrue('Processor1:Processed:{}:Success'.format(t1.task_id) in key_value_store.store)
        self.assertFalse(key_value_store.store['Processor1:Processed:{}:Success'.format(t1.task_id)])

        p1_logger = p1.logger
        self.assertIsNotNone(p1_logger)
        self.assertTrue('[LOG] INFO: [Processor1]: can_process=False' in p1.logger.info_lines, 'info_lines={}'.format(p1.logger.info_lines))

    def test_method_task_pre_processing_check_with_valid_task_1(self):
        p1 = Processor1()
        t1 = Task(
            kind='Processor2',  # !!!
            version='v1',
            spec={'field1': 'value1'},
            metadata = {
                "identifiers": [
                    {
                        "type": "ManifestName",
                        "key": "test1"
                    },
                ],
                "contextualIdentifiers": [
                    {
                        "type": "ExecutionScope",
                        "key": "INCLUDE",
                        "contexts": [
                            {
                                "type": "Environment",
                                "names": [
                                    "c1",
                                    "c2"
                                ]
                            }
                        ]
                    }
                ],
            },
            logger=TestLogger()
        )
        expected_key = 'PROCESSING_TASK:{}:command1:c1'.format(t1.task_id)
        key_value_store = p1.task_pre_processing_check(task=t1, command='command1', context='c1', key_value_store=KeyValueStore(), state_persistence=StatePersistence())
        self.assertIsNotNone(key_value_store)
        self.assertIsInstance(key_value_store, KeyValueStore)
        self.assertIsNotNone(key_value_store.store)
        self.assertIsInstance(key_value_store.store, dict)
        self.assertEqual(len(key_value_store.store), 1)
        self.assertTrue(expected_key in key_value_store.store)
        self.assertEqual(key_value_store.store[expected_key], 1)

    def test_method_task_pre_processing_check_with_valid_task_and_execute_1(self):
        p1 = Processor1()
        t1 = Task(
            kind='Processor1',
            version='v1',
            spec={'field1': 'value1'},
            metadata = {
                "identifiers": [
                    {
                        "type": "ManifestName",
                        "key": "test1"
                    },
                ],
                "contextualIdentifiers": [
                    {
                        "type": "ExecutionScope",
                        "key": "INCLUDE",
                        "contexts": [
                            {
                                "type": "Environment",
                                "names": [
                                    "c1",
                                    "c2"
                                ]
                            }
                        ]
                    }
                ],
            },
            logger=TestLogger()
        )
        key_value_store = KeyValueStore()
        expected_key = 'PROCESSING_TASK:{}:command1:c1'.format(t1.task_id)
        key_value_store = p1.task_pre_processing_check(task=t1, command='command1', context='c1', key_value_store=key_value_store, call_process_task_if_check_pass=True, state_persistence=StatePersistence())
        self.assertIsNotNone(key_value_store)
        self.assertIsInstance(key_value_store, KeyValueStore)
        self.assertIsNotNone(key_value_store.store)
        self.assertIsInstance(key_value_store.store, dict)
        self.assertEqual(len(key_value_store.store), 2)
        self.assertTrue(expected_key in key_value_store.store)
        self.assertEqual(key_value_store.store[expected_key], 2, 'key_value_store={}'.format(key_value_store.store))
        self.assertTrue('Processor1:Processed:{}:Success'.format(t1.task_id) in key_value_store.store)
        self.assertTrue(key_value_store.store['Processor1:Processed:{}:Success'.format(t1.task_id)], 'key_value_store={}'.format(key_value_store.store))

        key_value_store = p1.process_task(task=t1, command='command1', context='c1', key_value_store=key_value_store, state_persistence=StatePersistence())
        self.assertIsNotNone(key_value_store)
        self.assertIsInstance(key_value_store, KeyValueStore)
        self.assertIsNotNone(key_value_store.store)
        self.assertIsInstance(key_value_store.store, dict)
        self.assertEqual(len(key_value_store.store), 2)
        self.assertTrue(expected_key in key_value_store.store)
        self.assertEqual(key_value_store.store[expected_key], 2, 'key_value_store={}'.format(key_value_store.store))
        self.assertTrue('Processor1:Processed:{}:Success'.format(t1.task_id) in key_value_store.store)
        self.assertTrue(key_value_store.store['Processor1:Processed:{}:Success'.format(t1.task_id)], 'key_value_store={}'.format(key_value_store.store))

        key_value_store = p1.task_pre_processing_check(task=t1, command='command1', context='c1', key_value_store=key_value_store, call_process_task_if_check_pass=True, state_persistence=StatePersistence())
        self.assertTrue('[LOG] WARNING: Appears task was already previously validated and/or executed' in p1.logger.warn_lines, 'warn_lines={}'.format(p1.logger.warn_lines))


class TestClassTasks(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)
        self.key_value_store = KeyValueStore()

    def test_tasks_basic_single_task_1(self):
        tasks = Tasks(logger=TestLogger(), key_value_store=KeyValueStore(), state_persistence=StatePersistence(logger=TestLogger()))
        tasks.register_task_processor(processor=Processor1())
        tasks.register_task_processor(processor=Processor2())
        tasks.add_task(
            task=Task(
                kind='Processor1',
                version='v1',
                spec={'field1': 'value1'},
                metadata={
                    "identifiers": [
                        {
                            "type": "ManifestName",
                            "key": "test1"
                        }
                    ],
                    "annotations": {
                        "contexts": "c1,c2",
                    }
                },
                logger=tasks.logger
            )
        )
        tasks.process_context(command='command1', context='c1')
        key_value_store = tasks.key_value_store
        logger = tasks.logger
        self.assertIsNotNone(key_value_store)
        self.assertIsInstance(key_value_store, KeyValueStore)
        self.assertEqual(len(key_value_store.store), 2, 'key_value_store={}'.format(key_value_store.store))
        self.assertTrue(len(logger.info_lines) > 0, 'info_lines={}'.format(logger.info_lines))
        self.assertTrue(len(logger.error_lines) == 0, 'error_lines={}'.format(logger.error_lines))
        self.assertTrue(len(logger.critical_lines) == 0, 'critical_lines={}'.format(logger.critical_lines))
        for line in logger.all_lines_in_sequence:
            print(line)

    def test_tasks_basic_single_task_with_invalid_processor_1(self):
        tasks = Tasks(logger=TestLogger(), key_value_store=KeyValueStore(), state_persistence=StatePersistence(logger=TestLogger()))
        tasks.register_task_processor(processor=Processor1())
        tasks.register_task_processor(processor=Processor2())
        with self.assertRaises(Exception) as cm:
            tasks.add_task(
                task=Task(
                    kind='Processor3',
                    version='v1',
                    spec={'field1': 'value1'},
                    metadata={
                        "identifiers": [
                            {
                                "type": "ManifestName",
                                "key": "test1"
                            }
                        ],
                        "annotations": {
                            "contexts": "c1,c2",
                        }
                    },
                    logger=tasks.logger
                )
            )

    def test_tasks_basic_dependant_tasks_1(self):
        tasks = Tasks(logger=TestLogger(), key_value_store=KeyValueStore())
        tasks.register_task_processor(processor=Processor1())
        tasks.register_task_processor(processor=Processor2())
        tasks.add_task(
            task=Task(
                kind='Processor2',
                version='v1',
                spec={'field1': 'value1'},
                metadata={
                    "identifiers": [
                        {
                            "type": "ManifestName",
                            "key": "test2"
                        }
                    ],
                    "annotations": {
                        "contexts": "c1,c2",
                        "dependency/name": "test1",
                    }
                },
                logger=tasks.logger
            )
        )
        tasks.add_task(
            task=Task(
                kind='Processor1',
                version='v1',
                spec={'field1': 'value1'},
                metadata={
                    "identifiers": [
                        {
                            "type": "ManifestName",
                            "key": "test1"
                        }
                    ],
                    "annotations": {
                        "contexts": "c1,c2",
                    }
                },
                logger=tasks.logger
            )
        )

        key_value_store = tasks.key_value_store
        logger = tasks.logger

        tasks.process_context(command='command2', context='c1')
        self.assertIsNotNone(key_value_store)
        self.assertIsInstance(key_value_store, KeyValueStore)
        self.assertEqual(len(key_value_store.store), 4, 'key_value_store={}'.format(key_value_store.store))
        self.assertTrue(len(logger.info_lines) > 0)
        self.assertTrue(len(logger.error_lines) == 0)
        self.assertTrue(len(logger.critical_lines) == 0)
        for line in logger.all_lines_in_sequence:
            print(line)

    def test_tasks_basic_dependant_tasks_2(self):
        tasks = Tasks(logger=TestLogger(), key_value_store=KeyValueStore())
        tasks.register_task_processor(processor=Processor1())
        tasks.register_task_processor(processor=Processor2())

        task_1_metadata = {
            "identifiers": [
                {
                    "type": "ManifestName",
                    "key": "test1"
                },
                {
                    "type": "Label",
                    "key": "l1",
                    "value": "lv1"
                },
                {
                    "type": "Label",
                    "key": "l2",
                    "value": "lv2"
                },
            ],
            "contextualIdentifiers": [
                {
                    "type": "ExecutionScope",
                    "key": "INCLUDE",
                    "contexts": [
                        {
                            "type": "Environment",
                            "names": [
                                "c1",
                                "c2"
                            ]
                        }
                    ]
                }
            ]
        }
        task_2_metadata = {
            "identifiers": [
                {
                    "type": "ManifestName",
                    "key": "test2"
                },
                {
                    "type": "Label",
                    "key": "l1",
                    "value": "lv1"
                },
                {
                    "type": "Label",
                    "key": "l2",
                    "value": "lv2"
                },
            ],
            "contextualIdentifiers": [
                {
                    "type": "ExecutionScope",
                    "key": "INCLUDE",
                    "contexts": [
                        {
                            "type": "Environment",
                            "names": [
                                "c1",
                                "c2"
                            ]
                        }
                    ]
                }
            ],
            "dependencies": [
                {
                    "identifierType": "Label",
                    "identifiers": [
                        { "key": "l1", "value": "lv1" },
                        { "key": "l2", "value": "lv2" },
                    ]
                }
            ]
        }
        task_3_metadata = {
            "identifiers": [
                {
                    "type": "ManifestName",
                    "key": "test3"
                },
            ],
            "contextualIdentifiers": [
                {
                    "type": "ExecutionScope",
                    "key": "INCLUDE",
                    "contexts": [
                        {
                            "type": "Environment",
                            "names": [
                                "c1",
                                "c2"
                            ]
                        },
                        {
                            "type": "Command",
                            "names": ["command1",]
                        }
                    ]
                }
            ],
            "dependencies": [
                {
                    "identifierType": "ManifestName",
                    "identifiers": [
                        { "key": "test2" },
                    ]
                },
            ]
        }

        tasks = Tasks(logger=TestLogger(), key_value_store=KeyValueStore())
        tasks.register_task_processor(processor=Processor1())
        tasks.register_task_processor(processor=Processor2())
        tasks.add_task(
            task=Task(
                kind='Processor1',
                version='v1',
                spec={'field1': 'value1'},
                metadata=task_1_metadata,
                logger=tasks.logger
            )
        )
        tasks.add_task(
            task=Task(
                kind='Processor2',
                version='v1',
                spec={'field1': 'value1'},
                metadata=task_2_metadata,
                logger=tasks.logger
            )
        )
        tasks.add_task( # This task will NOT be processed...
            task=Task(
                kind='Processor2',
                version='v1',
                spec={'field1': 'value1'},
                metadata=task_3_metadata,
                logger=tasks.logger
            )
        )

        key_value_store = tasks.key_value_store
        logger = tasks.logger

        # order = tasks.calculate_current_task_order(command='command2', context='c1')
        order = tasks.calculate_current_task_order(processing_target_identifier=build_command_identifier(command='command2', context='c1'))
        print('order={}'.format(order))

        tasks.process_context(command='command2', context='c1')
        self.assertIsNotNone(key_value_store)
        self.assertIsInstance(key_value_store, KeyValueStore)
        self.assertEqual(len(key_value_store.store), 4, 'key_value_store={}'.format(key_value_store.store))
        self.assertTrue(len(logger.info_lines) > 0)
        self.assertTrue(len(logger.error_lines) == 0)
        self.assertTrue(len(logger.critical_lines) == 0)
        for line in logger.all_lines_in_sequence:
            print(line)

    def test_tasks_adding_same_task_twice_produces_exception_1(self):
        tasks = Tasks(logger=TestLogger(), key_value_store=KeyValueStore())
        tasks.register_task_processor(processor=Processor1())
        tasks.register_task_processor(processor=Processor2())
        tasks.add_task(
            task=Task(
                kind='Processor2',
                version='v1',
                spec={'field1': 'value1'},
                metadata={
                    'name': 'test2',
                    'annotations': {
                        'contexts': 'c1,c2',
                        'dependency/label/command2/l1': 'lv1',
                    }
                },
                logger=tasks.logger
            )
        )
        with self.assertRaises(Exception) as cm:
            tasks.add_task( # This task will NOT be processed...
                task=Task(
                    kind='Processor2',
                    version='v1',
                    spec={'field1': 'value1'},
                    metadata={
                        'name': 'test2',
                        'annotations': {
                            'contexts': 'c1,c2',
                            'dependency/label/command2/l1': 'lv1',
                        }
                    },
                    logger=tasks.logger
                )
            )

    def test_tasks_basic_dependant_tasks_not_found_throws_exception_1(self):
        tasks = Tasks(logger=TestLogger(), key_value_store=KeyValueStore())
        tasks.register_task_processor(processor=Processor1())
        tasks.register_task_processor(processor=Processor2())
        tasks.add_task(
            task=Task(
                kind='Processor2',
                version='v1',
                spec={'field1': 'value1'},
                metadata = {
                    "identifiers": [
                        {
                            "type": "ManifestName",
                            "key": "test2"
                        },
                    ],
                    "contextualIdentifiers": [
                        {
                            "type": "ExecutionScope",
                            "key": "INCLUDE",
                            "contexts": [
                                {
                                    "type": "Environment",
                                    "names": [
                                        "c1",
                                        "c2"
                                    ]
                                }
                            ]
                        }
                    ],
                    "dependencies": [
                        {
                            "identifierType": "ManifestName",
                            "identifiers": [
                                { "key": "test1" },
                            ]
                        }
                    ]
                },
                logger=tasks.logger
            )
        )
        with self.assertRaises(Exception) as cm:
            tasks.process_context(command='command2', context='c1')

    def test_tasks_non_qualifying_task_due_to_context_1(self):
        tasks = Tasks(logger=TestLogger(), key_value_store=KeyValueStore())
        tasks.register_task_processor(processor=Processor1())
        tasks.register_task_processor(processor=Processor2())
        tasks.add_task(
            task=Task(
                kind='Processor1',
                version='v1',
                spec={'field1': 'value1'},
                metadata = {
                    "identifiers": [
                        {
                            "type": "ManifestName",
                            "key": "test1"
                        },
                    ],
                    "contextualIdentifiers": [
                        {
                            "type": "ExecutionScope",
                            "key": "INCLUDE",
                            "contexts": [
                                {
                                    "type": "Environment",
                                    "names": [
                                        "c1",
                                        "c2"
                                    ]
                                }
                            ]
                        }
                    ]
                },
                logger=tasks.logger
            )
        )
        tasks.add_task(
            task=Task(
                kind='Processor1',
                version='v1',
                spec={'field1': 'value1'},
                metadata = {
                    "identifiers": [
                        {
                            "type": "ManifestName",
                            "key": "test2"
                        },
                    ],
                    "contextualIdentifiers": [
                        {
                            "type": "ExecutionScope",
                            "key": "INCLUDE",
                            "contexts": [
                                {
                                    "type": "Environment",
                                    "names": [
                                        "c2"
                                    ]
                                }
                            ]
                        }
                    ]
                },
                logger=tasks.logger
            )
        )

        key_value_store = tasks.key_value_store
        logger = tasks.logger

        tasks.process_context(command='command1', context='c1')

        self.assertIsNotNone(key_value_store)
        self.assertIsInstance(key_value_store, KeyValueStore)
        self.assertEqual(len(key_value_store.store), 2, 'key_value_store={}'.format(key_value_store.store))
        self.assertTrue(len(logger.info_lines) > 0)
        self.assertTrue(len(logger.error_lines) == 0)
        self.assertTrue(len(logger.critical_lines) == 0)
        for line in logger.all_lines_in_sequence:
            print(line)

    def test_tasks_method_find_task_by_name_task_not_found_returns_none(self):
        tasks = Tasks(logger=TestLogger(), key_value_store=KeyValueStore())
        tasks.register_task_processor(processor=Processor1())
        tasks.register_task_processor(processor=Processor2())
        tasks.add_task(
            task=Task(
                kind='Processor1',
                version='v1',
                spec={'field1': 'value1'},
                metadata = {
                    "identifiers": [
                        {
                            "type": "ManifestName",
                            "key": "test1"
                        },
                    ],
                    "contextualIdentifiers": [
                        {
                            "type": "ExecutionScope",
                            "key": "INCLUDE",
                            "contexts": [
                                {
                                    "type": "Environment",
                                    "names": [
                                        "c1",
                                        "c2"
                                    ]
                                }
                            ]
                        }
                    ]
                },
                logger=tasks.logger
            )
        )
        self.assertIsNone(tasks.find_task_by_name(name='test2'))

    def test_tasks_method_find_task_by_name_1(self):
        tasks = Tasks(logger=TestLogger(), key_value_store=KeyValueStore())
        tasks.register_task_processor(processor=Processor1())
        tasks.register_task_processor(processor=Processor2())
        tasks.add_task(
            task=Task(
                kind='Processor1',
                version='v1',
                spec={'field1': 'value1'},
                metadata = {
                    "identifiers": [
                        {
                            "type": "ManifestName",
                            "key": "test1"
                        },
                    ],
                    "contextualIdentifiers": [
                        {
                            "type": "ExecutionScope",
                            "key": "INCLUDE",
                            "contexts": [
                                {
                                    "type": "Environment",
                                    "names": [
                                        "c1",
                                        "c2"
                                    ]
                                }
                            ]
                        }
                    ]
                },
                logger=tasks.logger
            )
        )
        task1 = tasks.find_task_by_name(name='test1', calling_task_id='test2')
        self.assertIsNotNone(task1)
        self.assertIsInstance(task1, Task)
        self.assertEqual(task1.kind, 'Processor1')

        task2 = tasks.find_task_by_name(name='test1', calling_task_id='test1')
        self.assertIsNone(task2)

    def test_tasks_method_get_task_by_task_id(self):
        tasks = Tasks(logger=TestLogger(), key_value_store=KeyValueStore())
        tasks.register_task_processor(processor=Processor1())
        tasks.register_task_processor(processor=Processor2())
        tasks.add_task(
            task=Task(
                kind='Processor1',
                version='v1',
                spec={'field1': 'value1'},
                metadata = {
                    "identifiers": [
                        {
                            "type": "ManifestName",
                            "key": "test1"
                        },
                    ],
                    "contextualIdentifiers": [
                        {
                            "type": "ExecutionScope",
                            "key": "INCLUDE",
                            "contexts": [
                                {
                                    "type": "Environment",
                                    "names": [
                                        "c1",
                                        "c2"
                                    ]
                                }
                            ]
                        }
                    ]
                },
                logger=tasks.logger
            )
        )
        tasks.add_task(
            task=Task(
                kind='Processor2',
                version='v1',
                spec={'field1': 'value1'},
                metadata = {
                    "identifiers": [
                        {
                            "type": "ManifestName",
                            "key": "test2"
                        },
                    ]
                },
                logger=tasks.logger
            )
        )
        task1 = tasks.get_task_by_task_id(task_id='test1')
        self.assertIsNotNone(task1)
        self.assertIsInstance(task1, Task)
        self.assertEqual(task1.kind, 'Processor1')
        task2 = tasks.get_task_by_task_id(task_id='test2')
        self.assertIsNotNone(task2)
        self.assertIsInstance(task2, Task)
        self.assertEqual(task2.kind, 'Processor2')

        with self.assertRaises(Exception) as cm:
            tasks.get_task_by_task_id(task_id='test3')
        

class TestClassStatePersistence(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)

    def test_basic_state_persistence_1(self):
        state_persistence = StatePersistence(logger=TestLogger(), configuration=dict())
        state_persistence.save_object_state(object_identifier='test1', data={'key1': 'value1'})
        state_persistence.persist_all_state()
        object_data = state_persistence.get_object_state(object_identifier='test1')
        self.assertEqual(object_data['key1'], 'value1')
        self.assertTrue(len(state_persistence.state_cache), 1)


class TestClassTaskLifecycleStages(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)

    def test_basic_life_cycles_1(self):
        task_life_cycle_stages = TaskLifecycleStages()
        self.assertEqual(len(task_life_cycle_stages.stages), 12)

    def test_basic_life_cycles_2(self):
        task_life_cycle_stages = TaskLifecycleStages(init_default_stages=False)
        self.assertEqual(len(task_life_cycle_stages.stages), 0)
        task_life_cycle_stages.register_lifecycle_stage(task_life_cycle_stage=TaskLifecycleStage.TASK_REGISTERED)
        task_life_cycle_stages.register_lifecycle_stage(task_life_cycle_stage=TaskLifecycleStage.TASK_REGISTERED_ERROR)
        self.assertEqual(len(task_life_cycle_stages.stages), 2)

        self.assertTrue(task_life_cycle_stages.stage_registered(stage=TaskLifecycleStage.TASK_REGISTERED))
        self.assertTrue(task_life_cycle_stages.stage_registered(stage=TaskLifecycleStage.TASK_REGISTERED_ERROR))
        self.assertFalse(task_life_cycle_stages.stage_registered(stage=TaskLifecycleStage.TASK_PRE_PROCESSING_COMPLETED))


def hook_function_test_1(
    hook_name:str,
    task:Task,
    key_value_store:KeyValueStore,
    command:str,
    context:str,
    task_life_cycle_stage:int,
    extra_parameters:dict,
    logger:LoggerWrapper
):
    logger.info(
        'Function "hook_function_test_1" called on hook_name "{}" for task "{}" during task_lifecycle_stage "{}"'.format(
            hook_name,
            task.task_id,
            task_life_cycle_stage
        )
    )
    key = '{}:{}:{}:{}:{}'.format(
        hook_name,
        task.task_id,
        command,
        context,
        task_life_cycle_stage
    )
    key_value_store.save(key=key, value=True)
    return key_value_store


class TestClassHook(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)

    def test_exec_hook_on_every_lifecycle_stage_1(self):
        logger = TestLogger()

        hook = Hook(
            name='test_hook_1',
            commands=['command1'],
            contexts=['c1'],
            task_life_cycle_stages=TaskLifecycleStages(),
            function_impl=hook_function_test_1,
            logger=logger
        )

        t1 = Task(
            kind='Processor2',
            version='v1',
            spec={'field1': 'value1'},
            metadata = {
                "identifiers": [
                    {
                        "type": "ManifestName",
                        "key": "test2"
                    },
                ],
                "contextualIdentifiers": [
                    {
                        "type": "ExecutionScope",
                        "key": "INCLUDE",
                        "contexts": [
                            {
                                "type": "Environment",
                                "names": [
                                    "c1",
                                    "c2"
                                ]
                            }
                        ]
                    }
                ],
                "dependencies": [
                    {
                        "identifierType": "ManifestName",
                        "identifiers": [
                            { "key": "test1" },
                        ]
                    }
                ]
            },
            logger=logger
        )

        lifecycle_stages_to_test = (
            TaskLifecycleStage.TASK_PRE_REGISTER,
            TaskLifecycleStage.TASK_REGISTERED,
            TaskLifecycleStage.TASK_PRE_PROCESSING_START,
            TaskLifecycleStage.TASK_PRE_PROCESSING_COMPLETED,
            TaskLifecycleStage.TASK_PROCESSING_PRE_START,
            TaskLifecycleStage.TASK_PROCESSING_POST_DONE,
        )

        for lifecycle_stage in lifecycle_stages_to_test:
            result = hook.process_hook(
                command='command1',
                context='c1',
                task_life_cycle_stage=lifecycle_stage,
                key_value_store=KeyValueStore(),
                task=t1,
                task_id=t1.task_id,
                logger=TestLogger()
            )
            expected_log_entry = '[LOG] INFO: Function "hook_function_test_1" called on hook_name "{}" for task "{}" during task_lifecycle_stage "{}"'.format(
                hook.name,
                t1.task_id,
                lifecycle_stage
            )
            self.assertTrue(expected_log_entry in logger.info_lines, 'info_lines={}'.format(logger.info_lines))

            self.assertIsNotNone(result)
            self.assertIsInstance(result, KeyValueStore)
            expected_key = '{}:{}:command1:c1:{}'.format(
                hook.name,
                t1.task_id,
                lifecycle_stage
            )
            self.assertTrue(expected_key in result.store)
            self.assertTrue(result.store[expected_key])

        print_logger_lines(logger=logger)

    def test_exec_hook_skip_on_command_mismatch_1(self):
        logger = TestLogger()

        hook = Hook(
            name='test_hook_1',
            commands=['command1'],
            contexts=['c1'],
            task_life_cycle_stages=TaskLifecycleStages(),
            function_impl=hook_function_test_1,
            logger=logger
        )

        t1 = Task(
            kind='Processor2',
            version='v1',
            spec={'field1': 'value1'},
            metadata = {
                "identifiers": [
                    {
                        "type": "ManifestName",
                        "key": "test2"
                    },
                ],
                "contextualIdentifiers": [
                    {
                        "type": "ExecutionScope",
                        "key": "INCLUDE",
                        "contexts": [
                            {
                                "type": "Environment",
                                "names": [
                                    "c1",
                                    "c2"
                                ]
                            }
                        ]
                    }
                ],
                "dependencies": [
                    {
                        "identifierType": "ManifestName",
                        "identifiers": [
                            { "key": "test1" },
                        ]
                    }
                ]
            },
            logger=logger
        )

        result = hook.process_hook(
            command='command2',     # Mismatch
            context='c1',
            task_life_cycle_stage=TaskLifecycleStage.TASK_REGISTERED,
            key_value_store=KeyValueStore(),
            task=t1,
            task_id=t1.task_id,
            logger=TestLogger()
        )
        self.assertIsNotNone(result)
        self.assertIsInstance(result, KeyValueStore)
        self.assertEqual(len(result.store), 0)

        print_logger_lines(logger=logger)

    def test_exec_hook_skip_on_context_mismatch_1(self):
        logger = TestLogger()

        hook = Hook(
            name='test_hook_1',
            commands=['command1'],
            contexts=['c1'],
            task_life_cycle_stages=TaskLifecycleStages(),
            function_impl=hook_function_test_1,
            logger=logger
        )

        t1 = Task(
            kind='Processor2',
            version='v1',
            spec={'field1': 'value1'},
            metadata = {
                "identifiers": [
                    {
                        "type": "ManifestName",
                        "key": "test2"
                    },
                ],
                "contextualIdentifiers": [
                    {
                        "type": "ExecutionScope",
                        "key": "INCLUDE",
                        "contexts": [
                            {
                                "type": "Environment",
                                "names": [
                                    "c1",
                                    "c2"
                                ]
                            }
                        ]
                    }
                ],
                "dependencies": [
                    {
                        "identifierType": "ManifestName",
                        "identifiers": [
                            { "key": "test1" },
                        ]
                    }
                ]
            },
            logger=logger
        )

        result = hook.process_hook(
            command='command1',
            context='c3',     # Mismatch
            task_life_cycle_stage=TaskLifecycleStage.TASK_REGISTERED,
            key_value_store=KeyValueStore(),
            task=t1,
            task_id=t1.task_id,
            logger=TestLogger()
        )
        self.assertIsNotNone(result)
        self.assertIsInstance(result, KeyValueStore)
        self.assertEqual(len(result.store), 0)

        print_logger_lines(logger=logger)

    def test_exec_hook_skip_on_lifecycle_stage_mismatch_1(self):
        logger = TestLogger()

        hook = Hook(
            name='test_hook_1',
            commands=['command1'],
            contexts=['c1'],
            task_life_cycle_stages=TaskLifecycleStages(),
            function_impl=hook_function_test_1,
            logger=logger
        )

        t1 = Task(
            kind='Processor2',
            version='v1',
            spec={'field1': 'value1'},
            metadata = {
                "identifiers": [
                    {
                        "type": "ManifestName",
                        "key": "test2"
                    },
                ],
                "contextualIdentifiers": [
                    {
                        "type": "ExecutionScope",
                        "key": "INCLUDE",
                        "contexts": [
                            {
                                "type": "Environment",
                                "names": [
                                    "c1",
                                    "c2"
                                ]
                            }
                        ]
                    }
                ],
                "dependencies": [
                    {
                        "identifierType": "ManifestName",
                        "identifiers": [
                            { "key": "test1" },
                        ]
                    }
                ]
            },
            logger=logger
        )

        result = hook.process_hook(
            command='command1',
            context='c1',
            task_life_cycle_stage=9999,   # Mismatch
            key_value_store=KeyValueStore(),
            task=t1,
            task_id=t1.task_id,
            logger=TestLogger()
        )
        self.assertIsNotNone(result)
        self.assertIsInstance(result, KeyValueStore)
        self.assertEqual(len(result.store), 0)

        print_logger_lines(logger=logger)

    def test_exec_hook_handle_function_exception_1(self):
        logger = TestLogger()

        def f1(*args, **kwargs):
            raise Exception('I died !!')

        hook = Hook(
            name='test_hook_1',
            commands=['command1'],  # INVALID COMMAND...
            contexts=['c1'],
            task_life_cycle_stages=TaskLifecycleStages(),
            function_impl=f1,
            logger=logger
        )

        t1 = Task(
            kind='Processor2',
            version='v1',
            spec={'field1': 'value1'},
            metadata = {
                "identifiers": [
                    {
                        "type": "ManifestName",
                        "key": "test2"
                    },
                ],
                "contextualIdentifiers": [
                    {
                        "type": "ExecutionScope",
                        "key": "INCLUDE",
                        "contexts": [
                            {
                                "type": "Environment",
                                "names": [
                                    "c1",
                                    "c2"
                                ]
                            }
                        ]
                    }
                ],
                "dependencies": [
                    {
                        "identifierType": "ManifestName",
                        "identifiers": [
                            { "key": "test1" },
                        ]
                    }
                ]
            },
            logger=logger
        )

        with self.assertRaises(Exception) as cm:
            result = hook.process_hook(
                command='command1',
                context='c1',
                task_life_cycle_stage=TaskLifecycleStage.TASK_REGISTERED,
                key_value_store=KeyValueStore(),
                task=t1,
                task_id=t1.task_id,
                logger=TestLogger()
            )

        print_logger_lines(logger=logger)


class TestClassHooks(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)

    def test_init_basic_1(self):
        logger = TestLogger()

        hook = Hook(
            name='test_hook_1',
            commands=['NOT_APPLICABLE', 'command1'],
            contexts=['ALL', 'c1'],
            task_life_cycle_stages=TaskLifecycleStages(),
            function_impl=hook_function_test_1,
            logger=logger
        )

        t1 = Task(
            kind='Processor2',
            version='v1',
            spec={'field1': 'value1'},
            metadata = {
                "identifiers": [
                    {
                        "type": "ManifestName",
                        "key": "test2"
                    },
                ],
                "contextualIdentifiers": [
                    {
                        "type": "ExecutionScope",
                        "key": "INCLUDE",
                        "contexts": [
                            {
                                "type": "Environment",
                                "names": [
                                    "c1",
                                    "c2"
                                ]
                            }
                        ]
                    }
                ],
                "dependencies": [
                    {
                        "identifierType": "ManifestName",
                        "identifiers": [
                            { "key": "test1" },
                        ]
                    }
                ]
            },
            logger=logger
        )

        lifecycle_stages_to_test = (
            TaskLifecycleStage.TASK_PRE_REGISTER,
            TaskLifecycleStage.TASK_REGISTERED,
            TaskLifecycleStage.TASK_PRE_PROCESSING_START,
            TaskLifecycleStage.TASK_PRE_PROCESSING_COMPLETED,
            TaskLifecycleStage.TASK_PROCESSING_PRE_START,
            TaskLifecycleStage.TASK_PROCESSING_POST_DONE,
        )

        hooks = Hooks()
        hooks.register_hook(hook=hook)
        for lifecycle_stage in lifecycle_stages_to_test:
            result = hooks.any_hook_exists(command='command1', context='c1', task_life_cycle_stage=lifecycle_stage)
            self.assertTrue(result)

            key_value_store = hooks.process_hook(
                command='command1',
                context='c1',
                task_life_cycle_stage=lifecycle_stage,
                key_value_store=KeyValueStore(),
                task=t1,
                task_id=t1.task_id,
                logger=logger
            )

            self.assertIsNotNone(key_value_store)
            self.assertIsInstance(key_value_store, KeyValueStore)
            expected_key = '{}:{}:command1:c1:{}'.format(
                hook.name,
                t1.task_id,
                lifecycle_stage
            )
            self.assertTrue(expected_key in key_value_store.store)
            self.assertTrue(key_value_store.store[expected_key])

        print_logger_lines(logger=logger)

    def test_ensure_task_processing_covers_all_lifecycle_stages_1(self):
        logger = TestLogger()

        hook = Hook(
            name='test_hook_1',
            commands=['NOT_APPLICABLE', 'command1'],
            contexts=['ALL', 'c1'],
            task_life_cycle_stages=TaskLifecycleStages(),
            function_impl=hook_function_test_1,
            logger=logger
        )

        # FIXME
        t1 = Task(
            kind='Processor1',
            version='v1',
            spec={'field1': 'value1'},
            metadata = {
                "identifiers": [
                    {
                        "type": "ManifestName",
                        "key": "test1"
                    },
                ],
                "contextualIdentifiers": [
                    {
                        "type": "ExecutionScope",
                        "key": "INCLUDE",
                        "contexts": [
                            {
                                "type": "Environment",
                                "names": [
                                    "c1",
                                    "c2"
                                ]
                            }
                        ]
                    }
                ]
            },
            logger=logger
        )

        hooks = Hooks()
        hooks.register_hook(hook=hook)

        tasks = Tasks(logger=TestLogger(), key_value_store=KeyValueStore(), state_persistence=StatePersistence(logger=TestLogger()), hooks=hooks)
        tasks.register_task_processor(processor=Processor1())
        tasks.register_task_processor(processor=Processor2())
        tasks.add_task(task=t1)
        tasks.process_context(command='command1', context='c1')

        print('key_value_store: {}'.format(tasks.key_value_store.store))

        lifecycle_stages_to_test = (
            TaskLifecycleStage.TASK_PRE_REGISTER,
            TaskLifecycleStage.TASK_REGISTERED,
        )

        self.assertIsNotNone(tasks.key_value_store)
        self.assertIsInstance(tasks.key_value_store, KeyValueStore)
        for lifecycle_stage in lifecycle_stages_to_test:    
            expected_key = '{}:{}:NOT_APPLICABLE:ALL:{}'.format(
                hook.name,
                t1.task_id,
                lifecycle_stage
            )
            self.assertTrue(expected_key in tasks.key_value_store.store, 'FAILURE lifecycle_stage={} :: expected_key "{}" not found'.format(lifecycle_stage, expected_key))
            self.assertTrue(tasks.key_value_store.store[expected_key], 'FAILURE lifecycle_stage={} :: expected_key "{}" is False'.format(lifecycle_stage, expected_key))

        lifecycle_stages_to_test = (
            TaskLifecycleStage.TASK_PRE_PROCESSING_START,
            TaskLifecycleStage.TASK_PRE_PROCESSING_COMPLETED,
            TaskLifecycleStage.TASK_PROCESSING_PRE_START,
            TaskLifecycleStage.TASK_PROCESSING_POST_DONE,
        )

        self.assertIsNotNone(tasks.key_value_store)
        self.assertIsInstance(tasks.key_value_store, KeyValueStore)
        for lifecycle_stage in lifecycle_stages_to_test:    
            expected_key = '{}:{}:command1:c1:{}'.format(
                hook.name,
                t1.task_id,
                lifecycle_stage
            )
            self.assertTrue(expected_key in tasks.key_value_store.store, 'FAILURE lifecycle_stage={} :: expected_key "{}" not found'.format(lifecycle_stage, expected_key))
            self.assertTrue(tasks.key_value_store.store[expected_key], 'FAILURE lifecycle_stage={} :: expected_key "{}" is False'.format(lifecycle_stage, expected_key))

        print_logger_lines(logger=logger)


class TestFunctionHookFunctionAlwaysThrowException(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)

    def test_init_basic_1(self):
        logger = TestLogger()
        t1 = Task(
            kind='Processor1',
            version='v1',
            spec={'field1': 'value1'},
            metadata = {
                "identifiers": [
                    {
                        "type": "ManifestName",
                        "key": "test1"
                    },
                ],
                "contextualIdentifiers": [
                    {
                        "type": "ExecutionScope",
                        "key": "INCLUDE",
                        "contexts": [
                            {
                                "type": "Environment",
                                "names": [
                                    "c1",
                                    "c2"
                                ]
                            }
                        ]
                    }
                ]
            },
            logger=logger
        )

        hook_name = 'test_hook_1'
        command = 'command1'
        context = 'c1'
        life_cycle_stage = TaskLifecycleStage.TASK_PRE_REGISTER

        expected_error_message = 'Hook "{}" forced exception on command "{}" in context "{}" for life stage "{}" in task "{}"'.format(
            hook_name,
            command,
            context,
            life_cycle_stage,
            t1.task_id
        )

        with self.assertRaises(Exception) as cm:
            hook_function_always_throw_exception(
                hook_name=hook_name,
                task=t1,
                key_value_store=KeyValueStore(),
                command=command,
                context=context,
                task_life_cycle_stage=life_cycle_stage,
                extra_parameters=dict(),
                logger=logger
            )

            self.assertTrue(expected_error_message in cm.exception)

    def test_init_custom_1(self):
        logger = TestLogger()
        t1 = Task(
            kind='Processor1',
            version='v1',
            spec={'field1': 'value1'},
            metadata = {
                "identifiers": [
                    {
                        "type": "ManifestName",
                        "key": "test1"
                    },
                ],
                "contextualIdentifiers": [
                    {
                        "type": "ExecutionScope",
                        "key": "INCLUDE",
                        "contexts": [
                            {
                                "type": "Environment",
                                "names": [
                                    "c1",
                                    "c2"
                                ]
                            }
                        ]
                    }
                ]
            },
            logger=logger
        )

        hook_name = 'test_hook_1'
        command = 'command1'
        context = 'c1'
        life_cycle_stage = TaskLifecycleStage.TASK_PRE_REGISTER

        expected_error_message = 'This is a custom message'

        with self.assertRaises(Exception) as cm:
            hook_function_always_throw_exception(
                hook_name=hook_name,
                task=t1,
                key_value_store=KeyValueStore(),
                command=command,
                context=context,
                task_life_cycle_stage=life_cycle_stage,
                extra_parameters={
                    'ExceptionMessage': expected_error_message
                },
                logger=logger
            )

            self.assertTrue(expected_error_message in cm.exception)


class TestClassIdentifierContext(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)

    def test_init_basic_1(self):
        ic = IdentifierContext(context_type='type1', context_name='context1')
        self.assertIsNotNone(ic)
        self.assertIsInstance(ic, IdentifierContext)
        result = ic.context()
        self.assertEqual(result, 'type1:context1')

    def test_contexts_matches_1(self):
        ic1 = IdentifierContext(context_type='type1', context_name='context1')
        ic2 = IdentifierContext(context_type='type1', context_name='context1')
        self.assertTrue(ic1 == ic2)

    def test_contexts_does_not_match_1(self):
        ic1 = IdentifierContext(context_type='type1', context_name='context1')
        ic2 = IdentifierContext(context_type='type2', context_name='context1') # wrong context_type
        self.assertFalse(ic1 == ic2)

    def test_contexts_does_not_match_2(self):
        ic1 = IdentifierContext(context_type='type1', context_name='context1')
        ic2 = IdentifierContext(context_type='type1', context_name='context2') # wrong context_name
        self.assertFalse(ic1 == ic2)

    def test_contexts_does_not_match_3(self):
        ic1 = IdentifierContext(context_type='type1', context_name='context1')
        ic2 = None # Force exception, which is silently ignored
        self.assertFalse(ic1 == ic2)


class TestClassIdentifierContexts(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)

    def test_init_basic_1(self):
        ic1 = IdentifierContext(context_type='type1', context_name='context1')
        ic2 = IdentifierContext(context_type='type2', context_name='context2')
        ic3 = IdentifierContext(context_type='type3', context_name='context3')
        ics = IdentifierContexts()
        self.assertIsNotNone(ics)
        self.assertTrue(ics.is_empty())
        ics.add_identifier_context(identifier_context=ic1)
        ics.add_identifier_context(identifier_context=ic2)
        ics.add_identifier_context(identifier_context=ic2) # This duplicate will be ignored...
        self.assertFalse(ics.is_empty())
        self.assertEqual(len(ics), 2)
        self.assertTrue(ics.contains_identifier_context(target_identifier_context=ic1))
        self.assertTrue(ics.contains_identifier_context(target_identifier_context=ic2))
        self.assertFalse(ics.contains_identifier_context(target_identifier_context=ic3))
        counter = 0
        for ic in ics:
            self.assertIsNotNone(ic)
            self.assertIsInstance(ic, IdentifierContext)
            counter += 1
        self.assertEqual(counter, 2)

    def test_init_basic_2(self):
        ic1 = IdentifierContext(context_type='type1', context_name='context1')
        ic2 = 'Not the right type'
        ic3 = None
        ics = IdentifierContexts()
        self.assertIsNotNone(ics)
        self.assertTrue(ics.is_empty())
        ics.add_identifier_context(identifier_context=ic1)
        ics.add_identifier_context(identifier_context=ic2)
        ics.add_identifier_context(identifier_context=ic3)
        self.assertFalse(ics.is_empty())
        self.assertEqual(len(ics), 1)

    def test_find_matching_identifier_context_1(self):
        ic1 = IdentifierContext(context_type='type1', context_name='context1')
        ic2 = IdentifierContext(context_type='type2', context_name='context2')
        ics = IdentifierContexts()
        ics.add_identifier_context(identifier_context=ic1)
        ics.add_identifier_context(identifier_context=ic2)
        matching_identifier_context = IdentifierContext(context_type='type2', context_name='context2')
        non_matching_identifier_context = IdentifierContext(context_type='type3', context_name='context3')
        self.assertTrue(ics.contains_identifier_context(target_identifier_context=matching_identifier_context))
        self.assertFalse(ics.contains_identifier_context(target_identifier_context=non_matching_identifier_context))


class TestClassIdentifier(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)

    def test_init_basic_1(self):
        ic1 = IdentifierContext(context_type='type1', context_name='context1')
        ic2 = IdentifierContext(context_type='type2', context_name='context2')
        ic3 = IdentifierContext(context_type='type3', context_name='context3')
        main_ics = IdentifierContexts()
        main_ics.add_identifier_context(identifier_context=ic1)
        main_ics.add_identifier_context(identifier_context=ic2)
        matching_ics1 = IdentifierContexts()
        matching_ics1.add_identifier_context(identifier_context=ic1)
        matching_ics2 = IdentifierContexts()
        matching_ics2.add_identifier_context(identifier_context=ic2)
        matching_ics3 = IdentifierContexts()
        matching_ics3.add_identifier_context(identifier_context=ic1)
        matching_ics3.add_identifier_context(identifier_context=ic2)
        none_matching_ics1 = IdentifierContexts()
        none_matching_ics1.add_identifier_context(identifier_context=ic3)
        identifier = Identifier(identifier_type='id_type1', key='key1', val='val1', identifier_contexts=main_ics)
        self.assertIsNotNone(identifier)
        self.assertIsInstance(identifier, Identifier)
        self.assertTrue(identifier.is_contextual_identifier)
        self.assertTrue(identifier.identifier_matches_any_context(identifier_type='id_type1', key='key1', val='val1', target_identifier_contexts=matching_ics1))
        self.assertTrue(identifier.identifier_matches_any_context(identifier_type='id_type1', key='key1', val='val1', target_identifier_contexts=matching_ics2))
        self.assertTrue(identifier.identifier_matches_any_context(identifier_type='id_type1', key='key1', val='val1', target_identifier_contexts=matching_ics3))
        self.assertFalse(identifier.identifier_matches_any_context(identifier_type='id_type1', key='key1', val='val1', target_identifier_contexts=none_matching_ics1)) # target_identifier_contexts mismatches
        self.assertFalse(identifier.identifier_matches_any_context(identifier_type='id_type1', key='key2', val='val1', target_identifier_contexts=matching_ics1)) # key mismatches
        self.assertFalse(identifier.identifier_matches_any_context(identifier_type='id_type2', key='key1', val='val1', target_identifier_contexts=matching_ics1)) # type mismatches
        self.assertFalse(identifier.identifier_matches_any_context(identifier_type='id_type1', key='key1', val='val2', target_identifier_contexts=matching_ics1)) # val mismatches
        self.assertFalse(identifier.identifier_matches_any_context(identifier_type='id_type1', key='key1', target_identifier_contexts=matching_ics1)) # val mismatches

    def test_init_basic_2(self):
        identifier = Identifier(identifier_type='id_type1', key='key1', val='val1')
        self.assertIsNotNone(identifier)
        self.assertIsInstance(identifier, Identifier)
        self.assertFalse(identifier.is_contextual_identifier)
        self.assertTrue(identifier.identifier_matches_any_context(identifier_type='id_type1', key='key1', val='val1'))
        self.assertFalse(identifier.identifier_matches_any_context(identifier_type='id_type1', key='key2', val='val1')) # key mismatches
        self.assertFalse(identifier.identifier_matches_any_context(identifier_type='id_type2', key='key1', val='val1')) # type mismatches
        self.assertFalse(identifier.identifier_matches_any_context(identifier_type='id_type1', key='key1', val='val2')) # val mismatches
        self.assertFalse(identifier.identifier_matches_any_context(identifier_type='id_type1', key='key1')) # val mismatches

    # def test_init_to_dict_1(self):
    #     identifier = Identifier(identifier_type='id_type1', key='key1', val='val1')


class TestClassIdentifiers(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)

    def test_init_basic_1(self):
        ic1 = IdentifierContext(context_type='type1', context_name='context1')
        ic2 = IdentifierContext(context_type='type2', context_name='context2')
        ic3 = IdentifierContext(context_type='type3', context_name='context3')
        main_ics = IdentifierContexts()
        main_ics.add_identifier_context(identifier_context=ic1)
        main_ics.add_identifier_context(identifier_context=ic2)
        identifier1 = Identifier(identifier_type='id_type1', key='key1', val='val1', identifier_contexts=main_ics)
        identifier2 = Identifier(identifier_type='id_type2', key='key2')
        identifiers = Identifiers()
        identifiers.add_identifier(identifier=identifier1)
        identifiers.add_identifier(identifier=identifier2)
        identifiers.add_identifier(identifier=identifier2) # This will have no effect... identifier was already added and duplicates are ignored

        self.assertIsNotNone(identifiers)
        self.assertIsInstance(identifiers, Identifiers)
        self.assertEqual(len(identifiers), 2)
        for candidate_identifier in identifiers:
            self.assertIsInstance(candidate_identifier, Identifier)

        matching_ics1 = IdentifierContexts()
        matching_ics1.add_identifier_context(identifier_context=ic1)
        matching_ics2 = IdentifierContexts()
        matching_ics2.add_identifier_context(identifier_context=ic2)
        matching_ics3 = IdentifierContexts()
        matching_ics3.add_identifier_context(identifier_context=ic1)
        matching_ics3.add_identifier_context(identifier_context=ic2)
        none_matching_ics1 = IdentifierContexts()
        none_matching_ics1.add_identifier_context(identifier_context=ic3)

        self.assertTrue(identifiers.identifier_matches_any_context(identifier_type='id_type1', key='key1', val='val1', target_identifier_contexts=matching_ics1))
        self.assertTrue(identifiers.identifier_matches_any_context(identifier_type='id_type1', key='key1', val='val1', target_identifier_contexts=matching_ics2))
        self.assertTrue(identifiers.identifier_matches_any_context(identifier_type='id_type1', key='key1', val='val1', target_identifier_contexts=matching_ics3))
        self.assertFalse(identifiers.identifier_matches_any_context(identifier_type='id_type1', key='key1', val='val1', target_identifier_contexts=none_matching_ics1)) # target_identifier_contexts mismatches
        self.assertFalse(identifiers.identifier_matches_any_context(identifier_type='id_type1', key='key2', val='val1', target_identifier_contexts=matching_ics1)) # key mismatches
        self.assertFalse(identifiers.identifier_matches_any_context(identifier_type='id_type2', key='key1', val='val1', target_identifier_contexts=matching_ics1)) # type mismatches
        self.assertFalse(identifiers.identifier_matches_any_context(identifier_type='id_type1', key='key1', val='val2', target_identifier_contexts=matching_ics1)) # val mismatches
        self.assertFalse(identifiers.identifier_matches_any_context(identifier_type='id_type1', key='key1', target_identifier_contexts=matching_ics1)) # val mismatches

    def test_identifiers_matching_identifier(self):
        ic1 = IdentifierContext(context_type='type1', context_name='context1')
        ic2 = IdentifierContext(context_type='type2', context_name='context2')
        main_ics = IdentifierContexts()
        main_ics.add_identifier_context(identifier_context=ic1)
        main_ics.add_identifier_context(identifier_context=ic2)
        
        identifier1 = Identifier(identifier_type='id_type1', key='key1', val='val1', identifier_contexts=main_ics)
        identifier2 = Identifier(identifier_type='id_type2', key='key2')
        identifiers = Identifiers()
        identifiers.add_identifier(identifier=identifier1)
        identifiers.add_identifier(identifier=identifier2)

        matching_identifier1 = Identifier(identifier_type='id_type1', key='key1', val='val1', identifier_contexts=main_ics)
        self.assertTrue(identifiers.identifier_found(identifier=matching_identifier1))

        matching_identifier2 = Identifier(identifier_type='id_type2', key='key2')
        self.assertTrue(identifiers.identifier_found(identifier=matching_identifier2))

    def test_identifiers_no_matching_identifier(self):
        ic1 = IdentifierContext(context_type='type1', context_name='context1')
        ic2 = IdentifierContext(context_type='type2', context_name='context2')
        main_ics = IdentifierContexts()
        main_ics.add_identifier_context(identifier_context=ic1)
        main_ics.add_identifier_context(identifier_context=ic2)
        
        identifier1 = Identifier(identifier_type='id_type1', key='key1', val='val1', identifier_contexts=main_ics)
        identifier2 = Identifier(identifier_type='id_type2', key='key2')
        identifiers = Identifiers()
        identifiers.add_identifier(identifier=identifier1)
        identifiers.add_identifier(identifier=identifier2)

        no_matching_identifier1 = Identifier(identifier_type='id_type1', key='key3', val='val1', identifier_contexts=main_ics)
        self.assertFalse(identifiers.identifier_found(identifier=no_matching_identifier1))

        no_matching_identifier2 = Identifier(identifier_type='id_type4', key='key2')
        self.assertFalse(identifiers.identifier_found(identifier=no_matching_identifier2))

    def test_to_dict(self):
        ic1 = IdentifierContext(context_type='type1', context_name='context1')
        ic2 = IdentifierContext(context_type='type1', context_name='context2')
        ic3 = IdentifierContext(context_type='type2', context_name='context3')
        main_ics = IdentifierContexts()
        main_ics.add_identifier_context(identifier_context=ic1)
        main_ics.add_identifier_context(identifier_context=ic2)
        main_ics.add_identifier_context(identifier_context=ic3)
        
        identifier1 = Identifier(identifier_type='id_type1', key='key1', val='val1', identifier_contexts=main_ics)
        identifier2 = Identifier(identifier_type='id_type2', key='key2')
        identifiers = Identifiers()
        identifiers.add_identifier(identifier=identifier1)
        identifiers.add_identifier(identifier=identifier2)

        metadata = identifiers.to_metadata_dict()
        self.assertIsNotNone(metadata)
        self.assertIsInstance(metadata, dict)
        self.assertTrue('identifiers' in metadata)
        self.assertTrue('contextualIdentifiers' in metadata)
        print('IDENTIFIERS METADATA DICT: {}'.format(json.dumps(metadata)))
        self.assertEqual(len(metadata['contextualIdentifiers']), 1)
        self.assertEqual(len(metadata['identifiers']), 1)
        for non_contextual_identifier in metadata['identifiers']:
            self.assertTrue('contexts' not in non_contextual_identifier)
        for contextual_identifier in metadata['contextualIdentifiers']:
            self.assertTrue('contexts' in contextual_identifier)
            type1_found = False
            for context in contextual_identifier['contexts']:
                self.assertTrue('type' in context)
                self.assertTrue('names' in context)
                context_type = context['type']
                context_names = context['names']
                self.assertIsInstance(context_type, str)
                self.assertIsInstance(context_names, list)
                if context_type == 'type1':
                    type1_found = True
                    self.assertTrue(len(context_names), 2)
                else:
                    self.assertTrue(len(context_names), 1)
                self.assertTrue(type1_found)



class TestFunctionBuildNonContextualIdentifiers(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)

    def test_basic_1(self):
        metadata = {
            "identifiers": [
                {
                    "type": "ManifestName",
                    "key": "my-name"
                },
                {
                    "type": "Label",
                    "key": "my-key",
                    "value": "my-value"
                }
            ]
        }
        identifiers = build_non_contextual_identifiers(metadata=metadata)
        self.assertIsNotNone(identifiers)
        self.assertIsInstance(identifiers, Identifiers)
        self.assertEqual(len(identifiers), 2)

        manifest_name_found = False
        label_found = False
        identifier: Identifier
        for identifier in identifiers:
            self.assertIsNotNone(identifier)
            self.assertIsInstance(identifier, Identifier)
            self.assertFalse(identifier.is_contextual_identifier)
            if identifier.identifier_type == 'ManifestName':
                manifest_name_found = True
            elif identifier.identifier_type == 'Label':
                label_found = True
        self.assertTrue(manifest_name_found)
        self.assertTrue(label_found)


class TestFunctionBuildContextualIdentifiers(unittest.TestCase):    # pragma: no cover

    def setUp(self):
        print()
        print('-'*80)

    def test_basic_1(self):
        metadata = {
            "contextualIdentifiers": [
                {
                    "type": "ExecutionScope",
                    "key": "include",
                    "contexts": [
                        {
                            "type": "environment",
                            "names": [
                                "env1",
                                "env2",
                                "env3"
                            ]
                        },
                        {
                            "type": "command",
                            "names": [
                                "cmd1",
                                "cmd2"
                            ]
                        }
                    ]
                }
            ]
        }
        identifiers = build_contextual_identifiers(metadata=metadata)
        self.assertIsNotNone(identifiers)
        self.assertIsInstance(identifiers, Identifiers)
        self.assertEqual(len(identifiers), 1)
        identifier: Identifier
        for identifier in identifiers:
            self.assertIsNotNone(identifier)
            self.assertIsInstance(identifier, Identifier)
            self.assertTrue(identifier.is_contextual_identifier)


if __name__ == '__main__':
    unittest.main()

