import json
import hashlib


def keys_to_lower(data: dict):
    final_data = dict()
    for key in data.keys():
        if isinstance(data[key], dict):
            final_data[key.lower()] = keys_to_lower(data[key])
        else:
            final_data[key.lower()] = data[key]
    return final_data


class KeyValueStore:

    def __init__(self):
        self.store = dict()

    def save(self, key: str, value: object):
        self.store[key] = value


global_key_value_store = KeyValueStore()


class Task:

    def __init__(self, kind: str, version: str, spec: dict, metadata: dict=dict()):
        self.kind = kind
        self.version = version
        self.metadata = dict()
        if metadata is not None:
            if isinstance(metadata, dict):
                self.metadata = keys_to_lower(data=metadata)
        self.spec = dict()
        if spec is not None:
            if isinstance(spec, dict):
                self.spec = keys_to_lower(data=spec)
        self.selector_register = dict()
        self.annotations = dict()
        self.task_dependencies = dict()
        self.task_as_dict = dict()
        self._calculate_selector_registers()
        self._register_annotations()
        self._register_dependencies()
        self.task_checksum = None
        self.task_id = self._determine_task_id()

    def _calculate_selector_registers(self):
        if 'name' in self.metadata:
            self.selector_register['name'] = '{}'.format(self.metadata['name'])
        if 'labels' in self.metadata:
            if isinstance(self.metadata['labels'], dict):
                for label_key, label_value in self.metadata['labels'].items():
                    self.selector_register[label_key] = '{}'.format(label_value)
    
    def _register_annotations(self):
        if 'annotations' in self.metadata:
            if isinstance(self.metadata['annotations'], dict):
                for key, val in self.metadata['annotations'].items():
                    self.annotations[key] = '{}'.format(val)

    def _register_dependencies(self):
        if 'dependencies' in self.metadata:
            if isinstance(self.metadata['dependencies'], dict):
                for context, context_dependencies in self.metadata['dependencies'].items():
                    if isinstance(context_dependencies, list):
                        self.task_dependencies[context] = context_dependencies

    def _calculate_task_checksum(self)->str:
        data = dict()
        data['kind'] = self.kind
        data['version'] = self.version
        if len(self.metadata) > 0:
            data['metadata'] = self.metadata
        if len(self.spec) > 0:
            data['spec'] = self.spec
        self.task_as_dict = data
        return hashlib.sha256(json.dumps(data).encode('utf-8')).hexdigest()

    def _determine_task_id(self):
        self.task_checksum = self._calculate_task_checksum()
        if 'name' in self.selector_register:
            self.task_id = hashlib.sha256(self.selector_register['name'].encode('utf-8')).hexdigest()
        elif len(self.selector_register) > 0:
            selector_register_json = json.dumps(self.selector_register)
            self.task_id = hashlib.sha256(selector_register_json.encode('utf-8')).hexdigest()
        else:
            self.task_id = self.task_checksum


class TaskProcessor:

    def __init__(self, kind: str, kind_versions: list):
        self.kind = kind
        self.versions = kind_versions

    def process_task(self, task: Task, command: str, context: str='default', global_key_Value_store: KeyValueStore=KeyValueStore()):
        raise Exception('Not implemented')


class Tasks:

    def __init__(self):
        self.tasks = dict()
        self.task_processors_executors = dict()
        self.task_processor_register = dict()

    def add_task(self, task: Task):
        processor_id = '{}:{}'.format(task.kind, task.version)
        if processor_id not in self.task_processor_register:
            raise Exception('Task kind "{}" with version "{}" has no processor registered. Ensure all task processors are registered before adding tasks.'.format(task.kind, task.version))
        self.tasks[task.task_id] = task

    def register_task_processor(self, processor: TaskProcessor):
        if isinstance(processor.versions, list):
            executor_id = '{}'.format(processor.kind)
            for version in processor.versions:
                executor_id = '{}:{}'.format(executor_id, version)
            self.task_processors_executors[executor_id] = processor
            for version in processor.versions:
                id = '{}:{}'.format(processor.kind, version)
                self.task_processor_register[id] = executor_id

    def process_context(self, command: str, context: str):
        """
            1. Determine the order based on task dependencies
            2. Process tasks in order, with the available task processor registered for this task kind and version
        """
        task_order = list()
        # TODO : order tasks

        for task_id in task_order:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                target_task_processor_id = '{}:{}'.format(task.kind, task.version)
                if target_task_processor_id in self.task_processor_register:
                    target_task_processor_executor_id = self.task_processor_register[target_task_processor_id]
                    if target_task_processor_executor_id in self.task_processors_executors:
                        target_task_processor_executor = self.task_processors_executors[target_task_processor_executor_id]
                        if isinstance(target_task_processor_executor, TaskProcessor):
                            target_task_processor_executor.process_task(task=task, command=command, context=context, global_key_Value_store=global_key_value_store)

