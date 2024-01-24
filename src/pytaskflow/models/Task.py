import json
import hashlib
import copy


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


class LoggerWrapper:

    def __init__(self):
        pass

    def info(self, message: str):
        if isinstance(message, str):
            print(message)

    def warn(self, message: str):
        self.info(message=message)

    def warning(self, message: str):
        self.info(message=message)

    def debug(self, message: str):
        self.info(message=message)

    def critical(self, message: str):
        self.info(message=message)

    def error(self, message: str):
        self.info(message=message)


class Task:

    def __init__(self, kind: str, version: str, spec: dict, metadata: dict=dict(), logger: LoggerWrapper=LoggerWrapper()):
        """
            Typical Manifest:

                kind: STRING                                                    [required]
                version: STRING                                                 [required]
                metadata:
                  name: STRING                                                  [optional]
                  labels:                                                       [optional]
                    key: STRING
                  annotations:                                                  [optional]
                    contexts: CSV-STRING                                        [optional, but when supplied only commands within the defined context will be in scope for processing]
                    dependency/name: CSV-STRING                                 [optional. list of other task names this task depends on]
                    dependency/label/STRING(label-name): STRING(label-value)    [optional. select dependant task by label value]
                spec:
                  ... as required by the TaskProcessor ...
        """
        self.logger = logger
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
        self.task_dependencies['NamedTasks'] = list()
        self.task_dependencies['Labels'] = list()
        self.task_as_dict = dict()
        self.task_contexts = ['default']
        self._calculate_selector_registers()
        self._register_annotations()
        self._register_dependencies()
        self.task_checksum = None
        self.task_id = self._determine_task_id()
        logger.info('Task "{}" registered. Task checksum: {}'.format(self.task_id, self.task_checksum))

    def task_match_name(self, name: str)->bool:
        self.logger.debug(message='[task:{}] Attempting to match name "{}"'.format(self.task_id, name))
        if 'name' in self.selector_register:
            self.logger.debug(message='[task:{}] Local task name is "{}"'.format(self.task_id, self.selector_register['name']))
            if name == self.selector_register['name']:
                return True
        else:
            self.logger.debug(message='[task:{}] This task has no name defined and a match can therefore not be made.'.format(self.task_id))
        return False
    
    def task_match_label(self, key: str, value: str)->bool:
        self.logger.debug(message='[task:{}] Attempting to match label with key "{}" and value "{}"'.format(self.task_id, key, value))
        if key in self.selector_register:
            if value == self.selector_register[key]:
                return True
        return False

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
                    if key != 'dependency/name' and not key != 'contexts' and not key.startswith('dependency/label/'):
                        self.annotations[key] = '{}'.format(val)
                    elif key.startswith('contexts'):
                        for item in '{}'.format(val).replace(' ', '').split(','):
                            if 'default' in self.task_contexts and len(self.task_contexts) == 1 and item != 'default':
                                self.task_contexts = list()
                            self.task_contexts.append(item)

    def _register_dependencies(self):
        if 'annotations' in self.metadata:
            if isinstance(self.metadata['annotations'], dict):
                for annotation_key, annotation_value in self.metadata['annotations'].items():
                    if annotation_key.lower() == 'dependency/name':
                        for item in '{}'.format(annotation_value).replace(' ', '').split(','):
                            if len(item) > 0:
                                self.task_dependencies['NamedTasks'].append(item)
                    if annotation_key.lower().startswith('dependency/label/'):
                        self.task_dependencies['Labels'].append(
                            {
                                '{}'.format(annotation_key.lower()): '{}'.format(annotation_value)
                            }
                        )

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
            return hashlib.sha256(self.selector_register['name'].encode('utf-8')).hexdigest()
        elif len(self.selector_register) > 0:
            selector_register_json = json.dumps(self.selector_register)
            return hashlib.sha256(selector_register_json.encode('utf-8')).hexdigest()
        else:
            return copy.deepcopy(self.task_checksum)
        
    def __iter__(self):
        for k,v in self.task_as_dict.items():
            yield (k, v)


class TaskProcessor:

    def __init__(self, kind: str, kind_versions: list, supported_commands: list=['apply', 'get', 'delete', 'describe'], logger: LoggerWrapper=LoggerWrapper()):
        self.logger = logger
        self.kind = kind
        self.versions = kind_versions
        self.supported_commands = supported_commands

    def task_pre_processing_registration_check(self, task: Task, command: str, context: str='default'):
        task_run_id = 'PROCESSING_TASK:{}:{}:{}'.format(
            task.task_id,
            command,
            context
        )
        if task_run_id not in global_key_value_store.store:
            global_key_value_store.save(key=task_run_id, value=1)
            try:
                self.process_task(task=task, command=command, context=context)
                global_key_value_store.store[task_run_id] = 2
            except:
                global_key_value_store.store[task_run_id] = -1

    def process_task(self, task: Task, command: str, context: str='default'):
        raise Exception('Not implemented')


class Tasks:

    def __init__(self, logger: LoggerWrapper=LoggerWrapper()):
        self.logger = logger
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

    def _task_qualifies_for_contextual_processing(self, task: Task, command: str, context: str)->bool:
        if context in task.task_contexts:
            target_task_processor_id = '{}:{}'.format(task.kind, task.version)
            if target_task_processor_id in self.task_processor_register:
                target_task_processor_executor_id = self.task_processor_register[target_task_processor_id]
                if target_task_processor_executor_id in self.task_processors_executors:
                    target_task_processor_executor = self.task_processors_executors[target_task_processor_executor_id]
                    if command in target_task_processor_executor.supported_commands:
                        return True
        return False

    def find_task_by_name(self, name: str)->Task:
        for task_id, candidate_task in self.tasks.items():
            if candidate_task.task_match_name(name=name) is True:
                return candidate_task
        return None
    
    def find_task_by_label_match(self, label_key: str, label_value: str)->list:
        tasks = list()
        for task_id, candidate_task in self.tasks.items():
            if candidate_task.task_match_label(key=label_key, value=label_value) is True:
                tasks.append(candidate_task)
        return tasks

    def _order_tasks(self, ordered_list: list, candidate_task: Task)->list:
        if candidate_task.task_id in ordered_list:
            return ordered_list # Already seen...        
        for dependant_task_name in candidate_task.task_dependencies['NamedTasks']:
            dependant_task = self.find_task_by_name(name=dependant_task_name)
            if dependant_task is not None:
                if isinstance(dependant_task, Task):
                    ordered_list = self._order_tasks(ordered_list=ordered_list, candidate_task=dependant_task)
        for dependant_task_label in candidate_task.task_dependencies['Labels']:
            label_key = list(dependant_task_label.keys())[0]
            label_value = dependant_task_label[label_key]
            dependant_tasks = self.find_task_by_label_match(label_key=label_key, label_value=label_value)
            if len(dependant_tasks) > 0:
                for dependant_task in dependant_tasks:
                    if isinstance(dependant_task, Task):
                        ordered_list = self._order_tasks(ordered_list=ordered_list, candidate_task=dependant_task)
        ordered_list.append(candidate_task.task_id)
        return ordered_list

    def process_context(self, command: str, context: str):
        """
            1. Determine the order based on task dependencies
            2. Process tasks in order, with the available task processor registered for this task kind and version
        """
        task_order = list()
        for task_id, task in self.tasks.items():
            if self._task_qualifies_for_contextual_processing(task=task, command=command, context=context):
                task_order = self._order_tasks(ordered_list=task_order, candidate_task=task)

        for task_id in task_order:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                target_task_processor_id = '{}:{}'.format(task.kind, task.version)
                if target_task_processor_id in self.task_processor_register:
                    target_task_processor_executor_id = self.task_processor_register[target_task_processor_id]
                    if target_task_processor_executor_id in self.task_processors_executors:
                        target_task_processor_executor = self.task_processors_executors[target_task_processor_executor_id]
                        if isinstance(target_task_processor_executor, TaskProcessor):
                            target_task_processor_executor.task_pre_processing_registration_check(task=task, command=command, context=context, global_key_Value_store=global_key_value_store)

