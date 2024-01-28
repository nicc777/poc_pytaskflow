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


class LoggerWrapper:    # pragma: no cover

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


class TaskLifecycleStage:
    TASK_PRE_REGISTER                       = 1
    TASK_PRE_REGISTER_ERROR                 = -1
    TASK_REGISTERED                         = 2
    TASK_REGISTERED_ERROR                   = -2
    TASK_PRE_PROCESSING_START               = 3
    TASK_PRE_PROCESSING_START_ERROR         = -3
    TASK_PRE_PROCESSING_COMPLETED           = 4
    TASK_PRE_PROCESSING_COMPLETED_ERROR     = -4
    TASK_PROCESSING_PRE_START               = 5
    TASK_PROCESSING_PRE_START_ERROR         = -5
    TASK_PROCESSING_POST_DONE               = 6
    TASK_PROCESSING_POST_DONE_ERROR         = -6


class TaskLifecycleStages:

    def __init__(self, init_default_stages: bool=True):
        self.stages = list()
        if init_default_stages is True:
            for i in range(1,7):
                self.stages.append(i)
                self.stages.append(i*-1)

    def register_lifecycle_stage(self, task_life_cycle_stage: int):
        if task_life_cycle_stage not in self.stages:
            self.stages.append(task_life_cycle_stage)

    def stage_registered(self, stage: int)->bool:
        if stage in self.stages:
            return True
        return False


class Hook:

    def __init__(
            self,
            name: str,
            commands: list,
            contexts: list,
            task_life_cycle_stages: TaskLifecycleStages,
            function_impl: object,  # callable object, like a function
            logger: LoggerWrapper=LoggerWrapper()
        ):
        self.name = name
        self.logger = logger
        self.commands = commands
        self.contexts = contexts
        self.task_life_cycle_stages = task_life_cycle_stages
        self.function_impl = function_impl

    def process_hook(self, command: str, context: str, task_life_cycle_stage: int, key_value_store: KeyValueStore, task: object=None, task_id: str=None, extra_parameters:dict=dict(), logger: LoggerWrapper=LoggerWrapper())->KeyValueStore:
        if command not in self.commands or context not in self.contexts or self.task_life_cycle_stages.stage_registered(stage=task_life_cycle_stage) is False:
            return key_value_store
        try:
            self.logger.debug(
                'Hook "{}" executed on stage "{}" for task "{}" for command "{}" in context "{}"'.format(
                    self.name,
                    task_life_cycle_stage,
                    task_id,
                    command,
                    context
                )
            )
            result = self.function_impl(
                hook_name=self.name,
                task=task,
                key_value_store=key_value_store,
                command=command,
                context=context,
                task_life_cycle_stage=task_life_cycle_stage,
                extra_parameters=extra_parameters,
                logger=self.logger
            )
            if result is not None:
                if isinstance(result, KeyValueStore):
                    key_value_store = copy.deepcopy(result)
        except:
            self.logger.error(
                'Hook "{}" failed to execute during command "{}" in context "{}" in task life cycle stage "{}"'.format(
                    self.name,
                    command,
                    context,
                    task_life_cycle_stage
                )
            )
        return key_value_store


class Hooks:

    def __init__(self):
        self.hooks = dict()
        self.hook_registrar = dict()

    def register_hook(self, hook: Hook):
        if hook.name not in self.hook_registrar:
            self.hook_registrar[hook.name] = hook
        for context in hook.contexts:
            if context not in self.hooks:
                self.hooks[context] = dict()
            for command in hook.commands:
                if command not in self.hooks[context]:
                    self.hooks[context][command] = dict()
                if hook.name not in self.hooks[context][command]:
                    self.hooks[context][command][hook.name] = list()
                for stage in hook.task_life_cycle_stages.stages:
                    if stage not in self.hooks[context][command][hook.name]:
                        self.hooks[context][command][hook.name].append(stage)

    def process_hook(self, command: str, context: str, task_life_cycle_stage: int, key_value_store: KeyValueStore, task: object=None, task_id: str=None, extra_parameters:dict=dict(), logger: LoggerWrapper=LoggerWrapper())->KeyValueStore:
        if context in self.hooks:
            if command in self.hooks[context]:
                for hook_name, stages in self.hooks[context][command].items():
                    if hook_name in self.hook_registrar and task_life_cycle_stage in stages:
                        result = self.hook_registrar[hook_name].process_hook(
                            command=command,
                            context=context,
                            task_life_cycle_stage=task_life_cycle_stage,
                            key_value_store=key_value_store,
                            task=task,
                            task_id=task_id,
                            extra_parameters=extra_parameters,
                            logger=logger
                        )
                        if result is not None:
                            if isinstance(result, KeyValueStore):
                                key_value_store = copy.deepcopy(result)
        return key_value_store
    
    def any_hook_exists(self, command: str, context: str, task_life_cycle_stage: int)->bool:
        if context in self.hooks:
            if command in self.hooks[context]:
                for hook_name, life_cycles in self.hooks[context][command].items():
                    if task_life_cycle_stage in life_cycles:
                        return True
        return False


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
                    if key == 'contexts':
                        for item in '{}'.format(val).replace(' ', '').split(','):
                            if 'default' in self.task_contexts and len(self.task_contexts) == 1 and item != 'default':
                                self.task_contexts = list()
                            self.task_contexts.append(item)
                    elif key.startswith('dependency/label') is False and key.startswith('dependency/name') is False:
                        self.annotations[key] = '{}'.format(val)

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

    def task_pre_processing_check(
        self,
        task: Task,
        command: str,
        context: str='default',
        key_value_store: KeyValueStore=KeyValueStore(),
        call_process_task_if_check_pass: bool=False
    )->KeyValueStore:
        """
        Checks if the task can be run.
        """
        task_run_id = 'PROCESSING_TASK:{}:{}:{}'.format(
            task.task_id,
            command,
            context
        )
        if task_run_id not in key_value_store.store:
            key_value_store.save(key=task_run_id, value=1)
        if key_value_store.store[task_run_id] == 1:
            try:
                if call_process_task_if_check_pass is True:
                    key_value_store = self.process_task(task=task, command=command, context=context, key_value_store=key_value_store)
                    key_value_store.store[task_run_id] = 2
            except: # pragma: no cover
                key_value_store.store[task_run_id] = -1
        else:
            self.logger.warning(message='Appears task was already previously validated and/or executed')
        return key_value_store

    def process_task(self, task: Task, command: str, context: str='default', key_value_store: KeyValueStore=KeyValueStore())->KeyValueStore:
        raise Exception('Not implemented')  # pragma: no cover


def hook_function_always_throw_exception(
    hook_name:str,
    task:Task,
    key_value_store:KeyValueStore,
    command:str,
    context:str,
    task_life_cycle_stage:int,
    extra_parameters:dict,
    logger:LoggerWrapper
):
    task_id = 'unknown'
    if task is not None:
        if isinstance(task, Task):
            task_id = task.task_id
    exception_message = 'Hook "{}" forced exception on command "{}" in context "{}" for life stage "{}" in task "{}"'.format(
        hook_name,
        command,
        context,
        task_life_cycle_stage,
        task_id
    )
    if 'ExceptionMessage' in extra_parameters:
        logger.error(exception_message)
        exception_message = extra_parameters['ExceptionMessage']
    raise Exception(exception_message)


class Tasks:

    """
        TASK_PRE_REGISTER                       = 1
        TASK_PRE_REGISTER_ERROR                 = -1
        TASK_REGISTERED                         = 2
        TASK_REGISTERED_ERROR                   = -2
        TASK_PRE_PROCESSING_START               = 3
        TASK_PRE_PROCESSING_START_ERROR         = -3
        TASK_PRE_PROCESSING_COMPLETED           = 4
        TASK_PRE_PROCESSING_COMPLETED_ERROR     = -4
        TASK_PROCESSING_PRE_START               = 5
        TASK_PROCESSING_PRE_START_ERROR         = -5
        TASK_PROCESSING_POST_DONE               = 6
        TASK_PROCESSING_POST_DONE_ERROR         = -6
    """

    def __init__(self, logger: LoggerWrapper=LoggerWrapper(), key_value_store: KeyValueStore=KeyValueStore(), hooks: Hooks=Hooks()):
        self.logger = logger
        self.tasks = dict()
        self.task_processors_executors = dict()
        self.task_processor_register = dict()
        self.key_value_store = key_value_store
        self.hooks = hooks
        self._register_task_registration_failure_exception_throwing_hook()

    def _register_task_registration_failure_exception_throwing_hook(self):
        required_task_life_cycle_stages = TaskLifecycleStages(init_default_stages=False)
        required_task_life_cycle_stages.register_lifecycle_stage(task_life_cycle_stage=TaskLifecycleStage.TASK_REGISTERED_ERROR)
        if self.hooks.any_hook_exists(command='NOT_APPLICABLE', context='ALL', task_life_cycle_stage=TaskLifecycleStage.TASK_REGISTERED_ERROR) is False:
            self.hooks.register_hook(
                hook=Hook(
                    name='DEFAULT_TASK_REGISTERED_ERROR_HOOK',
                    commands=['NOT_APPLICABLE',],
                    contexts=['ALL',],
                    task_life_cycle_stages=required_task_life_cycle_stages,
                    function_impl=hook_function_always_throw_exception,
                    logger=self.logger
                )
            )

    def add_task(self, task: Task):
        self.key_value_store = self.hooks.process_hook(
            command='NOT_APPLICABLE',
            context='ALL',
            task_life_cycle_stage=TaskLifecycleStage.TASK_PRE_REGISTER,
            key_value_store=copy.deepcopy(self.key_value_store),
            task=task,
            task_id=task.task_id
        )
        processor_id = '{}:{}'.format(task.kind, task.version)
        if processor_id not in self.task_processor_register:
            self.key_value_store = self.hooks.process_hook(
                command='NOT_APPLICABLE',
                context='ALL',
                task_life_cycle_stage=TaskLifecycleStage.TASK_REGISTERED_ERROR,
                key_value_store=copy.deepcopy(self.key_value_store),
                task=task,
                task_id=task.task_id
            )
            #raise Exception('Task kind "{}" with version "{}" has no processor registered. Ensure all task processors are registered before adding tasks.'.format(task.kind, task.version))
            self.hooks.process_hook(
                command='NOT_APPLICABLE',
                context='ALL',
                task_life_cycle_stage=TaskLifecycleStage.TASK_REGISTERED_ERROR,
                key_value_store=self.key_value_store,
                task=task,
                task_id='N/A',
                extra_parameters='Task kind "{}" with version "{}" has no processor registered. Ensure all task processors are registered before adding tasks.'.format(task.kind, task.version),
                logger=self.logger
            )
        self.tasks[task.task_id] = task
        self.key_value_store = self.hooks.process_hook(
            command='NOT_APPLICABLE',
            context='ALL',
            task_life_cycle_stage=TaskLifecycleStage.TASK_REGISTERED,
            key_value_store=copy.deepcopy(self.key_value_store),
            task=task,
            task_id=task.task_id
        )

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
            self.logger.debug('_order_tasks(): Task "{}" already in ordered list.'.format(candidate_task.task_id))
            return ordered_list # Already seen...        
        for dependant_task_name in candidate_task.task_dependencies['NamedTasks']:
            dependant_task = self.find_task_by_name(name=dependant_task_name)
            if dependant_task is not None:
                self.logger.debug('_order_tasks(): Task "{}" has dependant task named "{}" with task_id "{}"'.format(candidate_task.task_id, dependant_task_name, dependant_task.task_id))
                if isinstance(dependant_task, Task):
                    ordered_list = self._order_tasks(ordered_list=ordered_list, candidate_task=dependant_task)
            else:
                self.logger.warning('_order_tasks(): Task "{}" has dependant task named "{}" which could NOT be found'.format(candidate_task.task_id, dependant_task_name))
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

    def calculate_current_task_order(self, command: str, context: str)->list:
        task_order = list()
        for task_id, task in self.tasks.items():
            self.logger.debug('calculate_current_task_order(): Considering task "{}"'.format(task.task_id))
            if self._task_qualifies_for_contextual_processing(task=task, command=command, context=context):
                task_order = self._order_tasks(ordered_list=task_order, candidate_task=task)
        return task_order

    def process_context(self, command: str, context: str):
        """
            1. Determine the order based on task dependencies
            2. Process tasks in order, with the available task processor registered for this task kind and version
        """
        task_order = self.calculate_current_task_order(command=command, context=context)
        self.logger.debug('task_order={}'.format(task_order))

        for task_id in task_order:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                target_task_processor_id = '{}:{}'.format(task.kind, task.version)
                if target_task_processor_id in self.task_processor_register:
                    target_task_processor_executor_id = self.task_processor_register[target_task_processor_id]
                    if target_task_processor_executor_id in self.task_processors_executors:
                        target_task_processor_executor = self.task_processors_executors[target_task_processor_executor_id]
                        if isinstance(target_task_processor_executor, TaskProcessor):
                            self.key_value_store = target_task_processor_executor.task_pre_processing_check(task=task, command=command, context=context, key_value_store=self.key_value_store, call_process_task_if_check_pass=True)
