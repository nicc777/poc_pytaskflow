import json
import hashlib
import copy
from collections.abc import Sequence


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


class IdentifierContext:

    def __init__(self, context_type: str, context_name: str):
        self.context_type = context_type
        self.context_name = context_name

    def context(self)->str:
        return '{}:{}'.format(
            self.context_type,
            self.context_name
        )
    
    def to_dict(self)->dict:
        data = dict()
        data['ContextType'] = self.context_type
        data['ContextName'] = self.context_name
        return data


class IdentifierContexts(Sequence):

    def __init__(self):
        self.identifier_contexts = list()
        self.unique_identifier_value = hashlib.sha256(json.dumps(self.identifier_contexts).encode('utf-8')).hexdigest()

    def add_identifier_context(self, identifier_context: IdentifierContext):
        duplicates = False
        for existing_identifier_context in self.identifier_contexts:
            if existing_identifier_context.context_type == identifier_context.context_type and existing_identifier_context.context_name == identifier_context.context_name:
                duplicates = True
        if duplicates is False:
            self.identifier_contexts.append(identifier_context)
            self.unique_identifier_value = hashlib.sha256(json.dumps(self.to_dict()).encode('utf-8')).hexdigest()

    def is_empty(self)->bool:
        if len(self.identifier_contexts) > 0:
            return False
        return True
    
    def contains_identifier_context(self, target_identifier_context: IdentifierContext)->bool:
        if target_identifier_context is not None:
            if isinstance(target_identifier_context, IdentifierContext):
                for local_identifier_context in self.identifier_contexts:
                    if local_identifier_context.context() == target_identifier_context.context():
                        return True
        return False
    
    def to_dict(self)->dict:
        data = dict()
        data['IdentifierContexts'] = list()
        for identifier_context in self.identifier_contexts:
            data['IdentifierContexts'].append(identifier_context.to_dict())
        data['UniqueId'] = self.unique_identifier_value
        return data

    def __getitem__(self, index):
        return self.identifier_contexts[index]

    def __len__(self):
        return len(self.identifier_contexts)


class Identifier:

    def __init__(self, identifier_type: str, key: str, val: str=None, identifier_contexts: IdentifierContexts=IdentifierContexts()):
        self.identifier_type = identifier_type
        self.key = key
        self.val = val
        self.identifier_contexts = identifier_contexts
        self.unique_identifier_value = self._calc_unique_id()
        self.is_contextual_identifier = bool(len(identifier_contexts))

    def _calc_unique_id(self)->str:
        data = dict()
        data['IdentifierType'] = self.identifier_type
        data['IdentifierKey'] = self.key
        if self.val is not None:
            data['IdentifierValue'] = self.val
        data['IdentifierContexts'] = self.identifier_contexts.to_dict()
        return hashlib.sha256(json.dumps(data).encode('utf-8')).hexdigest()

    def identifier_matches_any_context(self, identifier_type: str, key: str, val: str=None, target_identifier_contexts: IdentifierContexts=IdentifierContexts())->bool:
        if self.identifier_type == identifier_type and self.key == key and self.val == val:
            if self.identifier_contexts.is_empty() is True: # This identifier (self) is not context bound, therefore the the given contexts does not matter. 
                return True
            for target_identifier_context in target_identifier_contexts:
                if self.identifier_contexts.contains_identifier_context(target_identifier_context=target_identifier_context):
                    return True
        return False
    
    def to_dict(self)->dict:
        data = dict()
        data['IdentifierType'] = self.identifier_type
        data['IdentifierKey'] = self.key
        if self.val is not None:
            data['IdentifierValue'] = self.val
        data['IdentifierContexts'] = self.identifier_contexts.to_dict()
        data['UniqueId'] = self.unique_identifier_value
        return data
    

class Identifiers(Sequence):

    def __init__(self):
        self.identifiers = list()
        self.unique_identifier_value = hashlib.sha256(json.dumps(self.identifiers).encode('utf-8')).hexdigest()

    def add_identifier(self, identifier: Identifier):
        can_add = True
        for existing_identifier in self.identifiers:
            if existing_identifier.to_dict()['UniqueId'] == identifier.to_dict()['UniqueId']:
                can_add = False
        if can_add is True:
            self.identifiers.append(identifier)

    def identifier_matches_any_context(self, identifier_type: str, key: str, val: str=None, target_identifier_contexts: IdentifierContexts=IdentifierContexts())->bool:
        for local_identifier in self.identifiers:
            if local_identifier.identifier_matches_any_context(identifier_type=identifier_type, key=key, val=val, target_identifier_contexts=target_identifier_contexts) is True:
                return True
        return False

    def to_metadata_dict(self):
        """
            metadata:
              identifiers:                    # Non-contextual identifier
              - type: STRING                  # Example: ManifestName
                key: STRING                   # Example: my-manifest
                value: STRING|NULL            # [Optional]                  <-- Not required for type "ManifestName"
              - type: STRING                  # Example: Label
                key: STRING                   # Example: my-key
                value: STRING|NULL            # Example: my-value           <-- Required for type "Label"

              contextualIdentifiers:
              - type: STRING              # Example: ExecutionScope       <-- THEREFORE, this Manifest is scoped to 3x Environment contexts and 2x Command contexts
                key: STRING               # Example: INCLUDE              <-- or "EXCLUDE", to specifically exclude execution in a given context
                value val: STRING         # Example: Null|None
                contexts:
                - type: STRING              # Example: Environment
                  names:
                  - STRING                  # Example: sandbox
                  - STRING                  # Example: test
                  - STRING                  # Example: production
                - type: STRING              # Example: Command
                  names:
                  - STRING                  # Example: apply
                  - STRING                  # Example: delete

            Therefore, there are essentially 3x types of Identifiers in a standard Task processing context:

                * "ManifestName", which does not have contexts
                * "Label", which does not have contexts
                * "ExecutionScope", which DOES have contexts

            Any other "identifiers" (with or without contexts) must be handled/processed by the TaskProcessor Implementation as required
        """
        metadata = dict()
        for identifier in self.identifiers:
            if isinstance(identifier, Identifier):
                if identifier.is_contextual_identifier is True:
                    if 'annotations' not in metadata:
                        metadata['annotations'] = dict()
                    if 'contextualIdentifiers' not in metadata['annotations']:
                        metadata['annotations']['contextualIdentifiers'] = list()

                    # TODO

                else:
                    if 'annotations' not in metadata:
                        metadata['annotations'] = dict()
                    if 'identifiers' not in metadata['annotations']:
                        metadata['annotations']['identifiers'] = list()

                    # TODO

        return metadata

    def __getitem__(self, index):
        return self.identifiers[index]

    def __len__(self):
        return len(self.identifiers)


class StatePersistence:

    def __init__(self, logger: LoggerWrapper=LoggerWrapper(), configuration: dict=dict()):
        self.logger = logger
        self.state_cache = self.retrieve_all_state_from_persistence()
        self.configuration = configuration

    def retrieve_all_state_from_persistence(self)->dict:
        self.logger.warning(message='StatePersistence.retrieve_all_state_from_persistence() NOT IMPLEMENTED. Override this function in your own class for long term state storage.')
        return dict()

    def get_object_state(self, object_identifier: str)->dict:
        if object_identifier in self.state_cache:
            return copy.deepcopy(self.state_cache[object_identifier])
        return dict()

    def save_object_state(self, object_identifier: str, data: dict):
        self.state_cache[object_identifier] = copy.deepcopy(data)

    def persist_all_state(self):
        self.logger.warning(message='StatePersistence.persist_all_state() NOT IMPLEMENTED. Override this function in your own class for long term state storage.')



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
                    key_value_store.store = copy.deepcopy(result.store)
        except:
            exception_message = 'Hook "{}" failed to execute during command "{}" in context "{}" in task life cycle stage "{}"'.format(
                self.name,
                command,
                context,
                task_life_cycle_stage
            )
            self.logger.error(exception_message)
            raise Exception(exception_message)
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
                                key_value_store.store = copy.deepcopy(result.store)
        return key_value_store
    
    def any_hook_exists(self, command: str, context: str, task_life_cycle_stage: int)->bool:
        if context in self.hooks:
            if command in self.hooks[context]:
                for hook_name, life_cycles in self.hooks[context][command].items():
                    if task_life_cycle_stage in life_cycles:
                        return True
        return False


def build_non_contextual_identifiers(metadata: dict, current_identifiers: Identifiers=Identifiers())->Identifiers:
    new_identifiers = Identifiers()
    new_identifiers.identifiers = copy.deepcopy(current_identifiers.identifiers)
    new_identifiers.unique_identifier_value = copy.deepcopy(current_identifiers.unique_identifier_value)

    if 'annotations' in metadata:
        if 'identifiers' in metadata['annotations']:
            if isinstance(metadata['annotations']['identifiers'], list):
                for identifier_data in metadata['annotations']['identifiers']:
                    if 'type' in identifier_data and 'key' in identifier_data:
                        val = None
                        if 'val' in identifier_data:
                            val = identifier_data['val']
                        new_identifiers.add_identifier(identifier=Identifier(identifier_type=identifier_data['type'], key=identifier_data['key'], val=val))

    if 'name' in metadata:
        if isinstance(metadata['name'], str):
            if len(metadata['name']) > 0:
                new_identifiers.add_identifier(identifier=Identifier(identifier_type='ManifestName', key=metadata['name']))

    return new_identifiers


def build_contextual_identifiers(metadata: dict, current_identifiers: Identifiers=Identifiers())->Identifiers:
    new_identifiers = Identifiers()
    new_identifiers.identifiers = copy.deepcopy(current_identifiers.identifiers)
    new_identifiers.unique_identifier_value = copy.deepcopy(current_identifiers.unique_identifier_value)

    if 'annotations' in metadata:
        if 'contextualIdentifiers' in metadata['annotations']:
            if isinstance(metadata['annotations']['contextualIdentifiers'], list):
                for contextual_identifier_data in metadata['annotations']['contextualIdentifiers']:
                    if 'contexts' in contextual_identifier_data and 'identifiers' in contextual_identifier_data:
                        contexts = IdentifierContexts()
                        for context in contextual_identifier_data['contexts']:
                            if 'type' in context and 'names' in context:
                                if isinstance(context['type'], str) is True and isinstance(context['names'], list) is True:
                                    context_type = context['type']
                                    for name in context['names']:
                                        contexts.add_identifier_context(
                                            identifier_context=IdentifierContext(
                                                context_type=context_type,
                                                context_name=name
                                            )
                                        )
                        for identifier_data in contextual_identifier_data['identifiers'] and len(contexts) > 0:
                            if 'type' in identifier_data and 'key' in identifier_data:
                                val = None
                                if 'val' in identifier_data:
                                    val = identifier_data['val']
                                new_identifiers.add_identifier(
                                    identifier=Identifier(
                                        identifier_type=identifier_data['type'],
                                        key=identifier_data['key'],
                                        val=val,
                                        identifier_contexts=contexts
                                    )
                                )

    return new_identifiers


class Task:

    def __init__(self, kind: str, version: str, spec: dict, metadata: dict=dict(), logger: LoggerWrapper=LoggerWrapper()):
        """
            Typical Manifest:

                kind: STRING                                                                    # [required]
                version: STRING                                                                 # [required]
                metadata:
                  name: STRING                                                                  # [optional]
                  labels:                                                                       # [optional]
                    key: STRING
                  annotations:                                                                  # [optional]

                    # DEPRECATED....
                    contexts: CSV-STRING                                                        # [optional, but when supplied only commands within the defined context will be in scope for processing]
                    commands: CSV-STRING                                                        # [optional, but when supplied only commands listed here will bring the task in potential scope (dependant also on context)]
                    dependency/name: CSV-STRING                                                 # [optional. list of other task names this task depends on]
                    dependency/label/STRING(command)/STRING(label-name): STRING(label-value)    # [optional. select dependant task by label value]

                    # NEW....
                    identifiers:                    # Non-contextual identifier
                    - type: STRING                  # Example: ManifestName
                      key: STRING                   # Example: my-manifest
                      value: STRING|NULL            # [Optional]                  <-- Not required for type "ManifestName"
                    - type: STRING                  # Example: Label
                      key: STRING                   # Example: my-key
                      value: STRING|NULL            # Example: my-value           <-- Required for type "Label"

                    contextualIdentifiers:
                    - type: STRING              # Example: ExecutionScope       <-- THEREFORE, this Manifest is scoped to 3x Environment contexts and 2x Command contexts
                      key: STRING               # Example: INCLUDE              <-- or "EXCLUDE", to specifically exclude execution in a given context
                      value val: STRING         # Example: Null|None
                      contexts:
                      - type: STRING              # Example: Environment
                        names:
                        - STRING                  # Example: sandbox
                        - STRING                  # Example: test
                        - STRING                  # Example: production
                      - type: STRING              # Example: Command
                        names:
                        - STRING                  # Example: apply
                        - STRING                  # Example: delete

                    dependencies:
                      - identifierType: ManifestName|Label      # Link to a Non-contextual identifier
                        identifiers:
                        - key: STRING
                          value: STRING                         # Optional - required for identifierType "Label"
                          

                spec:
                  ... as required by the TaskProcessor ...
        """
        self.logger = logger
        self.kind = kind
        self.version = version
        self.metadata = dict()
        self.identifiers = build_contextual_identifiers(
            metadata=metadata,
            current_identifiers=build_non_contextual_identifiers(metadata=metadata)
        )
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
        self.task_commands = list()
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
                    if key == 'commands':
                        for item in '{}'.format(val).replace(' ', '').split(','):
                            self.task_commands.append(item)
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
        call_process_task_if_check_pass: bool=False,
        state_persistence: StatePersistence=StatePersistence()
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
                    key_value_store = self.process_task(task=task, command=command, context=context, key_value_store=key_value_store, state_persistence=state_persistence)
                    key_value_store.store[task_run_id] = 2
            except: # pragma: no cover
                key_value_store.store[task_run_id] = -1
        else:
            self.logger.warning(message='Appears task was already previously validated and/or executed')
        return key_value_store

    def process_task(self, task: Task, command: str, context: str='default', key_value_store: KeyValueStore=KeyValueStore(), state_persistence: StatePersistence=StatePersistence())->KeyValueStore:
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

    def __init__(self, logger: LoggerWrapper=LoggerWrapper(), key_value_store: KeyValueStore=KeyValueStore(), hooks: Hooks=Hooks(), state_persistence: StatePersistence=StatePersistence()):
        self.logger = logger
        self.tasks = dict()
        self.task_processors_executors = dict()
        self.task_processor_register = dict()
        self.key_value_store = key_value_store
        self.hooks = hooks
        self.state_persistence = state_persistence
        self.state_persistence.retrieve_all_state_from_persistence()
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
        if task.task_id in self.tasks:
            raise Exception('Task with ID "{}" was already added previously. Please use the "metadata.name" attribute to identify separate (but perhaps similar) manifests.')
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
        final_command_list = [copy.deepcopy(command),]
        if len(task.task_commands) > 0:
            final_command_list = copy.deepcopy(task.task_commands)
        if context in task.task_contexts and command in final_command_list:
            target_task_processor_id = '{}:{}'.format(task.kind, task.version)
            if target_task_processor_id in self.task_processor_register:
                target_task_processor_executor_id = self.task_processor_register[target_task_processor_id]
                if target_task_processor_executor_id in self.task_processors_executors:
                    target_task_processor_executor = self.task_processors_executors[target_task_processor_executor_id]
                    if command in target_task_processor_executor.supported_commands:
                        return True
        return False

    def find_task_by_name(self, name: str, calling_task_id: str=None)->Task:
        for task_id, candidate_task in self.tasks.items():
            process = True
            if calling_task_id is not None:
                if calling_task_id == task_id:
                    process = False
            if process is True:
                if candidate_task.task_match_name(name=name) is True:
                    return candidate_task
        return None
    
    def find_task_by_label_match(self, label_key: str, label_value: str, calling_task_id: str=None)->list:
        tasks = list()
        for task_id, candidate_task in self.tasks.items():
            if calling_task_id is not None:
                if calling_task_id == task_id:
                    return list()
            if candidate_task.task_match_label(key=label_key, value=label_value) is True:
                tasks.append(candidate_task)
        return tasks

    def _order_tasks(self, ordered_list: list, candidate_task: Task, command:str)->list:
        if candidate_task.task_id in ordered_list:
            self.logger.debug('_order_tasks(): Task "{}" already in ordered list.'.format(candidate_task.task_id))
            return ordered_list # Already seen...        
        for dependant_task_name in candidate_task.task_dependencies['NamedTasks']:
            dependant_task = self.find_task_by_name(name=dependant_task_name, calling_task_id=candidate_task.task_id)
            if dependant_task is not None:
                self.logger.debug('_order_tasks(): Task "{}" has dependant task named "{}" with task_id "{}"'.format(candidate_task.task_id, dependant_task_name, dependant_task.task_id))
                if isinstance(dependant_task, Task):
                    ordered_list = self._order_tasks(ordered_list=ordered_list, candidate_task=dependant_task, command=command)
            else:
                raise Exception('_order_tasks(): Task "{}" has dependant task named "{}" which could NOT be found'.format(candidate_task.task_id, dependant_task_name))
        for dependant_task_label in candidate_task.task_dependencies['Labels']:
            candidate_task_label_key = list(dependant_task_label.keys())[0] # example: 'dependency/label/command2/l1' 
            label_key = candidate_task_label_key.split('/')[-1]             # example: 'dependency/label/command2/l1' will become "l1"
            label_command_scope = list(dependant_task_label.keys())[0].split('/')[-2]    # example: 'dependency/label/command2/l1' will become "command2"
            if label_command_scope == command:
                label_value = dependant_task_label[candidate_task_label_key]
                dependant_tasks = self.find_task_by_label_match(label_key=label_key, label_value=label_value, calling_task_id=candidate_task.task_id)
                if len(dependant_tasks) > 0:
                    # FIXME test test_tasks_basic_dependant_tasks_2() is supposed to cover these lines, but it doesn't yet...
                    for dependant_task in dependant_tasks:
                        if isinstance(dependant_task, Task):
                            ordered_list = self._order_tasks(ordered_list=ordered_list, candidate_task=dependant_task, command=command)
        ordered_list.append(candidate_task.task_id)
        return ordered_list

    def calculate_current_task_order(self, command: str, context: str)->list:
        task_order = list()
        for task_id, task in self.tasks.items():
            self.logger.debug('calculate_current_task_order(): Considering task "{}"'.format(task.task_id))
            if self._task_qualifies_for_contextual_processing(task=task, command=command, context=context):
                task_order = self._order_tasks(ordered_list=task_order, candidate_task=task, command=command)
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

                self.key_value_store = self.key_value_store = self.hooks.process_hook(
                    command=command,
                    context=context,
                    task_life_cycle_stage=TaskLifecycleStage.TASK_PRE_PROCESSING_START,
                    key_value_store=self.key_value_store,
                    task=task,
                    task_id=task_id,
                    logger=self.logger
                )

                target_task_processor_id = '{}:{}'.format(task.kind, task.version)
                if target_task_processor_id in self.task_processor_register:
                    target_task_processor_executor_id = self.task_processor_register[target_task_processor_id]
                    if target_task_processor_executor_id in self.task_processors_executors:
                        target_task_processor_executor = self.task_processors_executors[target_task_processor_executor_id]
                        if isinstance(target_task_processor_executor, TaskProcessor):                            
                            self.key_value_store = target_task_processor_executor.task_pre_processing_check(task=task, command=command, context=context, key_value_store=self.key_value_store, call_process_task_if_check_pass=True, state_persistence=self.state_persistence)

                            self.key_value_store = self.key_value_store = self.hooks.process_hook(
                                command=command,
                                context=context,
                                task_life_cycle_stage=TaskLifecycleStage.TASK_PRE_PROCESSING_COMPLETED,
                                key_value_store=self.key_value_store,
                                task=task,
                                task_id=task_id,
                                logger=self.logger
                            )

                            self.key_value_store = self.key_value_store = self.hooks.process_hook(
                                command=command,
                                context=context,
                                task_life_cycle_stage=TaskLifecycleStage.TASK_PROCESSING_PRE_START,
                                key_value_store=self.key_value_store,
                                task=task,
                                task_id=task_id,
                                logger=self.logger
                            )
                            
                            self.state_persistence.persist_all_state()

                            self.key_value_store = self.key_value_store = self.hooks.process_hook(
                                command=command,
                                context=context,
                                task_life_cycle_stage=TaskLifecycleStage.TASK_PROCESSING_POST_DONE,
                                key_value_store=self.key_value_store,
                                task=task,
                                task_id=task_id,
                                logger=self.logger
                            )
