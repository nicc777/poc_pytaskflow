
def keys_to_lower(data: dict):
    final_data = dict()
    for key in data.keys():
        if isinstance(data[key], dict):
            final_data[key.lower()] = keys_to_lower(data[key])
        else:
            final_data[key.lower()] = data[key]
    return final_data


class Task:

    def __init__(self, kind: str, version: str, spec: dict, metadata: dict=dict()):
        self.kind = kind
        self.version = version
        self.metadata = keys_to_lower(data=metadata)
        self.spec = keys_to_lower(data=spec)
        self.selector_register = dict()
        self.annotations = dict()
        self.task_dependencies = dict()
        self._calculate_selector_registers()
        self._register_annotations()
        self._register_dependencies()

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


class TaskProcessor:

    def __init__(self, kind: str, kind_versions: list):
        self.kind = kind
        self.versions = kind_versions

    def process_task(self, task: Task, command: str, context: str='default'):
        raise Exception('Not implemented')


class Tasks:

    def __init__(self):
        self.tasks = list()
        self.task_processors_executors = dict()
        self.task_processor_register = dict()

    def add_task(self, task: Task):
        self.tasks.append(task)

    def register_task_processor(self, processor: TaskProcessor):
        if isinstance(processor.versions, list):
            executor_id = '{}'.format(processor.kind)
            for version in processor.versions:
                executor_id = '{}:{}'.format(executor_id, version)
            self.task_processors_executors[executor_id] = processor
            for version in processor.versions:
                id = '{}:{}'.format(processor.kind, version)
                self.task_processor_register[id] = executor_id

    

