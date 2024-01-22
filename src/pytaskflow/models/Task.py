
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
        self._calculate_selector_registers()
        self._register_annotations()

    def _calculate_selector_registers(self):
        if 'name' in self.metadata:
            self.selector_register['name'] = self.metadata['name']
        if 'labels' in self.metadata:
            for label_key, label_value in self.metadata['labels'].items():
                self.selector_register[label_key] = '{}'.format(label_value)
    
    def _register_annotations(self):
        if 'annotations' in self.metadata:
            if isinstance(self.metadata['annotations'], dict):
                for key, val in self.metadata['annotations'].items():
                    self.annotations[key] = '{}'.format(val)


class Tasks:

    def __init__(self):
        self.tasks = list()

    def add_task(self, task: Task):
        self.tasks.append(task)

    

