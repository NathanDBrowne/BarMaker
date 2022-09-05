import functools
from abc import ABCMeta, abstractmethod
from datetime import datetime


class NotRegisteredError(Exception):
    def __init__(self, msg, *args: object) -> None:
        super().__init__(*args)
        self.msg = msg


class TaskBase(metaclass=ABCMeta):
    def __init__(self, **kwargs) -> None:
        self.timestamp = datetime.now()
        for k, v in kwargs.items():
            setattr(self, k, v)

    @abstractmethod
    def execute(self):
        """all tasks require an execute method to call them"""


class Registry:
    registry = {}

    @classmethod
    def register(cls, name):
        @functools.wraps(cls)
        def inner_wrapper(wrapped_class):
            if name in cls.registry:
                print("already in use")
                return None

            cls.registry[name] = wrapped_class
            return wrapped_class

        return inner_wrapper

    @classmethod
    def get_task(cls, name, **kwargs):
        if name not in cls.registry:
            print("Not registered")
            return None

        task_class = cls.registry[name]
        return task_class(**kwargs)
