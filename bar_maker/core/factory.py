import functools
from abc import ABCMeta, abstractmethod
from datetime import datetime


class NotRegisteredError(Exception):
    """exception to raise if a task is not found in registry"""

    def __init__(self, msg, *args: object) -> None:
        super().__init__(*args)
        self.msg = msg


class TaskBase(metaclass=ABCMeta):
    def __init__(self, **kwargs) -> None:
        """instantiates a task object that can be executed by the worker without needing to inherit to the top level"""
        self.timestamp = datetime.now()
        for k, v in kwargs.items():
            setattr(self, k, v)

    @abstractmethod
    def execute(self):
        """all tasks require an execute method to call them"""


class Registry:
    """inheritable by tasks so that individual tasks do not need to be inherited to the top to be used by the worker"""

    registry = {}

    @classmethod
    def register(cls, name):
        """decorator which allows a defined class to be registered and used without needing direct inheritance

        Args:
            name (str): string reference to the desired task class

        Returns:
            callable: wrapper to return task class
        """

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
        """method to call a task instance from the registry

        Args:
            name (str): string reference to a desired class

        Returns:
            TaskBase: instance of TaskBase with execute method
        """
        if name not in cls.registry:
            print("Not registered")
            raise NotRegisteredError("Not registered")

        task_class = cls.registry[name]
        return task_class(**kwargs)
