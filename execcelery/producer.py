from typing import Any, Optional


class TaskMsgSender:
    def __init__(self, task_func, task_args: Optional[list, dict]):
        self.task_func = task_func
        self.task_args = task_args

    def send_msg(self, task_func: Any = None, priority: int = None):
        if isinstance(self.task_args, dict):
            self.task_args = [self.task_args]
        for task_args in self.task_args:
            self.__send_msg(task_args, task_func, priority)

    def __send_msg(self, task_args: dict, task_func: Any = None, priority: int = None):
        async_kwargs = {**task_args['async_kwargs'], **(dict(priority=priority) if priority else {})}
        (task_func or self.task_func).apply_async(tuple(task_args['args']), **async_kwargs)


class TaskMsgBuilder:
    @classmethod
    def pack_task_args(cls, *args, **async_kwargs):
        return dict(args=args, async_kwargs=async_kwargs)

    @classmethod
    def msg_sender(cls, task_func, pack_task_args: Optional[list, dict]):
        return TaskMsgSender(task_func, pack_task_args)
