from typing import Union


class TaskMsgSender:
    def __init__(self, task_func, task_args: Union[iter, dict]):
        self.__task_func = task_func
        self.__task_args = task_args

    def send_msg(self, task_func=None, priority: int = None, **async_kwargs):
        if isinstance(self.__task_args, dict):
            self.__task_args = [self.__task_args]
        for task_args in self.__task_args:
            self.__send_msg(task_args, task_func, priority, **async_kwargs)

    def __send_msg(self, task_args: dict, task_func, priority: int = None, **async_kwargs):
        async_kwargs = {**task_args['async_kwargs'], **(dict(priority=priority) if priority else {}), **async_kwargs}
        (task_func or self.__task_func).apply_async(tuple(task_args['args']), **async_kwargs)


class ProducerMsgBuilder:
    @classmethod
    def pack_args(cls, *args, **async_kwargs):
        return dict(args=args, async_kwargs=async_kwargs)

    @classmethod
    def msg_sender(cls, task_func, pack_args: Union[iter, dict]):
        return TaskMsgSender(task_func, pack_args)
