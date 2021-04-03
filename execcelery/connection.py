from celery import Celery
from kombu import Queue, Exchange
import os


def project_name(path=os.getcwd(), dirs=(".git",), default=None):
    """
    get current project name.
    :param path: scripts path
    :param dirs: save rule e.g: catch git
    :param default: if set default then save to the default absolute path
    :return: project root directory name
    """
    prev, path = None, os.path.abspath(path)
    while not default:
        if any(os.path.isdir(os.path.join(path, d)) for d in dirs):
            default = path.split('/')[-1]
        prev, path = path, os.path.abspath(os.path.join(path, os.pardir))
    return default


class CeleryClient(Celery):
    def __init__(self, main=None, loader=None, backend=None,
                 amqp=None, events=None, log=None, control=None,
                 set_as_current=True, tasks=None, broker=None, include=None,
                 changes=None, config_source=None, fixups=None, task_cls=None,
                 autofinalize=True, namespace=None, strict_typing=True,
                 **kwargs):
        super(CeleryClient, self).__init__(
            main, loader, backend, amqp, events, log, control, set_as_current, tasks, broker, include, changes,
            config_source, fixups, task_cls, autofinalize, namespace, strict_typing, **kwargs)

        self.conf.update(
            accept_content=['json', 'msgpack'],
            task_serializer='msgpack',
            task_compression='zstd',
            task_ignore_result=True,
            result_serializer='json',
            enable_utc=False,
            timezone='Asia/Singapore',
            max_tasks_per_child=100,
            broker_heartbeat=0,
            broker_pool_limit=1000,
        )


class ModelQueue:

    def __init__(self, model, num=True, queue_prefix=None):
        self.queue_prefix = queue_prefix or project_name()
        self.model = model
        self.store = []
        self.num = num
        self.format = ['{0}_{1}_{2:0>2d}_{3}', '{0}_{1}_{2}']

    def q(self, name, special=''):
        ns = name+','+special
        if ns not in self.store:
            self.store.append(ns)
        args, num = ([special+self.queue_prefix, self.model, self.store.index(ns), name], 0
                     ) if self.num else ([special + self.queue_prefix, self.model, name], 1)
        q_name = self.__q_format(num, *args)
        return {'queue': q_name, 'name': q_name}

    @property
    def q_names(self):
        return {self.model: [self.__q_exchange(self.q(*i.split(','))['queue']) for i in self.store]}

    def __q_format(self, num, *args):
        return self.format[num].format(*args)

    @staticmethod
    def __q_exchange(q_name):
        return Queue(**{'name': q_name, 'exchange': Exchange(q_name), 'routing_key': q_name})
