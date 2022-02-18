from celery import Celery
from functools import partial
from kombu import Queue, Exchange
from json import loads
import os


class ProjectName:
    default = None

    @classmethod
    def project_name(cls, path=os.getcwd(), dirs=(".git",), default=None):
        """
        get current project name.
        :param path: scripts path
        :param dirs: save rule e.g: catch git
        :param default: if set default then save to the default absolute path
        :return: project root directory name
        """
        default = default or cls.default
        prev, path = None, os.path.abspath(path)
        while not default:
            if any(os.path.isdir(os.path.join(path, d)) for d in dirs):
                default = path.split('/')[-1]
            prev, path = path, os.path.abspath(os.path.join(path, os.pardir))
        return default


class ModelQueue:

    def __init__(self, model, num=True, queue_prefix=None):
        self.queue_prefix = queue_prefix or ProjectName.project_name()
        self.model = model
        self.store = []
        self.num = num
        self.format = ['{0}.{1}_{2:0>2d}_{3}', '{0}_{1}_{2}']

    def q(self, name, special='', exchange='default', routing_key="", exchange_type='direct'):
        ns = f"{name},{special},{exchange},{routing_key},{exchange_type}"
        if ns not in self.store:
            self.store.append(ns)
        args, num = ([special+self.queue_prefix, self.model, self.store.index(ns), name], 0
                     ) if self.num else ([special + self.queue_prefix, self.model, name], 1)
        q_name = self.__q_format(num, *args)
        return {'queue': q_name, 'name': q_name}

    def q_info(self, name, special='', exchange='default', routing_key="", exchange_type='direct'):
        ns = f"{name},{special},{exchange},{routing_key},{exchange_type}"
        args, num = ([special+self.queue_prefix, self.model, self.store.index(ns), name], 0
                     ) if self.num else ([special + self.queue_prefix, self.model, name], 1)
        q_name = self.__q_format(num, *args)
        return {'queue': q_name, 'name': q_name, 'exchange': exchange, 'exchange_type': exchange_type,
                'routing_key': routing_key or q_name}

    @property
    def q_names(self):
        return {self.model: [self.__q_info(**self.q_info(*i.split(','))) for i in self.store]}

    def __q_format(self, num, *args):
        return self.format[num].format(*args)

    @staticmethod
    def __q_info(**kwargs):
        return {'name': kwargs['queue'], 'exchange': kwargs['exchange'], 'exchange_type': kwargs['exchange_type'],
                'routing_key': kwargs['routing_key']}


class CeleryClient(Celery):
    def __init__(self, main=None, loader=None, backend=None,
                 amqp=None, events=None, log=None, control=None,
                 set_as_current=True, tasks=None, broker=None, include=None,
                 changes=None, config_source=None, fixups=None, task_cls=None,
                 autofinalize=True, namespace=None, strict_typing=True, prefix=None,
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
            task_inherit_parent_priority=True,
            task_default_priority=3,
            task_queue_max_priority=10,
            worker_cancel_long_running_tasks_on_connection_loss=True,
        )

        self.argv = ['worker', '--without-heartbeat', '--without-gossip']
        self.conf.task_queues = set()
        self.model_queues = {}
        self.prefix = prefix or ProjectName.project_name()
        self.rest_task_default()

    def run(self, queue_type, queue_list=None, queue_all=False, hostnum=1, celery_args='', prefetch=1, proj_name=None):
        proj_name = proj_name or self.prefix
        ProjectName.default = proj_name
        self.loader.import_default_modules()
        self.conf.update(worker_prefetch_multiplier=prefetch)
        celery_args = list(celery_args)
        self.argv.append(f'-n~{proj_name}.{queue_type}_{"%02d" % hostnum}_{queue_list or "all"}@%d')
        if '-Q' not in celery_args and '-B' not in celery_args and '--beat' not in celery_args:
            self.argv.append('-Q'+','.join(self.choose_queues(queue_type, queue_list, queue_all)))
        if '-B' in celery_args or '--beat' in celery_args:
            beat = partial(self.Beat, logfile=None, pidfile=None)
            beat().run()
        else:
            argv = self.argv + (celery_args[:-1] if ('pro' in celery_args or 'dev' in celery_args) else celery_args)
            self.worker_main(argv)

    def choose_queues(self, q_type, q_list: str = None, q_all=False):
        return [j for i in self.model_queues.values() for j in i] if q_all else self.__split_queue(q_type, q_list)

    def model_q_update(self, model_q):
        self.model_queues.update({k: [i['name'] for i in v] for k, v in model_q.q_names.items()})
        [self.conf.task_queues.add(self.__init_q(**j)) for i in model_q.q_names.values() for j in i]

    def switch_task_meta(self, q_name, task_name=None, exchange=None, task_prefix='app.tasks', serializer='json',
                         model_q=None, exchange_type='direct', **kwargs):
        m_q = model_q or (lambda x: {'queue': x})
        q_data = {**m_q(q_name), **{"name": f"{task_prefix}.{task_name or q_name}", "exchange": exchange or q_name,
                                    "serializer": serializer}, **kwargs}
        self.conf.task_queues.add(self.__init_q(name=q_data['queue'], exchange=q_data['exchange'],
                                                exchange_type=exchange_type, routing_key=q_data['queue']))
        return q_data

    def switch_proj(self, msg, warning_task=None, logger=print):
        task_meta = self.current_worker_task.request.delivery_info
        w_msg = f"Wrong tasks consumer: {msg}, details: {task_meta}"
        if warning_task:
            warning_task.delay(w_msg)
        logger(w_msg)
        raise SystemError('Wrong tasks consumer.')

    def rest_task_default(self, q_name='default', exchange=None, routing_key=None, proj_name=None):
        proj_name = f"{proj_name or self.prefix}"
        self.conf.task_default_queue = f"*{proj_name}.{q_name}"
        self.conf.task_default_exchange = f"{proj_name}.{exchange or q_name}"
        self.conf.task_default_routing_key = f"{proj_name}.{routing_key or q_name}"

    def __init_q(self, **kwargs):
        if kwargs['exchange'] == 'default':
            kwargs["exchange"] = Exchange(self.conf.task_default_exchange, type=kwargs.pop('exchange_type'))
        else:
            kwargs["exchange"] = Exchange(kwargs['exchange'], type=kwargs.pop('exchange_type'))
        return Queue(**kwargs)

    def __split_queue(self, q_type, q_list: str = None):
        return (self.__snum(q_type, q_list) if q_list.count(':') == 1 else
                [self.model_queues.get(q_type)[int(index)] for index in loads(q_list)]
                ) if (q_list and q_list != "[:]") else self.model_queues[q_type]

    def __snum(self, q_type, q_list):
        min_num, max_num = q_list[1:-1].split(':')
        return self.model_queues[q_type][int(min_num):int(max_num)]
