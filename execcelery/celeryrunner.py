from execcelery.connection import project_name
from json import loads


class StartCelery:
    def __init__(self, celery_app, model_queue, prefix=None):
        self.argv = ['worker', '--without-heartbeat', '--without-gossip']
        self.app = celery_app
        self.queues = model_queue
        self.prefix = prefix or project_name()

    def run(self, queue_type, queue_list, queue_all, hostnum, celery_args, prefetch):
        self.app.conf.update(
            task_queues=self.choose_queues(queue_type, queue_list, queue_all), worker_prefetch_multiplier=prefetch)
        celery_args = list(celery_args)
        self.argv.append(f'-n=~ss_ig_{queue_type}_{"%02d" % hostnum}_{queue_list or "all"}@%d')
        argv = self.argv + (celery_args[:-1] if ('pro' in celery_args or 'dev' in celery_args) else celery_args)
        self.app.worker_main(argv)

    def choose_queues(self, q_type, q_list: str = None, q_all=False):
        return [j for i in self.queues.values() for j in i] if q_all else self.__split_queue(q_type, q_list)

    def __split_queue(self, q_type, q_list: str = None):
        return (self.__snum(q_type, q_list) if q_list.count(':') == 1 else
                [self.queues[q_type][int(index)] for index in loads(q_list)])

    def __snum(self, q_type, q_list):
        min_num, max_num = q_list[1:-1].split(':')
        return self.queues[q_type][int(min_num):int(max_num)]
