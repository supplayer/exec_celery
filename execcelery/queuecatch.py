from execcelery.queuecontrol import QueueControl, MsgDeclare
import time
from datetime import datetime


class CatchQueues(QueueControl, MsgDeclare):
    def __init__(self, celery_app, q_rules: dict, exclude_q: list = None, include_q: list = None,
                 catch_time: int = 60, logger=None):
        super().__init__(celery_app)
        self.logger = logger or print
        self.rules = q_rules
        self.catch_time = catch_time
        self.catch_qs = set(include_q or set(self.rules.keys())) - set(exclude_q or {})
        self.control_qs = self.control_queues()
        self.swich_func = {True: self.add_consumer, False: self.cancel_consumer}
        self.control_queues_data = self.active_queue_data()

    def start_catching(self, show_time=False):
        time_record = time.time()
        self.logger(f'Active queues: {self.active_queue_names()}')
        while True:
            if time.time() >= time_record:
                active_qs_data = self.active_queue_data()
                swich_res = [self.swich_func[v](k, active_qs_data)
                             for k, v in self.swich_qs_status(active_qs_data).items() if v is not None]
                self.logger(f'\n{datetime.now() if show_time else ""} Catching Report:\n' +
                            '\n'.join(map(str, swich_res))+'\n')

                time_record = time.time() + self.catch_time

    def control_queues(self):
        catch_data = {}
        [self.__catch_q(catch_data, j, i) for i in self.rules for j in self.rules[i]['up_task']]
        return catch_data

    def catch_data(self, active_qs):
        catch_qs_count = self.__catch_qs_count()
        return {i: self.__catch_q_status(i, catch_qs_count) if i in active_qs else False
                for i in self.catch_qs}

    def control_qs_cap(self, active_qs):
        catch_data = self.catch_data(active_qs)
        return {k: self.__cap_args(v, catch_data) for k, v in self.control_qs.items()}

    def swich_qs_status(self, active_qs=None):
        active_qs = active_qs or self.active_queue_names()
        return {k: self.__swich_status(k, v, active_qs) for k, v in self.control_qs_cap(active_qs).items()}

    def __catch_q_status(self, q_name, catch_qs_count):
        if catch_qs_count[q_name] >= self.rules[q_name]['max']:
            return False
        elif catch_qs_count[q_name] < self.rules[q_name]['min']:
            return True

    def __catch_qs_count(self):
        return {i: self.tasks_count(i) for i in self.catch_qs}

    @staticmethod
    def __swich_status(q_name, q_status, active_qs):
        if q_name not in active_qs and q_status:
            return True
        elif q_name in active_qs and not q_status:
            return False
        else:
            return None

    @staticmethod
    def __cap_args(catch_qs: list, catch_data):
        status = [catch_data[i] for i in catch_qs]
        if status.count(False) > 0:
            return False
        elif status.count(True) == len(status) - status.count(None):
            return True

    @staticmethod
    def __catch_q(catch_data, item, value):
        if item not in catch_data:
            catch_data[item] = []
        catch_data[item].append(value)
