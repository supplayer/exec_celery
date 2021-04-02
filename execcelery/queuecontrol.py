import time


class QueueInfo:
    def __init__(self, celery_app):
        self.inspect = celery_app.control.inspect()
        self.channel = celery_app.connection().channel()

    def queue_active_data(self):
        """
        get all active queue with destination.
        """
        queues, q_data = self.inspect.active_queues(), {}
        try:
            [self.__q_data(q_data, q_key, q['name']) for q_key, q_info in queues.items() for q in q_info]
        except AttributeError:
            print('No active queues, catching...')
        finally:
            return q_data

    def queue_active_names(self):
        """
        get all active queue name.
        """
        queues = self.inspect.active_queues()
        try:
            return set([q['name'] for q_info in queues.values() for q in q_info])
        except AttributeError:
            print('No active queues, catching...')
            return set()

    def queue_registered_names(self):
        """
        get all active queue name.
        """
        queues = self.inspect.registered()
        return set([q.split('.', 1)[-1] for q_info in queues.values() for q in q_info])

    @staticmethod
    def __q_data(q_data, q_destination, q_name):
        if q_name not in q_data:
            q_data[q_name] = []
        q_data[q_name].append(q_destination)


class MsgDeclare(QueueInfo):
    def __init__(self, celery_app, q_name):
        super().__init__(celery_app)
        self.__q_declare = self.channel.queue_declare(q_name, passive=True)
        self.queue_name = self.__q_declare.queue
        self.message_count = self.__q_declare.message_count
        self.consumer_count = self.__q_declare.consumer_count
        self.tasks_count = self.message_count + self.consumer_count


class QueueControl(QueueInfo):
    def __init__(self, celery_app):
        super().__init__(celery_app)
        self.celery_app = celery_app
        self.control_queues = self.queue_active_names()
        self.control_queues_data = self.queue_active_data()
        self.control = celery_app.control

    def catch_queues(self, q_rules: dict, exclude_q: list = None, include_q: list = None, catch_time: int = 60):
        control_queues = set(include_q or self.queue_active_names()) - set(exclude_q or {})
        active_q, time_record = self.control_queues, time.time()
        print(f'Active queues: {active_q}')
        while True:
            if time.time() >= time_record:
                active_q_data = self.queue_active_data()
                self.control_queues_data = {**self.control_queues_data, **active_q_data}
                for q_name in control_queues:
                    message_count = MsgDeclare(self.celery_app, q_name).tasks_count
                    if q_name not in active_q_data or message_count >= q_rules[q_name]['max']:
                        [self.__cancel_consumer(q, active_q_data, q_name, message_count, q_rules)
                         for q in q_rules[q_name]['up_task']]

                    if q_name in active_q_data and message_count <= q_rules[q_name]['min']:
                        [self.__add_consumer(q, active_q_data, q_name, message_count)
                         for q in q_rules[q_name]['up_task']]

                time_record = time.time() + catch_time

    def __cancel_consumer(self, q, active_q_data, q_name, message_count, q_rules):
        if q in active_q_data and q in self.control_queues_data:
            res = self.control.cancel_consumer(q, destination=self.control_queues_data[q], reply=True)
            print(f'{{q_name: {q_name}, q_count: {message_count}}}\n'
                  f'<{q_name}> has reached {q_rules[q_name]["max"]}, stoping queue <{res}> ....')

    def __add_consumer(self, q, active_q_data, q_name, message_count):
        if q not in active_q_data and q in self.control_queues_data:
            res = self.control.add_consumer(q, destination=self.control_queues_data[q], reply=True)
            print(f'{{q_name: {q_name}, q_count: {message_count}}}\n'
                  f'<{q_name}>\'s tasks has lowered preset value, will active queue <{res}> ....')
