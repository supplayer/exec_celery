class QueueInfo:
    def __init__(self, celery_app):
        self.control = celery_app.control
        self.inspect = self.control.inspect()
        self.channel = celery_app.connection().channel()

    def active_queue_names(self):
        """
        get all active queue name.
        """
        return set(self.active_queue_data().keys())

    def registered_queue_names(self):
        """
        get all registered queue name.
        """
        return set([q.split('.', 1)[-1] for q_info in self.registered_queues().values() for q in q_info])

    def active_queues(self):
        return self.inspect.active_queues()

    def registered_queues(self):
        return self.inspect.registered()

    def active_queue_data(self):
        """
        get all active queue data with destination.
        :return {queue_name: destination}
        """
        q_data = {}
        try:
            [self.__q_data(q_data, q_key, q['name']) for q_key, q_info in self.active_queues().items() for q in q_info]
        except AttributeError:
            print('No active queues, catching...')
        finally:
            return q_data

    @staticmethod
    def __q_data(q_data, q_destination, q_name):
        if q_name not in q_data:
            q_data[q_name] = []
        q_data[q_name].append(q_destination)


class MsgDeclare(QueueInfo):
    def __init__(self, celery_app):
        super().__init__(celery_app)

    def tasks_count(self, q_name):
        return self.msg_count(q_name) + self.consumer_count(q_name)

    def msg_count(self, q_name):
        return self.__q_declare(q_name).message_count

    def consumer_count(self, q_name):
        return self.__q_declare(q_name).consumer_count

    def __q_declare(self, q_name):
        return self.channel.queue_declare(q_name, passive=True)


class QueueControl(QueueInfo):
    def __init__(self, celery_app):
        super().__init__(celery_app)

    def cancel_consumer(self, q_name, active_queue_data, reply=True, **kwargs):
        active_queue_data = active_queue_data or self.active_queue_data()
        if q_name in active_queue_data:
            return self.control.cancel_consumer(
                q_name, destination=active_queue_data[q_name], reply=reply, **kwargs)
        else:
            return f'{q_name} not actived.'

    def add_consumer(self, q_name, active_queue_data=None, reply=True, **kwargs):
        active_queue_data = active_queue_data or self.active_queue_data()
        if q_name in active_queue_data:
            return self.control.add_consumer(
                q_name, destination=active_queue_data[q_name], reply=reply, **kwargs)
        else:
            return f'{q_name} not actived.'
