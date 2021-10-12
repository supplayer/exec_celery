from execcelery.connection import CeleryClient, Celery

app = CeleryClient('tests.celeryapp', broker='amqp://user:bitnami@0.0.0.0:5672//', include=['tests.celeryapp.tasks_final', 'tests.celeryapp.tasks_update', 'tests.celeryapp.tasks_usual'])  # noqa
app_o = Celery('tests.celeryapp', broker='amqp://user:bitnami@0.0.0.0:5672//', include=['tests.celeryapp.tasks_final', 'tests.celeryapp.tasks_update', 'tests.celeryapp.tasks_usual'])  # noqa


task_queues = {
    "streetflow_priority.streetsnap_filter": {
        "exchange": "default-exchange",
        "exchange_type": "direct",
        "routing_key": "streetflow_priority.streetsnap_filter"
    }
}

app_o.conf.update(
    task_queues=task_queues,
)

if __name__ == '__main__':
    # print(app.conf.task_default_queue)
    print(app_o.conf.task_queues)
