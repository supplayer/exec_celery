from execcelery.connection import CeleryClient

app = CeleryClient('tests.celeryapp', broker='pyamqp://user:bitnami@0.0.0.0:5672//', include=['tests.celeryapp.tasks'])


if __name__ == '__main__':
    print(app.conf.task_default_queue)
