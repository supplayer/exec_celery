from tests.celeryapp.config import app
from execcelery.connection import ModelQueue

fmq = ModelQueue('final')
retrying = {'autoretry_for': (Exception,), 'max_retries': None}


@app.task(queue='test1')
def test1(msg):
    print(app.current_worker_task.request.delivery_info)
    print(msg)


@app.task(**fmq.q('test2'))
def test2(msg):
    print(app.current_worker_task.request.delivery_info)
    print(msg)


@app.task(**fmq.q('test3'))
def test3(msg):
    print(app.current_worker_task.request.delivery_info)
    print(msg)


@app.task(queue='streetflow_priority.streetsnap_filter')
def test4(msg):
    print(app.current_worker_task.request.delivery_info)
    print(msg)


@app.task(**app.switch_task_meta('streetflow_priority.streetsnap_filter', 'pinterest_final_streetsnap_filter',
                                 'default-exchange'),
          **retrying)
def streetsnap_filter(msg):
    app.switch_proj(msg)


def msg_to_proj(msg, priority):
    app.send_task(
        **app.switch_task_meta('streetflow_priority.streetsnap_filter', 'pinterest_final_streetsnap_filter',
                               'default-exchange'),
        args=(msg,), priority=priority
    )


app.model_q_update(fmq)

if __name__ == '__main__':
    test1.delay('final test1')
