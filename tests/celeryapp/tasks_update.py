from tests.celeryapp.config import app
from execcelery.connection import ModelQueue

umq = ModelQueue('update')
retrying = {'autoretry_for': (Exception,), 'max_retries': None}


@app.task(**umq.q('update_test1'))
def update_test1(msg):
    print(app.current_worker_task.request.delivery_info)
    print(msg)


app.model_q_update(umq)


if __name__ == '__main__':
    update_test1.delay(123)
