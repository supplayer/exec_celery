from tests.celeryapp.config import app
from execcelery.connection import ModelQueue

usmq = ModelQueue('usual')
retrying = {'autoretry_for': (Exception,), 'max_retries': None}


@app.task(**usmq.q('usual_test1'))
def usual_test1(msg):
    print(app.current_worker_task.request.delivery_info)
    print(msg)


@app.task(**app.switch_task_meta('streetflow_priority.streetsnap_filter', 'pinterest_final_streetsnap_filter',
                                 'default-exchange'),
          **retrying)
def streetsnap_filter(msg):
    app.switch_proj(msg)


app.model_q_update(usmq)


if __name__ == '__main__':
    streetsnap_filter.delay('hahha')
