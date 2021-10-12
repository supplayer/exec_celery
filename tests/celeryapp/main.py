from gevent import monkey; monkey.patch_all()
from tests.celeryapp.config import app
import click


@click.command(context_settings=dict(ignore_unknown_options=True,))
@click.option('-qtype', '--queue_type', default='final', type=str, help='final')
@click.option('-qlist', '--queue_list', default=None, help='queue range', type=str)
@click.option('-qall', '--queue_all', default=False, help='all queue index', type=bool)
@click.option('-host', '--hostnum', default=1, help='set different host name for same node', type=int)
@click.option('-prefetch', '--prefetch', default=1, help='set celery worker_prefetch_multiplier', type=int)
@click.argument('celery_args', nargs=-1, type=click.UNPROCESSED)  # put celery start args at last
def start_celery(queue_type, queue_list, queue_all, hostnum, celery_args, prefetch):
    app.run(queue_type, queue_list, queue_all, hostnum, celery_args, prefetch)


if __name__ == '__main__':
    start_celery()
