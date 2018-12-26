import asyncio
from celery.schedules import crontab
from celery import Celery
import rethinkdb as r
from datetime import datetime, timedelta
import pytz
from celery.result import AsyncResult
import logging
from kombu import Queue, Exchange

app = Celery('celery_beat', broker='amqp://guest:guest@localhost:5672//', backend='rpc://',CELERY_DEFAULT_QUEUE='high_priority',CELERY_QUEUES=(Queue('high_priority'),Queue('mid_priority'), Queue('low_priority')))

logging.basicConfig(filename='beat.log', level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

r.set_loop_type('asyncio')

tz = pytz.timezone('Asia/Kolkata')

async def connection():
    return await r.connect(db='main_db')

@app.task
def check_for_revoke():
    print('started')
    print('printing queues',app.conf.task_queues[1])
    loop = asyncio.get_event_loop()
    task = loop.create_task(connection())
    conn = loop.run_until_complete(task)

    loop1 = asyncio.get_event_loop()
    task1 = loop1.create_task(scan_db(conn))
    results = loop1.run_until_complete(task1)

async def scan_db(conn):
    now = datetime.now()
    hour_from_now = timedelta(hours=1) + now
    hour_before_now = now - timedelta(hours=1)
    high_priority = now + timedelta(minutes=20)
    mid_priority = high_priority + timedelta(minutes=20)
    low_priority = mid_priority + timedelta(minutes=20)
    #TODO:Try removing return statement from logging
    #TODO:Check for past 1 hour for confirmation.
    #TODO:Sort the certificates to be revoked in an hour based on their respective epoch and set priority.
    print ('hour from now', hour_from_now.date())
    collections = await r.table('share_assets').run(conn)
    to_be_revoked_certs = []
    while (await collections.fetch_next()):
        item = await collections.next()
        epoch_of_revoking = item['revoked_on']
        datetime_of_revoking = datetime.fromtimestamp(epoch_of_revoking, tz)
        print ('date of revoking', datetime_of_revoking.date())
        keys = ['revoked_on', 'id']
        if int(epoch_of_revoking) in range (int(now.timestamp()), int(hour_from_now.timestamp())):
            to_be_revoked_dict = dict((k, item[k]) for k in keys)
            to_be_revoked_certs.append(to_be_revoked_dict)
            #API Call
            print('Falls in 1 hour range')
#            task_res = revoke_certi_task.apply_async((item['id'], epoch_of_revoking), eta=datetime_of_revoking)
#            await task_status_logging(task_res.id, datetime_of_revoking)
        if int(epoch_of_revoking) in range(int(hour_before_now.timestamp()), int(hour_from_now.timestamp())):
            if item['revoked_flag'] == 1:
                print('revoked certificate an hour ago with cid', item['id'])

            else:
                print('unrevoked certificate with cid',item['id'])
#                task_res = revoke_certi_task.apply_async((item['id'], epoch_of_revoking), eta=datetime_of_revoking)
#                await task_status_logging(task_res.id, datetime_of_revoking)
    print(to_be_revoked_certs)
    try:
        for _dict in to_be_revoked_certs:
            datetime_of_revoking = datetime.fromtimestamp(_dict['revoked_on'], tz)
            if int(_dict['revoked_on']) in range(int(now.timestamp()), int(high_priority.timestamp())):
                print('High Priority')
                task_res = revoke_certi_task.apply_async((_dict['id'], _dict['revoked_on']), eta=datetime_of_revoking, queue=app.conf.task_queues[0])
#                await task_status_logging(task_res.id, datetime_of_revoking)

            if int(_dict['revoked_on']) in range(int(high_priority.timestamp()), int(mid_priority.timestamp())):
                print('Mid Priority')
                task_res = revoke_certi_task.apply_async((_dict['id'],_dict['revoked_on']), eta=datetime_of_revoking, queue=app.conf.task_queues[1])
#                await task_status_logging(task_res.id, datetime_of_revoking)

            if int(_dict['revoked_on']) in range(int(mid_priority.timestamp()), int(low_priority.timestamp())):
                print('Low Priority')
                task_res = revoke_certi_task.apply_async((_dict['id'], _dict['revoked_on']), eta=datetime_of_revoking, queue=app.conf.task_queues[2])
#                await task_status_logging(task_res.id, datetime_of_revoking)

    except TypeError as e:
        print ('to_be_revoked_certs list is empty',e)

@app.task
def revoke_certi_task(c_id, revoke_date):
    """It is a celery task which takes two arguments
    c_id --> Certificate id of the certificate to be revoked
    revoke_date --> Date(epoch) on which the certificate should be revoked
    """

    loop1 = asyncio.get_event_loop()
    task1 = loop1.create_task(connection())
    conn = loop1.run_until_complete(task1)
    loop2 = asyncio.get_event_loop()
    task2 = loop2.create_task(revoke_flag(c_id, conn, revoke_date))
    loop2.run_until_complete(task2)

async def revoke_flag(c_id, conn, revoke_date):
    """The function takes 2 arguments and updates the revoked_flag to 1
    c_id --> Certificate id of the certificate to be revoked
    conn --> connection to rethinkdb asyncio pool"""

    epoch_revoke_date = await (await r.table('share_assets').filter({'id' : c_id}).pluck('revoked_on').run(conn)).next()
    iso_revoke_date = datetime.fromtimestamp(epoch_revoke_date['revoked_on'], tz)
    if epoch_revoke_date['revoked_on'] == revoke_date:
        print ('API Call')
        #API Call
#        return await r.table('share_assets').filter({'id' : c_id}).update({"revoked_flag" : 1}).run(conn)
#        return await r.table(db_config['revoke_table']).filter({'c_id' : c_id}).update({"revoked_flag" : "1"}).run(conn)

    else:
        pass
        print ('pass')
#        task_res = revoke_certi_task.apply_async((c_id, epoch_revoke_date['revoked_on']),eta=iso_revoke_date)
#        await task_status_logging(task_res.id, iso_revoke_date)


async def task_status_logging(task_id, revoke_date):
    res = AsyncResult(task_id, app = app)
    logging.info('X----------X------------TASK----------------X-----------X')
    logging.info('Task has been registered with task id ' + str(task_id) + ' and will be excuted at ' + str(revoke_date))
    return

app.conf.beat_schedule = {
    "check-for-revoke-task": {
        "task": "celery_beat.check_for_revoke",
        "schedule": 20.0
    }
}

app.conf.task_queues = (
    Queue('high_priority', Exchange('high_priority'), routing_key='high_priority'),
    Queue('mid_priority', Exchange('mid_priority'), routing_key='mid_priority'),
    Queue('low_priority', Exchange('low_priority'), routing_key='low_priority')
)
app.conf.task_default_queue = 'default'
app.conf.task_default_exchange = 'default'
app.conf.task_default_routing_key = 'default'
