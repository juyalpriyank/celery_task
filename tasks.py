import logging
from time import strftime, gmtime
import json
import asyncio
import time
import rethinkdb as r
from celery import Celery
import datetime
from celery.result import AsyncResult

with open('config.json', 'r') as f:
    config = json.load(f)

celery_config = config['CELERY']
app = Celery(celery_config['TASK_NAME'], backend=celery_config['BACKEND'], broker= celery_config['BROKER'])

r.set_loop_type('asyncio')

logging.basicConfig(filename='celery_task.log', level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

db_config = config['DATABASE']

@app.task
def revoke_certi_task(c_id, revoke_date):
    """It is a celery task which takes one argument
    c_id --> Certificate id of the certificate to be revoked
    """

    loop1 = asyncio.get_event_loop()
    task1 = loop1.create_task(connection())
    conn = loop1.run_until_complete(task1)
    loop2 = asyncio.get_event_loop()
    task2 = loop2.create_task(revoke_flag(c_id, conn, revoke_date))
    loop2.run_until_complete(task2)
    

async def connection():
    """The function establishes a rethinkdb connection to server Asynchronously."""
    return await r.connect(db='main_db')

#    return await r.connect(host=db_config['ip'], port=db_config['port'], user=db_config['user'], password=db_config['password'], db=db_config['dbname'])

async def revoke_flag(c_id, conn, revoke_date):
    """The function takes 2 arguments and updates the revoked_flag to 1
    c_id --> Certificate id of the certificate to be revoked
    conn --> connection to rethinkdb asyncio pool"""

    epoch_revoke_date = r.table('share_assets').filter({'id' : c_id}).pluck('revoked_on').run(conn)[0]['revoked_on']
    iso_revoke_date = datetime.datetime.utcfromtimestamp(epoch_revoke_date).isoformat()
    if iso_revoke_date == revoke_date:
        return await r.table('share_assets').filter({'id' : c_id}).update({"revoked_flag" : 1}).run(conn)
    return

#    return await r.table(db_config['revoke_table']).filter({'c_id' : c_id}).update({"revoked_flag" : "1"}).run(conn)

async def change_feed_filter():
    """It is a rethinkdb Changefeed function which invokes an event whenever an entry with revoke_date
    key is inserted in the table """

    conn = await connection()
    feed = await r.table('share_assets').has_fields('revoked_on').changes().run(conn)
#    feed = await r.table(db_config['revoke_table']).has_fields('revoked_on').changes().run(conn)

    #TODO:Add filter for new entry only(old_val.eq()=None)
    #TODO:Add filter for updated entry aswell
    #TODO:Check if the revoked flag is already revoked.
    while (await feed.fetch_next()):
        change = await feed.next()
        c_id = change['new_val']['id']
        revoke_date = datetime.datetime.utcfromtimestamp(change['new_val']['revoked_on']).isoformat()
        revoked_flag_new = change['new_val']['revoked_flag']
        try:

            revoked_flag_old = change['old_val']['revoked_flag']
            if (revoked_flag_new == 1 and revoked_flag_old == 1) or (revoked_flag_new == 0 and revoked_flag_old == 0) or (revoked_flag_new == 1 and revoked_flag_old == 0):
                #if any entry in the table is updated other than revoke flags
                pass
            else:
                task_res = revoke_certi_task.apply_async((c.id,),eta=revoke_date)
                await task_status_logging(task_res.id, revoke_date)

        except KeyError:
            task_res = revoke_certi_task.apply_async((c_id,revoke_date,),eta=revoke_date)
            await task_status_logging(task_res.id, revoke_date)
       
async def task_status_logging(task_id, revoke_date):
    res = AsyncResult(task_id, app = app)
    logging.info('X----------X------------TASK----------------X-----------X')
    logging.info('Task has been registered with task id ' + str(task_id) + ' and will be excuted at ' + str(revoke_date))
    while(res.state):
        if res.state == 'SUCCESS':
            logging.info('Task with task id ' + str(task_id) + ' has been successfully executed at ' + str(datetime.datetime.now()))
            return

def main():
    loop = asyncio.get_event_loop()
    task = loop.create_task(change_feed_filter())
    loop.run_until_complete(task)

if __name__ == '__main__':
    main()

