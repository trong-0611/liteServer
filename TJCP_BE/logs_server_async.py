import datetime

import aioredis
import uvicorn
from fastapi import FastAPI, Request

SUCCESS = '#0'

app = FastAPI(docs_url=None, redoc_url=None)


# async def pe3(content):


@app.get("/HeyLS", tags=["Root"])
def read_root():
    return "DevTest LS: Hi"


@app.post("/PostEvent3")
async def post_event_sql_handler_3(request: Request):
    kvs = await request.body()
    pipe = app.state.redis.multi_exec()
    pipe.rpush('pvs', kvs.decode())
    pipe.rpush('utc_time', datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
    await pipe.execute()
    return SUCCESS


@app.post("/RePostEvent3")
async def re_post_event_sql_handler_3(request: Request):
    kvs = await request.body()
    pipe = app.state.redis.multi_exec()
    pipe.rpush('pvs_rp', kvs.decode())
    pipe.rpush('utc_time_rp', datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
    await pipe.execute()
    return SUCCESS


@app.on_event("startup")
async def startup_event_handler():
    app.state.redis = await aioredis.create_redis_pool('redis://localhost/0')


@app.on_event("shutdown")
async def shutdown_event_handler():
    app.state.redis.close()
    await app.state.redis.wait_closed()


if __name__ == "__main__":
    uvicorn.run("logs_server_async:app", host="localhost", port=9090, reload=True)

# gunicorn -b localhost:9090 -w 3 -k uvicorn.workers.UvicornWorker --keep-alive 70 logs_server_async:app &

