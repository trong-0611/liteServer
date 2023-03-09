import datetime
import random

import mysql.connector
import simplejson as json
import uvicorn
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from mysql.connector import pooling
import server_config

SUCCESS = '#0'

if server_config.server_mode == 0:
    app = FastAPI()
else:
    app = FastAPI(docs_url=None, redoc_url=None)

cnx_pool = cnx_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name='my_pool',
                                                                  pool_size=32,
                                                                  host=server_config.mysql_host,
                                                                  user=server_config.mysql_user,
                                                                  password=server_config.mysql_password,
                                                                  database=server_config.mysql_database)


@app.get("/", tags=["Root"])
def read_root():
    return "Analytics Server: Welcome to the world of Traffic Jam Cars Puzzle"


http_error = \
    [status.HTTP_400_BAD_REQUEST, status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN, status.HTTP_404_NOT_FOUND,
     status.HTTP_405_METHOD_NOT_ALLOWED, status.HTTP_406_NOT_ACCEPTABLE, status.HTTP_407_PROXY_AUTHENTICATION_REQUIRED,
     status.HTTP_408_REQUEST_TIMEOUT, status.HTTP_409_CONFLICT, status.HTTP_410_GONE, status.HTTP_411_LENGTH_REQUIRED,
     status.HTTP_412_PRECONDITION_FAILED, status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
     status.HTTP_414_REQUEST_URI_TOO_LONG, status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
     status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE, status.HTTP_417_EXPECTATION_FAILED,
     status.HTTP_500_INTERNAL_SERVER_ERROR, status.HTTP_501_NOT_IMPLEMENTED, status.HTTP_502_BAD_GATEWAY,
     status.HTTP_503_SERVICE_UNAVAILABLE, status.HTTP_504_GATEWAY_TIMEOUT, status.HTTP_505_HTTP_VERSION_NOT_SUPPORTED]


@app.post("/PostEvent3")
async def post_event_sql_handler_3(request: Request):
    # return JSONResponse(status_code=random.choice(http_error), content='Fake error. Please ignore it')
    kvs = await request.json()
    # cnx = mysql.connector.connect(host=server_config.mysql_host, user=server_config.mysql_user,
    #                               password=server_config.mysql_password, database=server_config.mysql_database)
    cnx = cnx_pool.get_connection()
    cursor = cnx.cursor()
    sql = ("INSERT INTO gamelog3 "
           "(`ParamValues`, `ServerTime`) "
           "VALUES (%s, %s)")
    value = (json.dumps(kvs), datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
    cursor.execute(sql, value)
    cnx.commit()
    cursor.close()
    cnx.close()
    return SUCCESS


@app.post("/RePostEvent3")
async def re_post_event_sql_handler_3(request: Request):
    kvs = await request.body()
    # cnx = mysql.connector.connect(host=server_config.mysql_host, user=server_config.mysql_user,
    #                               password=server_config.mysql_password, database=server_config.mysql_database)
    cnx = cnx_pool.get_connection()
    cursor = cnx.cursor()
    sql = ("INSERT INTO gamelog3repost "
           "(`ParamValues`, `ServerTime`) "
           "VALUES (%s, %s)")
    value = (kvs.decode(), datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
    cursor.execute(sql, value)
    cnx.commit()
    cursor.close()
    cnx.close()
    return SUCCESS


@app.on_event("startup")
def startup_event_handler():
    pass


@app.on_event("shutdown")
def shutdown_event_handler():
    pass


if __name__ == "__main__":
    uvicorn.run("analytics_server:app", host="0.0.0.0", port=54321, reload=True)
# The UvicornWorker implementation uses the uvloop and httptools implementations
# gunicorn -b 0.0.0.0:54321 -w 1 -k uvicorn.workers.UvicornWorker --keep-alive 70 analytics_server:app
# gunicorn -b 0.0.0.0:54321 -w 1 -k uvicorn.workers.UvicornWorker analytics_server:app
