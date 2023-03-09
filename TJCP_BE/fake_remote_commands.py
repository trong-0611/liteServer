import random
import pymongo
import redis
import datetime


mongodb_client = pymongo.MongoClient('localhost', 27017)

for i in range(1, 101):
  cmdId = 'fakecommandid'+str(i)
  userId = 'fakeuserid'+ str(random.randint(0,99))
  cmd = 'command' + str(i)
  title = 'commandTitle'+ str(i)
  msg = 'message'+ str(i)
  stt = random.randint(0,2)
  creaTime = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
  execTime = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

  mongodb_client['GametaminTJCP']['RemoteCommands'].insert_one(
                            {
                              'uid': userId,
                              'cmd': cmd,
                              'title': title,
                              'msg': msg,
                              'state': stt,
                              'createdAt': creaTime,
                              'execAt': execTime
                            })
