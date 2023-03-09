import random
import pymongo
import redis
import datetime


mongodb_client = pymongo.MongoClient('localhost', 27017)

for i in range(1, 101):
  userId = 'fakeuserid'+ str(random.randint(0,13))
  title = 'mailTitle'+ str(i)
  msg = 'message'+ str(i)
  stt = 0
  creaTime = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

  mongodb_client['GametaminTJCP']['MailData'].insert_one(
                            {
                              'uid': userId,
                              'title': title,
                              'msg': msg,
                              'state': stt,
                              'createdAt': creaTime,
                            })
