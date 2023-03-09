import random
import pymongo
import redis
import datetime


mongodb_client = pymongo.MongoClient('localhost', 27017)
#jt > st
# jt = datetime.datetime.utcnow()
# jt2 = jt + datetime.timedelta(days=2)
# print(jt.strftime("%Y-%m-%d %H:%M:%S"), jt2.strftime("%Y-%m-%d %H:%M:%S"))

for i in range(1, 100):
  tid = '_fakegiftid'+ str(i)
  title = 'title' + str(i)
  msg = 'message' + str(i)
  rewards = 'rewards' + str(i)
  cond = 'conditions' + str(i)
  limit = 0
  redeemed = random.randint(-2,2)
  state = random.randint(0,2)
  createdAt = datetime.datetime.utcnow().strftime("%d-%m-%Y %H:%M:%S")
  mongodb_client['GametaminTJCP']['RemoteGifts'].insert_one(
                            {
                              "_id": tid,
                              "title": title,
                              "msg": msg,
                              "rewards": rewards,
                              "cond": cond,
                              "limit": limit,
                              "redeemed": redeemed,
                              "state": state,
                              "createdAt": createdAt,
                            })
