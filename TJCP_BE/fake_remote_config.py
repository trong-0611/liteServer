import random
import pymongo
import redis
import datetime


mongodb_client = pymongo.MongoClient('localhost', 27017)
#jt > st
# jt = datetime.datetime.utcnow()
# jt2 = jt + datetime.timedelta(days=2)
# print(jt.strftime("%Y-%m-%d %H:%M:%S"), jt2.strftime("%Y-%m-%d %H:%M:%S"))

for i in range(1, 10):
  key = 'config' + str(i)
  default = 'defaultVal' + str(i)
  custom = []
  plfs = [0,1,2,7,8,11]
  conds = ['eq', 'gte', 'lte', 'lt', 'gt']
  for i in range(random.randint(1,3)):
    index = random.randint(0,5-i)
    plf = plfs.pop(index)
    dic ={'plf': plf,'value': random.randint(0,1)}
    if random.randint(0,1):
      dic["ver"] = '1.4'+ str(random.randint(10,70))
      dic["cond"] = conds[random.randint(0,4)]
    custom.append(dic)
  mongodb_client['GametaminTJCP']['RemoteConfigs'].insert_one(
                            {
                              "_id": key,
                              "default": default,
                              "custom": custom,
                            })
