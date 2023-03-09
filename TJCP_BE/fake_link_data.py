import random
import pymongo
import redis
import datetime


mongodb_client = pymongo.MongoClient('localhost', 27017)

for i in range(1, 101):
  uid = 'fakeuserid'+str(i)
  typ = random.randint(0,4)
  lid = 'fakeLinkId' + str(i)
  mongodb_client['GametaminTJCP']['LinkData'].insert_one(
                            {'uid': uid,
                                     'typ': typ,
                                     'lid': lid,
                                     'time': datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                                    })
