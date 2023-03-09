import random
import pymongo
import redis
import datetime


mongodb_client = pymongo.MongoClient('localhost', 27017)

for i in range(1, 101):
  uid = 'fakeuserid'+str(i)
  name = 'gametamin' + str(i)
  ava_url = ''
  mongodb_client['GametaminTJCP']['FacebookData'].insert_one(
                            {'uid': uid,
                              'name': name,
                              'ava_url': ava_url,
                            })
