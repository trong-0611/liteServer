import random
import pymongo
import redis
import datetime


mongodb_client = pymongo.MongoClient('localhost', 27017)

for i in range(1, 101):
  id = 'fakelevelid' + str(i)
  mongodb_client['GametaminTJCP']['LevelData'].insert_one(
                            {
                              '_id': id,
                              'data': {
                                'key': "val"
                              }
                            })
