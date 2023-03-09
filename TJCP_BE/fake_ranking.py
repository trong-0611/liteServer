import random

import pymongo
import redis

# a2 username, a3 avatarID, a4 avatarBorderID, a9 facebookID

mongodb_client = pymongo.MongoClient('localhost', 27017)
redis_client = redis.Redis('localhost', 6379)
nation_list = ['United States', 'United Kingdom', 'Vietnam']
# plf_list = ['Android', 'IPhonePlayer', 'WindowsEditor']
plf_list = ['Android']

for i in range(1, 101):
    userID = 'fakeuserid' + str(i)
    username = 'FakeUser ' + str(i)
    maxLevel = i//10+1
    totalStars = 3*maxLevel
    score = maxLevel * 30000 + totalStars
    avatarID = random.randint(0, 13)
    avatarBorderID = random.randint(0, 5)
    nation = random.choices(nation_list)[0]
    platform = random.choices(plf_list)[0]
    mongodb_client['GametaminTJCP']['UserData'].update_one({'_id': userID},
                                                           {'$set': {'a2': username, 'a3': avatarID, 'a4': avatarBorderID, 'nation': nation, 'pla':  platform}}, upsert=True)
    redis_client.zadd(platform+'INTSagaRanking', {userID: score})
    redis_client.zadd(platform + nation + 'SagaRanking', {userID: score})




