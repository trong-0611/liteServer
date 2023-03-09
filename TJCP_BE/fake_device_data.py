import random
import pymongo
import redis


mongodb_client = pymongo.MongoClient('localhost', 27017)
platforms = ['OSXEditor', 'OSXPlayer', 'WindowsPlayer', 'WindowsEditor', 'IOS','Android']
languages = ['Vietnamses', 'English']
operatingSystem = ['IOS 13', "Android 11"]
for i in range(1, 101):
  did = 'fakeDeviceDataId' + str(i)
  user_cnt = random.randint(0,10)
  platform = random.choices(platforms)[0]
  os = random.choices(operatingSystem)[0]
  uid = 'fakeuserid'+str(i)
  lan = random.choices(languages)[0]
  mongodb_client['GametaminTJCP']['DeviceData'].update_one({'_id': did},
                            {'$set':{'user_cnt': user_cnt,
                                     'uids':[uid],
                                     'platform': platform,
                                     'operatingSystem': os,
                                     'language':lan
                                    }},upsert=True)
