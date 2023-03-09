import random
import pymongo
import redis
import datetime


mongodb_client = pymongo.MongoClient('localhost', 27017)
#jt > st
# jt = datetime.datetime.utcnow()
# jt2 = jt + datetime.timedelta(days=2)
# print(jt.strftime("%Y-%m-%d %H:%M:%S"), jt2.strftime("%Y-%m-%d %H:%M:%S"))
event_type = ["StarRace", "EasterRace", "HalloweenRace", "XmasRace", "MedalRace"]
for i in range(1, 5):
  tid = event_type[random.randint(0,4)] + 'faketournamentid'+ str(i)
  tt = random.randint(0,4)
  now = datetime.datetime.utcnow()
  st = (now - datetime.timedelta(days=2)).strftime("%d-%m-%Y %H:%M:%S")
  jt = (now + datetime.timedelta(days=2)).strftime("%d-%m-%Y %H:%M:%S")
  et = (now + datetime.timedelta(days=5)).strftime("%d-%m-%Y %H:%M:%S")
  rt = (now + datetime.timedelta(days=10)).strftime("%d-%m-%Y %H:%M:%S")
  jp = random.randint(30000, 40000)
  mp = random.randint(40000, 50000)
  r1 = "fakeuserid1"
  r2 = "fakeuserid2"
  r3 = "fakeuserid3"
  r4 = "fakeuserid4"
  tc = random.randint(1,10)
  r5 = ""
  ca = 50
  ra =  '1.' + str(random.randint(0,3)) + '.'+str(random.randint(1,9))
  ra2 = '1.' + str(random.randint(0,3)) + '.'+str(random.randint(1,9))
  rl = random.randint(1,10)
  mlv = random.randint(10,15)
  mongodb_client['GametaminTJCP']['Tournament'].insert_one(
                            {
                              "_id": tid,
                              "tt": tt,
                              "st_e": st,
                              "jt_e": jt,
                              "et": et,
                              "rt": rt,
                              "jp": jp,
                              "mp": mp,
                              "r1": r1,
                              "r2": r2,
                              "r3": r3,
                              "r4": r4,
                              "r5": r5,
                              "tc": tc,
                              "ca": ca,
                              "ra": ra,
                              "ra2": ra2,
                              "rl": rl,
                              "mlv": mlv,
                              'tabCnt': 0
                            })
