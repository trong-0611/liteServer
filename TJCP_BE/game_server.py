import datetime
import random
import string
from typing import List, Optional

import bson
import pymongo
import redis
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from pymongo import errors, UpdateOne

import server_config

SUCCESS = '#0'
USER_NOT_FOUND = '#1'
LINK_NOT_FOUND = '#2'
LINK_FAILED = '#3'
KEY_NOT_FOUND = '#4'
VALUE_NOT_FOUND = '#5'
COUNTRY_CODE_NOT_FOUND = '#6'
DEVICE_NOT_FOUND = '#7'
GIFT_CODE_NOT_FOUND = '#8'
TOURNAMENT_NOT_FOUND = '#9'
SERVER_ERROR = '#500'
FAKE_REMAIN = 3

if server_config.server_mode == 0:
    app = FastAPI()
else:
    app = FastAPI(docs_url=None, redoc_url=None)

mongodb_client = pymongo.MongoClient(server_config.mongodb_host, server_config.mongodb_port)
user_data_table = mongodb_client[server_config.mongodb_name]['UserData']#
# facebook_data_table = mongodb_client[server_config.mongodb_name]['FacebookData']
device_data_table = mongodb_client[server_config.mongodb_name]['DeviceData']#
remote_cmd_table = mongodb_client[server_config.mongodb_name]['RemoteCommands']#
link_data_table = mongodb_client[server_config.mongodb_name]['LinkData']#
level_data_table = mongodb_client[server_config.mongodb_name]['LevelData']#
mail_data_table = mongodb_client[server_config.mongodb_name]['MailData']#
remote_configs_table = mongodb_client[server_config.mongodb_name]['RemoteConfigs']
remote_configs_pv_table = mongodb_client[server_config.mongodb_name]['RemoteConfigsPV']
tournament_data_table = mongodb_client[server_config.mongodb_name]['Tournament']#
tournament_table_data_table = mongodb_client[server_config.mongodb_name]['TournamentTable']#
remote_gift_table = mongodb_client[server_config.mongodb_name]['RemoteGifts']#
# link_2_a_table = mongodb_client[server_config.mongodb_name]['LinkToA']
b_2_a_table = mongodb_client[server_config.mongodb_name]['BToA']
redis_client = redis.Redis(host=server_config.redis_host, port=server_config.redis_port, db=server_config.redis_db,
                           decode_responses=True)


@app.get('/Hey', tags=['Root'])
def read_root():
    return 'TJCP DevTest Game Server: Hello'


class GetUDEvent(BaseModel):
    uid: str
    key: str


class GetUDSEvent(BaseModel):
    uid: str
    keys: List[str]


class SetUDEvent(BaseModel):
    uid: str
    key: str
    val: str


class SetUDEvent2(BaseModel):
    uid: str
    pv: dict


class SetUDSEvent(BaseModel):
    uid: str
    keys: List[str]
    vals: List[str]


class SetUDSEvent2(BaseModel):
    uid: str
    kvs: dict


# class GetFBEvent(BaseModel):
#     fid: str


class LogUIDEvent(BaseModel):
    uid: str


class LogDEVEvent(BaseModel):
    did: str
    plf: str
    lan: str


class UidFromDev(BaseModel):
    did: str


# class BondFBEvent(BaseModel):
#     uid: str
#     fid: str
#     fna: str
#     fav: str


class GetSRUDEvent(BaseModel):
    uids: List[str]


class UpdateSREvent(BaseModel):
    uid: str
    mlv: int
    sta: int


@app.post('/GetUD', tags=['User Data Manipulation'])
def get_user_datum_handler(event: GetUDEvent):
    try:
        response = user_data_table.find_one({'_id': event.uid}, {'_id': 0, event.key: 1})
        return response[event.key]
    except TypeError:
        return USER_NOT_FOUND
    except KeyError:
        return KEY_NOT_FOUND
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/GetUDs', tags=['User Data Manipulation'])
def get_user_data_handler(event: GetUDSEvent):
    field_to_return = {'_id': 0}
    for item in event.keys:
        field_to_return[item] = 1
    try:
        response = user_data_table.find_one({'_id': event.uid}, field_to_return)
        if response is None:
            return USER_NOT_FOUND
        else:
            return response
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/SetUD', tags=['User Data Manipulation'])
def set_user_datum_handler(event: SetUDEvent):
    try:
        response = user_data_table.update_one({'_id': event.uid}, {'$set': {event.key: event.val}})
        if response.matched_count == 1:
            return SUCCESS
        else:
            return USER_NOT_FOUND
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/SetUD2', tags=['User Data Manipulation'])
def set_user_datum_handler_2(event: SetUDEvent2):
    try:
        response = user_data_table.update_one({'_id': event.uid}, {'$set': event.pv})
        if response.matched_count == 1:
            return SUCCESS
        else:
            return USER_NOT_FOUND
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/SetUDs', tags=['User Data Manipulation'])
def set_user_data_handler(event: SetUDSEvent):
    field_to_set = {}
    for key, value in zip(event.keys, event.vals):
        field_to_set[key] = value
    try:
        response = user_data_table.update_one({'_id': event.uid}, {'$set': field_to_set})
        if response.matched_count == 1:
            return SUCCESS
        else:
            return USER_NOT_FOUND
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/SetUDs2', tags=['User Data Manipulation'])
def set_user_data_handler_2(event: SetUDSEvent2):
    try:
        response = user_data_table.update_one({'_id': event.uid}, {'$set': event.kvs})
        if response.matched_count == 1:
            return SUCCESS
        else:
            return USER_NOT_FOUND
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/DelKey', tags=['User Data Manipulation'])
def delete_user_datum_handler(event: GetUDEvent):
    try:
        response = user_data_table.update_one({'_id': event.uid}, {'$unset': {event.key: ''}})
        if response.matched_count == 1:
            return SUCCESS
        else:
            return USER_NOT_FOUND
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/DelKeys', tags=['User Data Manipulation'])
def delete_user_data_handler(event: GetUDSEvent):
    field_to_delete = {}
    for key in event.keys:
        field_to_delete[key] = ''
    try:
        response = user_data_table.update_one({'_id': event.uid}, {'$unset': field_to_delete})
        if response.matched_count == 1:
            return SUCCESS
        else:
            return USER_NOT_FOUND
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


# @app.post('/GetFB', tags=['User Data Manipulation'])
# def get_fb_info_handler(event: GetFBEvent):
#     try:
#         response = facebook_data_table.find_one({'_id': event.fid}, {'_id': 0, 'uid': 0})
#         if response is None:
#             return FACEBOOK_NOT_FOUND
#         else:
#             return response
#     except pymongo.errors.PyMongoError:
#         return SERVER_ERROR


@app.post('/Reset', tags=['User Data Manipulation'])
def reset_user_data_handler(event: LogUIDEvent):
    try:
        response = user_data_table.find_one({'_id': event.uid}, {'_id': 0, 'nation': 1, 'pla': 1})
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR
    try:
        nation = response['nation']
        platform = response['pla']
        # redis_client.zrem('INTSagaRanking', event.uid)
        # redis_client.zrem(nation + 'SagaRanking', event.uid)
        redis_client.zrem(platform + 'INTSagaRanking', event.uid)
        redis_client.zrem(platform + nation + 'SagaRanking', event.uid)
    except KeyError:
        # redis_client.zrem(platform + 'INTSagaRanking', event.uid)
        pass
    except TypeError:
        # return USER_NOT_FOUND
        pass
    try:
        user_data_table.replace_one({'_id': event.uid}, {'_id': event.uid})
        return SUCCESS
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR

class SetDevInfoEvent(BaseModel):
    did: str
    kvs: dict


@app.post('/SetDevInfo', tags=['User Data Manipulation'])
def set_device_info_handler(event: SetDevInfoEvent):
    try:
        response = device_data_table.update_one({'_id': event.did}, {'$set': event.kvs})
        if response.matched_count == 1:
            return SUCCESS
        else:
            return DEVICE_NOT_FOUND
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


class GetDevInfoEvent(BaseModel):
    did: str
    keys: List[str]


@app.post('/GetDevInfo', tags=['User Data Manipulation'])
def get_device_info_handler(event: GetDevInfoEvent):
    field_to_return = {'_id': 0}
    for item in event.keys:
        field_to_return[item] = 1
    try:
        response = device_data_table.find_one({'_id': event.did}, field_to_return)
        if response is None:
            return DEVICE_NOT_FOUND
        else:
            return response
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR

@app.post('/LogUID', tags=['Login Methods'])
def login_with_user_id_handler(event: LogUIDEvent):
    try:
        response = user_data_table.find_one({'_id': event.uid}, {'_id': 1})
        return response['_id']
    except TypeError:
        return USER_NOT_FOUND
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


# @app.post('/LogFB', tags=['Login Methods'])
# def login_with_fb_handler(event: GetFBEvent):
#     try:
#         response = facebook_data_table.find_one({'_id': event.fid}, {'_id': 0, 'uid': 1})
#         return response['uid']
#     except TypeError:
#         return USER_NOT_FOUND
#     except pymongo.errors.PyMongoError:
#         return SERVER_ERROR


# @app.post('/LogDEV', tags=['Login Methods'])
# def login_with_device_handler(event: LogDEVEvent):
#     try:
#         user_id = event.did + '_' + str(1) + chr(random.randrange(97, 97 + 26))
#         device_data_table.insert_one({'_id': event.did, 'platform': event.plf, 'cnt': 1, user_id: 1})
#     except pymongo.errors.DuplicateKeyError:
#         counter = device_data_table.find_one({'_id': event.did}, {'_id': 0, 'cnt': 1})['cnt']
#         user_id = event.did + '_' + str(counter + 1) + chr(random.randrange(97, 97 + 26))
#         device_data_table.update_one({'_id': event.did}, {'$inc': {'cnt': 1}, '$set': {user_id: 1}})
#     except pymongo.errors.PyMongoError:
#         return SERVER_ERROR
#     try:
#         user_data_table.insert_one({'_id': user_id})
#         return user_id
#     except pymongo.errors.PyMongoError:
#         return SERVER_ERROR


@app.post('/LogDEV2', tags=['Login Methods'])
def login_with_device_handler_2(event: LogDEVEvent):
    if event.did[0] == '_':
        device_id = event.did[1:]
    else:
        device_id = event.did
    if len(device_id) < 16:
        device_id += device_id[0] * (16 - len(device_id))
    device_platform = event.plf
    device_language = event.lan
    try:
        temp_user_id = device_id[0:16] + device_platform[0] + ''.join(
            random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits)) + server_config.cnt_to_char[
                           0] \
                       + ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits))
        user_id = ''
        for char in temp_user_id:
            user_id += server_config.lookup_tab[char]
        if event.did[0] == '_':
            user_id = '_' + user_id
        device_data_table.insert_one(
            {'_id': event.did, 'user_cnt': 1, 'uids': [user_id], 'platform': device_platform,
             'language': device_language})
    except pymongo.errors.DuplicateKeyError:
        counter = device_data_table.find_one({'_id': event.did}, {'_id': 0, 'user_cnt': 1})['user_cnt']
        # if counter < 10:
        #     counter_char = str(counter)
        # else:
        #     counter_char = chr(counter + 55)
        temp_user_id = device_id[0:16] + device_platform[0] + ''.join(
            random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits)) + server_config.cnt_to_char[
                           counter % 62] \
                       + ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits))
        user_id = ''
        for char in temp_user_id:
            user_id += server_config.lookup_tab[char]
        if event.did[0] == '_':
            user_id = '_' + user_id
        device_data_table.update_one({'_id': event.did}, {'$inc': {'user_cnt': 1}, '$push': {'uids': user_id}})
    try:
        user_data_table.insert_one(
            {'_id': user_id, 'pla': device_platform,
             'createdAt': datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")})
        return user_id
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


class LogLIDEvent(BaseModel):
    typ: int
    lid: str


@app.post('/LogLID', tags=['Login Methods'])
def login_with_link_id_handler(event: LogLIDEvent):
    response = link_data_table.find_one({'typ': event.typ, 'lid': event.lid})
    if response is None:
        return LINK_NOT_FOUND
    else:
        return response['uid']

@app.post('/GetUIDs', tags=['Login Methods'])
def get_uids_from_device_id_handler(event: UidFromDev):
    device_id = event.did
    try:
        list_of_uid = device_data_table.find_one({'_id': device_id}, {'_id': 0, 'uids': 1})['uids']
        return list_of_uid
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR
    except TypeError:
        return DEVICE_NOT_FOUND

class LinkEvent(BaseModel):
    uid: str
    typ: int
    lid: str


@app.post('/Link', tags=['Linking'])
def link_handler(event: LinkEvent):
    response1 = link_data_table.find_one({'typ': event.typ, 'lid': event.lid})
    response2 = link_data_table.find_one({'typ': event.typ, 'uid': event.uid})
    if response1 is None and response2 is None:
        link_data_table.insert_one({'uid': event.uid, 'typ': event.typ, 'lid': event.lid,
                                    'time': datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")})
        return SUCCESS
    if response1 is not None:
        if response1['uid'] == event.uid:
            return SUCCESS
    if response2 is not None:
        if response2['lid'] == event.lid:
            return SUCCESS
    return LINK_FAILED


@app.post('/Unlink', tags=['Linking'])
def unlink_handler(event: LinkEvent):
    response = link_data_table.delete_one({'uid': event.uid, 'typ': event.typ, 'lid': event.lid})
    if response.deleted_count == 1:
        return SUCCESS
    else:
        return LINK_NOT_FOUND


@app.post('/GetAllLinks', tags=['Linking'])
def get_all_link_handler(event: LogUIDEvent):
    response = link_data_table.find({'uid': event.uid}, {'_id': 0, 'uid': 0, 'time': 0})
    list_to_return = []
    for item in response:
        list_to_return.append(item)
    return list_to_return


@app.post('/GetAllLinkData', tags=['DB Admin'])
def get_all_link_data_handler():
    response = link_data_table.find({}, {'_id': 0})
    list_to_return = []
    for item in response:
        list_to_return.append(item)
    return list_to_return


@app.post('/GetAllUserIDs', tags=['DB Admin'])
def get_all_user_ids_handler():
    try:
        response = user_data_table.distinct('_id')
        return response
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


class GetAllUserIDsFrom(BaseModel):
    frm: str


@app.post('/GetAllUserIDsFromDate', tags=['DB Admin'])
def get_all_user_ids_from_date_handler(event: GetAllUserIDsFrom):
    date = event.frm.split('/')
    if len(date) != 3:
        return SERVER_ERROR
    date_iso = date[2] + '-' + date[1] + '-' + date[0] + ' 00:00:00'

    try:
        response = user_data_table.distinct('_id', {'createdAt': {'$gte': date_iso}})
        return response
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/GetAllDeviceIDs', tags=['DB Admin'])
def get_all_device_ids_handler():
    try:
        response = device_data_table.distinct('_id')
        return response
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/GetAllUserData', tags=['DB Admin'])
def get_all_user_data_handler():
    try:
        cur = user_data_table.find({})
        response = []
        for item in cur:
            response.append(item)
        return response
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/GetAllDeviceData', tags=['DB Admin'])
def get_all_device_data_handler():
    try:
        cur = device_data_table.find({})
        response = []
        for item in cur:
            response.append(item)
        return response
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/DeleteDeviceData', tags=['DB Admin'])
def delete_device_data_handler(event: UidFromDev):
    try:
        res = device_data_table.find_one({'_id': event.did}, {'uids': 1})
        uids = res['uids']
        for user_id in uids:
            response = user_data_table.find_one({'_id': user_id}, {'_id': 0, 'nation': 1, 'pla': 1})
            platform = response['pla']
            nation = response['nation']
            redis_client.zrem(platform + 'INTSagaRanking', user_id)
            redis_client.zrem(platform + nation + 'SagaRanking', user_id)
            user_data_table.delete_one({'_id': user_id})
        device_data_table.delete_one({'_id': event.did})
        return SUCCESS
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/DeleteUserData', tags=['DB Admin'])
def delete_user_data_handler(event: LogUIDEvent):
    try:
        response = user_data_table.find_one({'_id': event.uid}, {'_id': 0, 'nation': 1, 'pla': 1})
        platform = response['pla']
        nation = response['nation']
        redis_client.zrem(platform + 'INTSagaRanking', event.uid)
        redis_client.zrem(platform + nation + 'SagaRanking', event.uid)
        user_data_table.delete_one({'_id': event.uid})
        res = device_data_table.find_one({'uids': event.uid}, {'_id': 1, 'user_cnt': 1, 'uids': 1})
        device_id = res['_id']
        uids = res['uids']
        uids.remove(event.uid)
        user_cnt = res['user_cnt'] - 1
        if res['user_cnt'] == 1:
            device_data_table.delete_one({'_id': device_id})
        else:
            device_data_table.update_one({'_id': res['_id']},
                                         {'$set': {'uids': uids, 'user_cnt': user_cnt}})
        return SUCCESS
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR

# @app.post('/BondFB', tags=['Linking UserID to FacebookID'])
# def bond_uid_fid_handler(event: BondFBEvent):
#     uid_linked = None
#     fid_linked = None
#     try:
#         res_1 = facebook_data_table.find_one({'_id': event.fid}, {'_id': 0, 'uid': 1})
#         uid_linked = res_1['uid']
#     except TypeError:
#         pass
#     except KeyError:
#         pass
#     except pymongo.errors.PyMongoError:
#         return SERVER_ERROR
#     try:
#         res_2 = facebook_data_table.find_one({'uid': event.uid}, {'_id': 1})
#         fid_linked = res_2['_id']
#     except TypeError:
#         pass
#     except KeyError:
#         pass
#     except pymongo.errors.PyMongoError:
#         return SERVER_ERROR
#     if uid_linked is None and fid_linked is None:
#         try:
#             facebook_data_table.insert_one(
#                 {'_id': event.fid, 'uid': event.uid, 'name': event.fna, 'ava_url': event.fav})
#             return SUCCESS
#         except pymongo.errors.PyMongoError:
#             return SERVER_ERROR
#     if uid_linked == event.uid and fid_linked == event.fid:
#         try:
#             facebook_data_table.update_one({'_id': event.fid}, {'$set': {'name': event.fna, 'ava_url': event.fav}})
#             return SUCCESS
#         except pymongo.errors.PyMongoError:
#             return SERVER_ERROR
#     return LINK_FB_FAILED


@app.post('/GetSRUD', tags=['Saga Ranking'])
def get_saga_ranking_user_data_handler(event: GetSRUDEvent):
    try:
        response = user_data_table.find({'_id': {'$in': event.uids}},
                                        {'_id': 1, 'a2': 1, 'a3': 1, 'a4': 1, 'a9': 1, 'a21': 1, 'a42': 1, 'l31': 1})
        data_to_return = []
        for record in response:
            record['id'] = record.pop('_id')
            data_to_return.append(record)
        return data_to_return
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/UpdateSR', tags=['Saga Ranking'])
def update_saga_ranking_handler(event: UpdateSREvent):
    try:
        response = user_data_table.find_one({'_id': event.uid}, {'_id': 0, 'nation': 1, 'pla': 1})
        if response is None:
            return USER_NOT_FOUND
        score = event.mlv * 30000 + event.sta
        platform = response['pla']
        redis_client.zadd(platform + 'INTSagaRanking', {event.uid: score})
        nation = response['nation']
        redis_client.zadd(platform + nation + 'SagaRanking', {event.uid: score})
    except KeyError:
        pass
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR
    return SUCCESS


class GetSREvent(BaseModel):
    uid: str
    zne: int
    top: int
    ard: int


@app.post('/GetSR', tags=['Saga Ranking'])
def get_saga_ranking_handler(event: GetSREvent):
    if event.zne == 1: #local
        try:
            response = user_data_table.find_one({'_id': event.uid}, {'_id': 0, 'nation': 1, 'pla': 1})
            if response is None:
                return USER_NOT_FOUND
            platform = response['pla']
            nation = response['nation']
        except KeyError:
            return COUNTRY_CODE_NOT_FOUND
        except pymongo.errors.PyMongoError:
            return SERVER_ERROR
    else: #global
        response = user_data_table.find_one({'_id': event.uid}, {'_id': 0, 'pla': 1})
        platform = response['pla']
        nation = 'INT'
    top = event.top - 1
    around = event.ard
    # No matter what happens, always return top players
    lb_top = redis_client.zrange(platform + nation + 'SagaRanking', 0, top, desc=True, withscores=True)
    lb_top_json = []
    # print(">>> ", lb_top[1:5] ) #[(uid, score), ()]
    current_rank = 1
    for item in lb_top:
        new_item = {'id': item[0], 'rank': current_rank, 'maxLevel': int((item[1])) // 30000,
                    'stars': int(item[1]) % 30000}
        lb_top_json.append(new_item)
        current_rank += 1
    user_rank = redis_client.zrevrank(platform + nation + 'SagaRanking', event.uid)
    # print(">>>: ", user_rank) zrevrank: return rank with start at 0
    if user_rank is None:
        return USER_NOT_FOUND
    if user_rank <= top:
        return {'TOP': lb_top_json, 'AR': {}}
    else:
        around_half = around // 2
        if user_rank - top > around_half:
            # (user_rank - around_half, user_rank + around_half)
            lb_around = redis_client.zrange(platform + nation + 'SagaRanking', user_rank - around_half,
                                            user_rank + around_half, desc=True, withscores=True)
            lb_around_json = []
            current_rank = user_rank - around_half + 1
            for item in lb_around:
                new_item = {'id': item[0], 'rank': current_rank, 'maxLevel': int((item[1])) // 30000,
                            'stars': int(item[1]) % 30000}
                lb_around_json.append(new_item)
                current_rank += 1
            return {'TOP': lb_top_json, 'AR': lb_around_json}
        else:
            # (top + 1, top + around + 1)
            lb_around = redis_client.zrange(platform + nation + 'SagaRanking', top + 1,
                                            top + around + 1, desc=True, withscores=True)
            lb_around_json = []
            current_rank = top + 2
            for item in lb_around:
                new_item = {'id': item[0], 'rank': current_rank, 'maxLevel': int((item[1])) // 30000,
                            'stars': int(item[1]) % 30000}
                lb_around_json.append(new_item)
                current_rank += 1
            return {'TOP': lb_top_json, 'AR': lb_around_json}


class GetSR2Event(BaseModel):
    uid: str
    zne: int
    ard: int
    gmx: int


@app.post('/GetSR2', tags=['Saga Ranking'])
def get_saga_ranking_2_handler(event: GetSR2Event):
    if event.zne == 1:
        try:
            response = user_data_table.find_one({'_id': event.uid}, {'_id': 0, 'nation': 1, 'pla': 1})
            if response is None:
                return USER_NOT_FOUND
            platform = response['pla']
            nation = response['nation']
        except KeyError:
            return COUNTRY_CODE_NOT_FOUND
        except pymongo.errors.PyMongoError:
            return SERVER_ERROR
    else:
        response = user_data_table.find_one({'_id': event.uid}, {'_id': 0, 'pla': 1})
        platform = response['pla']
        nation = 'INT'

    user_score = redis_client.zscore(platform + nation + 'SagaRanking', event.uid)
    user_lv = int(user_score) // 30000
    print(user_lv)

    # upper = redis_client.zrange(platform + nation + 'SagaRanking',
    #                             (user_lv + 1) * 30000,
    #                             (user_lv + event.ard // 2) * 30000 + (user_lv + event.ard // 2) * 3,
    #                             desc=False, withscores=True, byscore=True)
    upper = redis_client.zrangebyscore(platform + nation + 'SagaRanking',
                                       (user_lv + 1) * 30000,
                                       (user_lv + event.ard * 2) * 30000 + (user_lv + event.ard // 2) * 3,
                                       withscores=True)
    # lower = redis_client.zrange(platform + nation + 'SagaRanking',
    #                             (max(1, user_lv - event.ard // 2)) * 30000,
    #                             (user_lv - 1) * 30000 + (user_lv - 1) * 3,
    #                             desc=False, withscores=True, byscore=True)
    lower = redis_client.zrangebyscore(platform + nation + 'SagaRanking',
                                       (max(1, user_lv - event.ard * 2)) * 30000,
                                       (user_lv - 1) * 30000 + (user_lv - 1) * 3,
                                       withscores=True)
    # print(upper)
    # print(lower)
    res = []
    grp_cnt = 0
    cur_lvl = user_lv - 1
    mem_cnt = 0
    for item in reversed(lower):
        if int(item[1]) // 30000 == cur_lvl and grp_cnt == event.gmx:
            continue
        elif int(item[1]) // 30000 != cur_lvl:
            grp_cnt = 1
            cur_lvl = int(item[1]) // 30000
            new_item = {'id': item[0], 'maxLevel': int((item[1])) // 30000, 'stars': int(item[1]) % 30000}
            res.append(new_item)
            mem_cnt += 1
        else:
            grp_cnt += 1
            new_item = {'id': item[0], 'maxLevel': int((item[1])) // 30000, 'stars': int(item[1]) % 30000}
            res.append(new_item)
            mem_cnt += 1
        if mem_cnt >= event.ard // 2:
            break
    res = res[::-1]
    res.append({'id': event.uid, 'maxLevel': int(user_score) // 30000, 'stars': int(user_score) % 30000})
    grp_cnt = 0
    cur_lvl = user_lv + 1
    mem_cnt = 0
    for item in upper:
        if int(item[1]) // 30000 == cur_lvl and grp_cnt == event.gmx:
            continue
        elif int(item[1]) // 30000 != cur_lvl:
            grp_cnt = 1
            cur_lvl = int(item[1]) // 30000
            new_item = {'id': item[0], 'maxLevel': int((item[1])) // 30000, 'stars': int(item[1]) % 30000}
            res.append(new_item)
            mem_cnt += 1
        else:
            grp_cnt += 1
            new_item = {'id': item[0], 'maxLevel': int((item[1])) // 30000, 'stars': int(item[1]) % 30000}
            res.append(new_item)
            mem_cnt += 1
        if mem_cnt >= event.ard // 2:
            break
    return res[::-1]


class UpdateNation(BaseModel):
    uid: str
    nation: str


@app.post('/SetNation', tags=['Saga Ranking'])
def set_nation_saga_ranking_handler(event: UpdateNation):
    try:
        response = user_data_table.find_one({'_id': event.uid}, {'_id': 0, 'nation': 1})
        if response is None:
            return USER_NOT_FOUND
        old_nation = response['nation']
    except KeyError:
        old_nation = None
    try:
        response = user_data_table.update_one({'_id': event.uid}, {'$set': {'nation': event.nation}})
        if response.matched_count == 1:
            pass
        else:
            return USER_NOT_FOUND
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR
    if old_nation is not None:
        redis_client.zrem(old_nation + 'SagaRanking', event.uid)
    score = redis_client.zscore('INTSagaRanking', event.uid)
    if score is not None:
        redis_client.zadd(event.nation + 'SagaRanking', {event.uid: score})
    return SUCCESS



class GetUsersValEvent(BaseModel):
    uids: List[str]
    key: str


@app.post('/GetValues', tags=['Saga Ranking'])
def get_values_many_usr_one_key_handler(event: GetUsersValEvent):
    try:
        response = user_data_table.find({'_id': {'$in': event.uids}}, {'_id': 1, event.key: 1})
        data_to_return = []
        for record in response:
            record['id'] = record.pop('_id')
            data_to_return.append(record)
        return data_to_return
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/RemoveSR', tags=['Saga Ranking for Unity Editor'])
def remove_saga_ranking_handler(event: LogUIDEvent):
    try:
        response = user_data_table.find_one({'_id': event.uid}, {'_id': 0, 'nation': 1, 'pla': 1})
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR
    try:
        if 'nation' in response:
            nation = response['nation']
        else:
            nation = None
        if 'pla' in response:
            platform = response['pla']
        else:
            platform = None
        # redis_client.zrem('INTSagaRanking', event.uid)
        # redis_client.zrem(nation + 'SagaRanking', event.uid)
        if platform is not None:
            redis_client.zrem(platform + 'INTSagaRanking', event.uid)
        if nation is not None:
            redis_client.zrem(platform + nation + 'SagaRanking', event.uid)
        return SUCCESS
    except KeyError:
        # redis_client.zrem(platform + 'INTSagaRanking', event.uid)
        return SUCCESS
    except TypeError:
        # return USER_NOT_FOUND
        return SUCCESS


@app.delete('/RemoveManySR', tags=['Saga Ranking for Unity Editor'])
def remove_many_saga_ranking_handler(event: GetSRUDEvent):
    for user_id in event.uids:
        res = user_data_table.find_one({'_id': user_id}, {'_id': 0, 'nation': 1, 'pla': 1})
        if 'pla' in res:
            redis_client.zrem(res['pla'] + 'INTSagaRanking', user_id)
        if 'pla' in res and 'nation' in res:
            redis_client.zrem(res['pla'] + res['nation'] + 'SagaRanking', user_id)
    return SUCCESS


class GetGlobalRankingEvent(BaseModel):
    plf: str
    frm: int
    to: int


@app.post('/GetGlobalRanking', tags=['Saga Ranking for Unity Editor'])
def get_global_ranking_handler(event: GetGlobalRankingEvent):
    lb_top = redis_client.zrange(event.plf + 'INT' + 'SagaRanking', event.frm - 1, event.to - 1, desc=True,
                                 withscores=True)
    lb_top_json = []
    current_rank = event.frm
    for item in lb_top:
        new_item = {'id': item[0], 'rank': current_rank, 'maxLevel': int((item[1])) // 30000,
                    'stars': int(item[1]) % 30000}
        lb_top_json.append(new_item)
        current_rank += 1
    return lb_top_json


class GetSagaRankingFakeIDsEvent(BaseModel):
    plf: str


@app.post('/GetSRFakeIDs', tags=['Saga Ranking for Unity Editor'])
def get_saga_ranking_fake_ids_handler(event: GetSagaRankingFakeIDsEvent):
    platform = event.plf
    nation = 'INT'
    lb_top = redis_client.zrange(platform + nation + 'SagaRanking', 0, -1, desc=True, withscores=False)
    fake_ids = filter(lambda x: x[0:5] == 'fake_', lb_top)
    return list(fake_ids)




class UserEvent(BaseModel):
    uid: str


@app.post('/GetCMDs', tags=['Remote Commands'])
def get_commands_handler(event: UserEvent):
    response = remote_cmd_table.find({'uid': event.uid, 'state': 0}, {'uid': 0, 'state': 0})
    list_to_return = []
    for item in response:
        item['cid'] = str(item.pop('_id'))
        list_to_return.append(item)
    remote_cmd_table.update_many({'uid': event.uid, 'state': 0}, {'$set': {'state': 1}})
    return list_to_return


class CMDEvent(BaseModel):
    cid: str


@app.post('/SetCmdExec', tags=['Remote Commands'])
def set_command_executed_handler(event: CMDEvent):
    try:
        response = remote_cmd_table.update_one({'_id': bson.ObjectId(event.cid)},
                                               {'$set': {'state': 2, 'execAt': datetime.datetime.utcnow().strftime(
                                                   "%Y-%m-%d %H:%M:%S")}})
        if response.matched_count == 1:
            return SUCCESS
        else:
            return SERVER_ERROR
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/GetAllCmds', tags=['Remote Commands for Unity Editor'])
def get_all_commands_handler(event: UserEvent):
    try:
        response = remote_cmd_table.find({'uid': event.uid}, {'uid': 0})
        list_to_return = []
        for item in response:
            item['cid'] = str(item.pop('_id'))
            list_to_return.append(item)
        return list_to_return
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


class AddCmdEvent(BaseModel):
    uid: str
    cmd: str
    msg: str
    title: str


@app.post('/AddCmd', tags=['Remote Commands for Unity Editor'])
def add_command_handler(event: AddCmdEvent):
    try:
        response = user_data_table.find_one({'_id': event.uid}, {'_id': 1})
        if response is None:
            return USER_NOT_FOUND
        else:
            remote_cmd_table.insert_one({'uid': event.uid, 'msg': event.msg, 'cmd': event.cmd,
                                         'state': 0, 'title': event.title,
                                         'createdAt': datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")})
            return SUCCESS
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


class UpdateCmdEvent(BaseModel):
    cid: str
    cmd: str
    msg: str
    title: str
    state: int


@app.post('/UpdateCmd', tags=['Remote Commands for Unity Editor'])
def update_command_handler(event: UpdateCmdEvent):
    try:
        response = remote_cmd_table.update_one({'_id': bson.ObjectId(event.cid)},
                                               {'$set': {'cmd': event.cmd, 'msg': event.msg, 'state': event.state,
                                                         'title': event.title}})
        if response.matched_count == 1:
            return SUCCESS
        else:
            return SERVER_ERROR

    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/RemoveCmd', tags=['Remote Commands for Unity Editor'])
def remove_command_handler(event: CMDEvent):
    try:
        response = remote_cmd_table.delete_one({'_id': bson.ObjectId(event.cid)})
        if response.deleted_count == 1:
            return SUCCESS
        else:
            return SERVER_ERROR
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/RemoveAllCmds', tags=['Remote Commands for Unity Editor'])
def remove_all_commands_handler(event: UserEvent):
    try:
        remote_cmd_table.delete_many({'uid': event.uid})
        return SUCCESS
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/GetAllCmdUIDs', tags=['Remote Commands for Unity Editor'])
def get_all_command_user_ids_handler():
    try:
        response = remote_cmd_table.distinct('uid')
        return response
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


class GetLevelEvent(BaseModel):
    levelID: str


@app.post('/GetLevel', tags=['Level Data'])
def get_level_handler(event: GetLevelEvent):
    response = level_data_table.find_one({'_id': event.levelID}, {'_id': 0})
    return response


class GetLevelsEvent(BaseModel):
    prefix: str
    frm: int
    to: int


@app.post('/GetLevels', tags=['Level Data'])
def get_levels_handler(event: GetLevelsEvent):
    level_list = [event.prefix + str(i) for i in range(event.frm, event.to + 1)]
    response = level_data_table.find({'_id': {'$in': level_list}})
    data_list = []
    for item in response:
        data_list.append(item)
    return data_list


@app.post('/GetAllLevels', tags=['Level Data'])
def get_all_levels_handler():
    response = level_data_table.find({})
    data_list = []
    for item in response:
        data_list.append(item)
    return data_list


class SetLevelEvent(BaseModel):
    levelID: str
    data: str


@app.post('/SetLevel', tags=['Level Data for Unity Editor'])
def set_level_handler(event: SetLevelEvent):
    try:
        response = level_data_table.update_one({'_id': event.levelID}, {'$set': {'data': event.data}})
        if response.matched_count == 1:
            return SUCCESS
        else:
            return SERVER_ERROR
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


class SetLevelsEvent(BaseModel):
    idjson: dict


@app.post('/SetLevels', tags=['Level Data for Unity Editor'])
def set_levels_handler(event: SetLevelsEvent):
    requests = []
    for key in event.idjson:
        requests.append(UpdateOne({'_id': key}, {'$set': {'data': event.idjson[key]}}))
    try:
        level_data_table.bulk_write(requests)
        return SUCCESS
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/RemoveLevel', tags=['Level Data for Unity Editor'])
def remove_level_handler(event: GetLevelEvent):
    try:
        level_data_table.delete_one({'_id': event.levelID})
        return SUCCESS
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/RemoveLevels', tags=['Level Data for Unity Editor'])
def remove_levels_handler(event: GetLevelsEvent):
    level_list = [event.prefix + str(i) for i in range(event.frm, event.to + 1)]
    try:
        level_data_table.delete_many({'_id': {'$in': level_list}})
        return SUCCESS
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/GetMails', tags=['Mail System'])
def get_mails_handler(event: UserEvent):
    response = mail_data_table.find({'uid': event.uid, 'state': {'$ne': 5}},
                                    {'uid': 0})
    list_to_return = []
    list_to_in_progress = []
    for item in response:
        item['id'] = str(item.pop('_id'))
        if item['state'] == 0:
            item['state'] = 1
            list_to_in_progress.append(bson.ObjectId(item['id']))
        list_to_return.append(item)
    mail_data_table.update_many({'_id': {'$in': list_to_in_progress}}, {'$set': {'state': 1}})
    return list_to_return


class SetMailStateEvent(BaseModel):
    mid: str
    state: int


@app.post('/SetMailState', tags=['Mail System'])
def set_mail_state_handler(event: SetMailStateEvent):
    try:
        response = mail_data_table.update_one({'_id': bson.ObjectId(event.mid)}, {'$set': {'state': event.state}})
        if response.matched_count == 1:
            return SUCCESS
        else:
            return SERVER_ERROR
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


class AddMailEvent(BaseModel):
    uid: str
    title: str
    msg: str


@app.post('/AddMail', tags=['Mail System for Unity Editor'])
def add_mail_handler(event: AddMailEvent):
    try:
        response = user_data_table.find_one({'_id': event.uid}, {'_id': 1})
        if response is None:
            return USER_NOT_FOUND
        else:
            mail_data_table.insert_one({'uid': event.uid, 'msg': event.msg,
                                        'state': 0, 'title': event.title,
                                        'createdAt': datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")})
            return SUCCESS
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


class UpdateMailEvent(BaseModel):
    mid: str
    title: str
    msg: str
    state: int


@app.post('/UpdateMail', tags=['Mail System for Unity Editor'])
def update_mail_handler(event: UpdateMailEvent):
    try:
        response = mail_data_table.update_one({'_id': bson.ObjectId(event.mid)},
                                              {'$set': {'title': event.title, 'msg': event.msg, 'state': event.state}})
        if response.matched_count == 1:
            return SUCCESS
        else:
            return SERVER_ERROR
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/GetAllMails', tags=['Mail System for Unity Editor'])
def get_all_mails_handler(event: UserEvent):
    response = mail_data_table.find({'uid': event.uid}, {'uid': 0})
    list_to_return = []
    for item in response:
        item['id'] = str(item.pop('_id'))
        list_to_return.append(item)
    return list_to_return


class MailEvent(BaseModel):
    mid: str


@app.post('/RemoveMail', tags=['Mail System for Unity Editor'])
def remove_mail_handler(event: MailEvent):
    try:
        response = mail_data_table.delete_one({'_id': bson.ObjectId(event.mid)})
        if response.deleted_count == 1:
            return SUCCESS
        else:
            return SERVER_ERROR
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/RemoveAllMails', tags=['Mail System for Unity Editor'])
def remove_all_mails_of_user_handler(event: UserEvent):
    try:
        mail_data_table.delete_many({'uid': event.uid})
        return SUCCESS
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/GetAllMailUserIDs', tags=['Mail System for Unity Editor'])
def get_all_mail_user_ids_handler():
    try:
        response = mail_data_table.distinct('uid')
        return response
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


class GetConfigsEvent(BaseModel):
    pla: str
    ver: str

@app.post('/GetConfigs', tags=['Remote Configs'])
def get_config_handler(event: GetConfigsEvent):
    res = remote_configs_pv_table.find_one({'_id': event.pla + '_' + event.ver}, {'_id': 0})
    if res is None:
        return SERVER_ERROR
    else:
        return res


def gle(v1, v2):
    # 0: greater than, 1 less than, 2 equal
    [v11, v12, v13] = [int(s1) for s1 in v1.split('.')]
    [v21, v22, v23] = [int(s2) for s2 in v2.split('.')]
    if v11 > v21:
        return 0
    elif v11 < v21:
        return 1
    else:
        if v12 > v22:
            return 0
        elif v12 < v22:
            return 1
        else:
            if v13 > v23:
                return 0
            elif v13 < v23:
                return 1
            else:
                return 2


def match(ver, cond, ap_ver):
    comp_res = gle(ap_ver, ver)
    if comp_res == 2:
        if cond in ['eq', 'gte', 'lte']:
            return True
        else:
            return False
    elif comp_res == 1:
        if cond in ['lt', 'lte']:
            return True
        else:
            return False
    else:
        if cond in ['gt', 'gte']:
            return True
        else:
            return False


def update_all():
    res = remote_configs_pv_table.find({}, {'_id': 1})
    response = remote_configs_table.find({})
    list_of_config = [item for item in response]
    for pv in res:
        [platform, version] = pv['_id'].split('_')
        for config in list_of_config:
            # print(config)
            if platform in config:
                for spec in config[platform]:
                    if match(spec['ver'], spec['cond'], version):
                        remote_configs_pv_table.update_one({'_id': platform + '_' + version},
                                                           {'$set': {config['_id']: spec['val']}}, upsert=True)
                        break
            else:
                remote_configs_pv_table.update_one({'_id': platform + '_' + version},
                                                   {'$set': {config['_id']: config['default']}}, upsert=True)



@app.post('/AddNewPV', tags=['Remote Configs for Unity Editor'])
def add_new_pv_handler(event: GetConfigsEvent):
    try:
        response = remote_configs_table.find({}, {'_id': 1, event.pla: 1, 'default': 1})
        for config in response:
            if event.pla in config:
                for spec in config[event.pla]:
                    if match(spec['ver'], spec['cond'], event.ver):
                        remote_configs_pv_table.update_one({'_id': event.pla + '_' + event.ver},
                                                           {'$set': {config['_id']: spec['val']}}, upsert=True)
                        break
            else:
                remote_configs_pv_table.update_one({'_id': event.pla + '_' + event.ver},
                                                   {'$set': {config['_id']: config['default']}}, upsert=True)
        return 'SUCCESS'
    except pymongo.errors.PyMongoError:
        return 'SERVER_ERROR'


@app.post('/GetAllConfigs', tags=['Remote Configs for Unity Editor'])
def get_all_configs_handler():
    try:
        res = remote_configs_table.find({})
        list_to_ret = []
        for item in res:
            list_to_ret.append(item)
        return list_to_ret
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


class AddConfigEvent(BaseModel):
    key: str
    dval: Optional[int] = None
    pla: Optional[str] = None
    ver: Optional[str] = None
    cond: Optional[str] = None
    val: Optional[int] = None


@app.post('/AddConfig', tags=['Remote Configs for Unity Editor'])
def add_config_handler(event: AddConfigEvent):
    if event.dval is not None:
        remote_configs_table.insert_one({'_id': event.key, 'default': event.dval})
        update_all()
        return SUCCESS
    if event.cond is not None and event.ver is not None:
        temp = {'ver': event.ver, 'cond': event.cond, 'val': event.val}
        remote_configs_table.update_one({'_id': event.key},
                                        {'$push': {event.pla: {'$each': [temp], '$position': 0}}}, upsert=True)
        update_all()
        return SUCCESS
    if event.pla is not None:
        temp = {'ver': '0.0.0', 'cond': 'gt', 'val': event.val}
        remote_configs_table.update_one({'_id': event.key},
                                        {'$push': {event.pla: {'$each': [temp], '$position': 0}}}, upsert=True)
        update_all()
        return SUCCESS
    return SERVER_ERROR


@app.post('/UpdateConfig', tags=['Remote Configs for Unity Editor'])
def update_config_handler(event: AddConfigEvent):
    if event.dval is not None:
        remote_configs_table.update_one({'_id': event.key}, {'$set': {'default': event.dval}})
        update_all()
        return SUCCESS
    if event.cond is not None and event.ver is not None:
        remote_configs_table.update_one({'_id': event.key, event.pla + '.ver': event.ver},
                                        {'$set': {event.pla + '.$.cond': event.cond, event.pla + '.$.val': event.val}})
        update_all()
        return SUCCESS
    if event.pla is not None:
        remote_configs_table.update_one({'_id': event.key, event.pla + '.ver': '0.0.0'},
                                        {'$set': {event.pla + '.$.val': event.val}})
        update_all()
        return SUCCESS
    return SERVER_ERROR


@app.post('/RemoveConfig', tags=['Remote Configs for Unity Editor'])
def remove_config_handler(event: AddConfigEvent):
    if event.cond is not None and event.ver is not None:
        remote_configs_table.update_one({'_id': event.key},
                                        {'$pull': {event.pla: {'ver': event.ver, 'cond': event.cond}}})
        update_all()
        return SUCCESS
    if event.pla is not None:
        remote_configs_table.update_one({'_id': event.key}, {'$unset': {event.pla: ''}})
        update_all()
        return SUCCESS
    if event.key is not None:
        remote_configs_table.delete_one({'_id': event.key})
        remote_configs_pv_table.update_many({}, {'$unset': {event.key: ''}})
        return SUCCESS




class UserInfoEvent(BaseModel):
    uid: str
    plf: int
    ver: str
    mlv: int


@app.post('/FindTournament', tags=['Tournament'])
def find_tournament_handler(event: UserInfoEvent):
    current_time = datetime.datetime.utcnow().timestamp().__int__()
    # st_e : StartTime in epoch (UTC)
    # jt_e : JoinTime in epoch (UTC)
    # rl : RequiredLevel
    # ra: RequiredAppVersion
    # ra2: RequiredAppVersion for iOS
    try:
        res = tournament_data_table.find({'$or': [{'$and': [{'st_e': {'$lte': current_time}},
                                                            {'jt_e': {'$gte': current_time}},
                                                            {'rl': {'$lte': event.mlv}},
                                                            {'mlv': 0}]},
                                                  {'$and': [
                                                            {'rl': {'$lte': event.mlv}},
                                                            {'mlv': {'$gte': event.mlv}}]}]},
                                         {'st_e': 0, 'jt_e': 0, 'tabCnt': 0})
        # print(current_time)
        # print(len(list(res)))
        # for cur in res:
        #     print(cur)
        # res = tournament_data_table.find({'st_e': {'$lte': current_time}, 'jt_e': {'$gte': current_time},
        #                                   'rl': {'$lte': event.mlv}, 'ml': 0}, {'st_e': 0, 'jt_e': 0, 'tabCnt': 0})
        list_to_return = []
        if event.plf == 8:
            for tour in res:
                if gle(event.ver, tour['ra2']) in [0, 2]:
                    tour['tid'] = tour.pop('_id')
                    list_to_return.append(tour)
        else:
            for tour in res:
                print(tour)
                if gle(event.ver, tour['ra']) in [0, 2]:
                    tour['tid'] = tour.pop('_id')
                    list_to_return.append(tour)
        try:
            return list_to_return[-1]
        except IndexError:
            return {}
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


class JoinInfoEvent(BaseModel):
    tid: str
    uid: str
    plf: int
    mlv: int
    rp: int
    iap: Optional[int] = 0


@app.post('/JoinTournament', tags=['Tournament'])
def join_tournament_handler(event: JoinInfoEvent):
    # print(event.uid)
    res1 = tournament_table_data_table.find_one({event.uid: 1, 'tourID': event.tid}, {'_id': 1})
    # if user has joined
    if res1 is not None:
        table_id = res1['_id']
        uid_pts = redis_client.zrevrange(table_id, 0, -1, withscores=True)
        user_score_max = 0
        user_score_min = 100000
        user_cnt = 0
        for item in uid_pts:
            print(item)
            if item[0][0:5] != 'fake_': #0:5!=fake_
                user_score_max = max(user_score_max, item[1])
                user_score_min = min(user_score_min, item[1])
                user_cnt += 1
        redis_client.zadd(table_id, {event.uid: event.rp})
        return {'tab': table_id, 'map': int(user_score_max), 'mip': int(user_score_min), 'cnt': user_cnt}

    tour = tournament_data_table.find_one({'_id': event.tid}, {'_id': 0, 'ca': 1, 'tabCnt': 1, 'mp': 1})
    if tour is None:
        return TOURNAMENT_NOT_FOUND
    if 'mp' in tour:
        max_points = tour['mp']
    else:
        max_points = 500
    max_capacity = tour['ca'] - FAKE_REMAIN
    table_count = tour['tabCnt']
    if event.mlv > 3500:
        expert = 5
    elif event.mlv > 500:
        expert = 4
    elif event.mlv > 300:
        expert = 3
    elif event.mlv > 150:
        expert = 2
    else:
        expert = 1

    if event.iap > 20:
        vip = 2
    else:
        vip = 1

    # Find all tables satisfying tournament id, platform, level-range (expertise), total iap (vip) and number of players (count)
    res = tournament_table_data_table.find({'tourID': event.tid, 'plf': event.plf,
                                            'expLV': expert, 'vipLV': vip, 'cnt': {'$lt': max_capacity}})
    # Check score of top player to see if it is less than max points
    table_id = None
    for tab in res:
        uid_pts = redis_client.zrevrange(tab['_id'], 0, -1, withscores=True)
        # [user_id_max, user_score_max] = redis_client.zrevrange(tab['_id'], 0, 0, withscores=True)[0]
        # [user_id_min, user_score_min] = redis_client.zrange(tab['_id'], 0, 0, withscores=True)[0]
        if uid_pts[0][1] < max_points:
            table_id = tab['_id']
            break
    if table_id is not None:
        user_count = tab['cnt']
        user_score_max = 0
        user_score_min = 100000
        for item in uid_pts:
            if item[0][0:4] != 'fake':
                user_score_max = max(user_score_max, item[1])
                user_score_min = min(user_score_min, item[1])
            else:
                uid_to_remove = item[0]
        # tournament_data_table.update_one({'_id': event.tid}, {'$set': {'TabCnt': table_count + 1}})
        tournament_table_data_table.update_one({'_id': table_id}, {'$inc': {'cnt': 1}, '$set': {event.uid: 1}})
        # member = redis_client.zrange(table_id, 0, -1, desc=False, withscores=False)
        redis_client.zrem(table_id, uid_to_remove)
        redis_client.zadd(table_id, {event.uid: event.rp})
        return {'tab': table_id, 'map': int(user_score_max), 'mip': int(user_score_min), 'cnt': user_count + 1}
    else:
        # No good table, create new table
        table_id_new = event.tid + '_' + str(table_count + 1)
        tournament_data_table.update_one({'_id': event.tid}, {'$set': {'tabCnt': table_count + 1}})
        tournament_table_data_table.insert_one({'_id': table_id_new, 'tourID': event.tid, 'plf': event.plf,
                                                'expLV': expert, 'vipLV': vip, 'cnt': 1, event.uid: 1})
        list_to_add = {}
        fake_uid_list = random.sample(range(0, 10000), tour['ca'] - 1)
        # fake_uid_pts = list(numpy.random.randint(low=5, high=51, size=max_capacity - 1))
        # fake_uid_pts = random.sample(range(5, 51), 20)
        fake_uid_pts = [5, 10, 15, 20] * 25 ################ error when capacity >  100
        for i in range(tour['ca'] - 1):
            list_to_add['fake_' + str(fake_uid_list[i])] = fake_uid_pts[i]
        list_to_add[event.uid] = event.rp
        redis_client.zadd(table_id_new, list_to_add)
        return {'tab': table_id_new, 'map': event.rp, 'mip': event.rp, 'cnt': 1}


class UpdateTREvent(BaseModel):
    tab: str
    uid: str
    rp: int


@app.post('/UpdateTR', tags=['Tournament'])
def update_tournament_ranking_handler(event: UpdateTREvent):
    try:
        response = user_data_table.find_one({'_id': event.uid}, {'_id': 1})
        if response is None:
            return USER_NOT_FOUND
        redis_client.zadd(event.tab, {event.uid: event.rp})
        return SUCCESS
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


class GetTREvent(BaseModel):
    tab: str


@app.post('/GetTR', tags=['Tournament'])
def get_tournament_ranking_handler(event: GetTREvent):
    lb_top = redis_client.zrange(event.tab, 0, -1, desc=True, withscores=True)
    lb_top_json = []
    current_rank = 1
    for item in lb_top:
        new_item = {'id': item[0], 'rank': current_rank, 'rp': int((item[1]))}
        lb_top_json.append(new_item)
        current_rank += 1
    return lb_top_json


@app.post('/GetTRUD', tags=['Tournament'])
def get_tournament_ranking_user_data_handler(event: GetSRUDEvent):
    try:
        response = user_data_table.find({'_id': {'$in': event.uids}},
                                        {'_id': 1, 'a2': 1, 'a3': 1, 'a4': 1, 'a9': 1, 'a21': 1, 'a42': 1, 'l31': 1})
        data_to_return = []
        for record in response:
            record['id'] = record.pop('_id')
            data_to_return.append(record)
        return data_to_return
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/GetAllTourIDs', tags=['Tournament for Unity Editor'])
def get_all_tournament_ids_handler():
    try:
        response = tournament_data_table.distinct('_id')
        if 'Master' in response:
            response.remove('Master')
        # response.remove('Master') not found in local
        return response
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


class GetTourEvent(BaseModel):
    tid: str


@app.post('/GetTournament', tags=['Tournament for Unity Editor'])
def get_tournament_handler(event: GetTourEvent):
    response = tournament_data_table.find_one({'_id': event.tid}, {'_id': 0, 'st_e': 0, 'jt_e': 0, 'tabCnt': 0})
    if response is None:
        return TOURNAMENT_NOT_FOUND
    return response


class AddTournamentEvent(BaseModel):
    tt: int
    st: str
    et: str
    jt: str
    rt: str
    jp: int
    r1: str
    r2: str
    r3: str
    r4: str
    tc: int
    r5: str
    ca: int
    ra: Optional[str] = None
    ra2: Optional[str] = None
    rl: Optional[int] = None
    mlv: int


@app.post('/AddTournament', tags=['Tournament for Unity Editor'])
def add_tournament_handler(event: AddTournamentEvent):
    info = tournament_data_table.find_one({'_id': 'Master'}, {'_id': 0, str(event.tt): 1})
    tour_name = info[str(event.tt)]['Name']
    tour_count = info[str(event.tt)]['Count']
    if len(event.st) < 12:
        start_epoch = datetime.datetime.strptime(event.st + ' 00:00:00 +0000',
                                                 '%d/%m/%Y %H:%M:%S %z').timestamp().__int__()
    else:
        start_epoch = datetime.datetime.strptime(event.st + ' +0000',
                                                 '%d/%m/%Y %H:%M:%S %z').timestamp().__int__()
    if len(event.jt) < 12:
        join_epoch = datetime.datetime.strptime(event.jt + ' 00:00:00 +0000',
                                                '%d/%m/%Y %H:%M:%S %z').timestamp().__int__()
    else:
        join_epoch = datetime.datetime.strptime(event.jt + ' +0000',
                                                '%d/%m/%Y %H:%M:%S %z').timestamp().__int__()
    new_tour_id = tour_name + str(tour_count + 1)
    helper_info = {'_id': new_tour_id, 'st_e': start_epoch, 'jt_e': join_epoch, 'tabCnt': 0}
    if event.ra is None:
        helper_info['ra'] = '0.0.0'
    if event.ra2 is None:
        helper_info['ra2'] = '0.0.0'
    if event.rl is None:
        helper_info['rl'] = 0
    for item in event:
        if item[1] is not None:
            helper_info[item[0]] = item[1]
    tournament_data_table.insert_one(helper_info)
    tournament_data_table.update_one({'_id': 'Master'}, {'$inc': {str(event.tt) + '.Count': 1}})
    return new_tour_id


class UpdateTournamentEvent(BaseModel):
    tid: str
    tt: Optional[int]
    st: Optional[str]
    et: Optional[str]
    jt: Optional[str]
    rt: Optional[str]
    jp: Optional[int]
    r1: Optional[str]
    r2: Optional[str]
    r3: Optional[str]
    r4: Optional[str]
    tc: Optional[int]
    r5: Optional[str]
    ca: Optional[int]
    ra: Optional[str]
    ra2: Optional[str]
    rl: Optional[int]
    mlv: Optional[int]


@app.post('/UpdateTournament', tags=['Tournament for Unity Editor'])
def update_tournament_handler(event: UpdateTournamentEvent):
    try:
        helper_info = {}
        if event.st is not None:
            if len(event.st) < 12:
                start_epoch = datetime.datetime.strptime(event.st + ' 00:00:00 +0000',
                                                         '%d/%m/%Y %H:%M:%S %z').timestamp().__int__()
            else:
                start_epoch = datetime.datetime.strptime(event.st + ' +0000',
                                                         '%d/%m/%Y %H:%M:%S %z').timestamp().__int__()
            helper_info['st_e'] = start_epoch
        if event.jt is not None:
            if len(event.jt) < 12:
                join_epoch = datetime.datetime.strptime(event.jt + ' 00:00:00 +0000',
                                                        '%d/%m/%Y %H:%M:%S %z').timestamp().__int__()
            else:
                join_epoch = datetime.datetime.strptime(event.jt + ' +0000',
                                                        '%d/%m/%Y %H:%M:%S %z').timestamp().__int__()
            helper_info['jt_e'] = join_epoch
        for item in event:
            if item[1] is not None:
                helper_info[item[0]] = item[1]
        tour_id = helper_info.pop('tid')
        response = tournament_data_table.update_one({'_id': tour_id}, {'$set': helper_info})
        if response.matched_count == 1:
            return SUCCESS
        else:
            return TOURNAMENT_NOT_FOUND
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


class DeleteTournamentEvent(BaseModel):
    tid: str


@app.post('/RemoveTournament', tags=['Tournament for Unity Editor'])
def delete_tournament_handler(event: DeleteTournamentEvent):
    try:
        res1 = tournament_data_table.delete_one({'_id': event.tid})
        if res1.deleted_count != 1:
            return TOURNAMENT_NOT_FOUND
        response = tournament_table_data_table.distinct('_id', {'tourID': event.tid})
        for table in response:
            redis_client.delete(table)
        tournament_table_data_table.delete_many({'tourID': event.tid})
        return SUCCESS
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/GetAllTableIDs', tags=['Tournament for Unity Editor'])
def get_all_table_ids_handler(event: DeleteTournamentEvent):
    try:
        res1 = tournament_data_table.find_one({'_id': event.tid}, {'_id': 1})
        if res1 is None:
            return TOURNAMENT_NOT_FOUND
        response = tournament_table_data_table.distinct('_id', {'tourID': event.tid})
        return response
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR



@app.post('/GetFreeGifts', tags=['Remote Gifts'])
def get_free_gifts_handler():
    response = remote_gift_table.find({'_id': {'$regex': '^_'}, 'state': 1},
                                      {'state': 0, 'createdAt': 0})
    list_to_return = []
    for item in response:
        if item['limit'] <= 0 or item['redeemed'] < item['limit']:
            item['code'] = str(item.pop('_id'))
            item.pop('redeemed')
            item.pop('limit')
            list_to_return.append(item)
    return list_to_return


class GetGiftCodeEvent(BaseModel):
    code: str


@app.post('/GetGift', tags=['Remote Gifts'] )
def get_gift_handler(event: GetGiftCodeEvent):
    try:
        response = remote_gift_table.find_one({'_id': event.code},
                                              {'limit': 0, 'redeemed': 0, 'state': 0, 'createdAt': 0})
        if not response:
            return GIFT_CODE_NOT_FOUND
        else:
            response['code'] = str(response.pop('_id'))
            return response
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/RedeemGift', tags=['Remote Gifts'])
def redeem_gift_handler(event: GetGiftCodeEvent):
    try:
        remote_gift_table.update_one({'_id': event.code}, {'$inc': {'redeemed': 1}})
        return SUCCESS
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/GetAllGifts', tags=['Remote Gifts for Unity Editor'])
def get_all_gift_handler():
    response = remote_gift_table.find({}, {'limit': 0, 'redeemed': 0, 'state': 0, 'createdAt': 0})
    list_to_return = []
    for item in response:
        item['code'] = str(item.pop('_id'))
        list_to_return.append(item)
    return list_to_return


class AddGiftCodeEvent(BaseModel):
    code: str
    title: str
    msg: str
    rewards: str
    cond: str
    limit: int


@app.post('/AddGift', tags=['Remote Gifts for Unity Editor'])
def add_gift_handler(event: AddGiftCodeEvent):
    try:
        remote_gift_table.insert_one({'_id': event.code, 'title': event.title, 'msg': event.msg,
                                      'rewards': event.rewards, 'cond': event.cond, 'limit': event.limit,
                                      'state': 0, 'redeemed': 0,
                                      'createdAt': datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")})
        return SUCCESS
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


class UpdateGiftCodeEvent(BaseModel):
    code: str
    title: str
    msg: str
    rewards: str
    cond: str
    limit: int
    state: int


@app.post('/UpdateGift', tags=['Remote Gifts for Unity Editor'])
def update_gift_handler(event: UpdateGiftCodeEvent):
    try:
        response = remote_gift_table.update_one({'_id': event.code},
                                                {'$set': {'title': event.title, 'msg': event.msg,
                                                          'rewards': event.rewards, 'cond': event.cond,
                                                          'limit': event.limit, 'state': event.state}})
        if response.matched_count == 1:
            return SUCCESS
        else:
            return GIFT_CODE_NOT_FOUND
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/RemoveGift', tags=['Remote Gifts for Unity Editor'])
def remove_gift_handler(event: GetGiftCodeEvent):
    try:
        response = remote_gift_table.delete_one({'_id': event.code})
        if response.deleted_count == 1:
            return SUCCESS
        else:
            return GIFT_CODE_NOT_FOUND
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR



# class Link2AEvent(BaseModel):
#     uid: str
#     link: str
#
#
# @app.post('/ShareLink', tags=['User Referral'])
# def share_link_handler(event: Link2AEvent):
#     try:
#         link_2_a_table.insert_one({'_id': event.link, 'id_A': event.uid})
#         return SUCCESS
#     except pymongo.errors.PyMongoError:
#         return SERVER_ERROR


class B2AEvent(BaseModel):
    link: str
    did: str
    aid: str


@app.post('/PlayFromLink', tags=['User Referral'])
def play_from_link_handler(event: B2AEvent):
    try:
        res = b_2_a_table.find_one({'_id': event.did + event.aid})
        if res is None:
            id_A = event.link[-21:]
            if id_A[0] != '_':
                b_2_a_table.insert_one({'_id': event.did + event.aid, 'id_A': id_A[-20:]})
            else:
                b_2_a_table.insert_one({'_id': event.did + event.aid, 'id_A': id_A})
        return SUCCESS
    except pymongo.errors.PyMongoError:
        return SERVER_ERROR


@app.post('/RefCount', tags=['User Referral'])
def ref_count_handler(event: LogUIDEvent):
    cnt = b_2_a_table.find({'id_A': event.uid}).count()
    return cnt


# create api to know how many players has installed from a shared link


if __name__ == '__main__':
    uvicorn.run('game_server:app', host='localhost', port=8080, reload=True)

# The UvicornWorker implementation uses the uvloop and httptools implementations
# gunicorn -b localhost:8080 -w 1 -k uvicorn.workers.UvicornWorker --keep-alive 70 game_server:app &
