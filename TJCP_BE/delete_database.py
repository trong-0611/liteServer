import pymongo
import server_config

mongodb_client = pymongo.MongoClient(server_config.mongodb_host, server_config.mongodb_port)

user_data_table = mongodb_client['GametaminTJCP']['UserData']
facebook_data_table = mongodb_client['GametaminTJCP']['FacebookData']
device_data_table = mongodb_client['GametaminTJCP']['DeviceData']

user_data_table.delete_many({})
device_data_table.delete_many({})
facebook_data_table.delete_many({})