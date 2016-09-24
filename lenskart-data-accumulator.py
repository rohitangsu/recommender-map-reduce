from pymongo import MongoClient
from bson.objectid import ObjectId

#repl set mongo settings..
REPL_CONN_SETTINGS = {
        'host': 'replset-mongo-9.qg.1536.mongodbdns.com:27000, replset-mongo-10.qg.1536.mongodbdns.com:27000, replset-mongo-11.qg.1536.mongodbdns.com:27000',
        'replicaSet': 'replset',
        'readPreference': 'nearest',
        'w': 0
}

CLIENT_ID = '6ed11a9246404d1b95fe'

mongo_client = MongoClient(**REPL_CONN_SETTINGS)
events_coll = mongo_client[CLIENT_ID].events

#mongo row id to start from..
object_id = ObjectId("577565280000000000000000")

#query
query = {'eventName': 'product_viewed', '_id': {'$gt': object_id}}


#projection..
projection = {'_id': 0, 'parameters.product_id': 1, 'userId': 1}



#Open file object to put recommendation data
fp = open('lenskart_user_data.txt', 'a+')


number_of_events_seen = 0

for event in events_coll.find(query, projection):
    if number_of_events_seen % 1000 == 0:
        print 'events seen so far: ', number_of_events_seen
    user_id = event['userId']
    if 'product_id' not in event['parameters']:
       continue
    product_id = event['parameters']['product_id']
    line = '|'.join([str(user_id), str(product_id)])
    fp.write(line + '\n')
    number_of_events_seen += 1
#Closing file
fp.close()
