# Search addresses in MongoDB

from flask import jsonify
from pymongo import TEXT
from flask import Flask
from flask_pymongo import PyMongo
from pymongo import MongoClient

app = Flask(__name__)
app.config['MONGO_DBNAME'] = 'es_addresses'
app.config['MONGO_URI'] = 'mongodb://172.17.0.2:27017/es_addresses'

client = MongoClient('172.17.0.2', 27017, connect=False, serverSelectionTimeoutMS=9000)
mongo = PyMongo(app)
db = client['es_addresses']
collection = db['es_25829']
# es_nc_statewide', 'es_addresses.es_25829


# index1 = IndexModel([("hello", DESCENDING),
#                     ("world", ASCENDING)], name="hello_world")
# index2 = IndexModel([("goodbye", DESCENDING)])
# db.test.create_indexes([index1, index2])


# Create index on searchable indices
# db['es_addresses.es_25829'].create_index([("STREET", TEXT), ( "CITY", TEXT), ("POSTCODE", TEXT),
#                                           ("DISTRICT", TEXT), ("REGION", TEXT)])


@app.route("/")
def home_page():
    return "home page"


@app.route('/search', methods=['GET'])
def search():
    search = mongo.db.collection
    output = []
    for q in search.find().limit(10):
        output.append(
            # {'name' : q['name'], 'categories' : q['categories']}
            {'_id' : q['_id'], 'LON' : q['LON'], 'LAT' : q['LAT'], 'NUMBER' : q['NUMBER'], 'STREET' : q['STREET'],
             'UNIT' : q['UNIT'], 'CITY' : q['CITY'], 'DISTRICT' : q['DISTRICT'], 'REGION' : q['REGION'],
             'POSTCODE' : q['POSTCODE'], 'ID' : q['ID'], 'HASH': q['HASH']})

    return jsonify({'result' : output})



@app.route('/search_street/<keyword>', methods = ['GET'])
def search_street(keyword):
    output = []
    for q in collection.find({'STREET': keyword}).limit(10):
        output.append({'_id' : q['_id'], 'LON' : q['LON'], 'LAT' : q['LAT'], 'NUMBER' : q['NUMBER'],
                       'STREET' : q['STREET'], 'UNIT' : q['UNIT'], 'CITY' : q['CITY'], 'DISTRICT' : q['DISTRICT'],
                       'REGION' : q['REGION'], 'POSTCODE' : q['POSTCODE'], 'ID' : q['ID'], 'HASH': q['HASH']})

    return jsonify({'results' : output})


@app.route('/search/<keyword>', methods = ['GET'])
def search_by_keyword(keyword):
    output = []
    for q in  collection.find({"$text": {"$search": keyword}}).limit(10):
        output.append({'_id' : q['_id'], 'LON' : q['LON'], 'LAT' : q['LAT'], 'NUMBER' : q['NUMBER'],
                       'STREET' : q['STREET'], 'UNIT' : q['UNIT'], 'CITY' : q['CITY'], 'DISTRICT' : q['DISTRICT'],
                       'REGION' : q['REGION'], 'POSTCODE' : q['POSTCODE'], 'ID' : q['ID'], 'HASH': q['HASH']})

    return jsonify({'results' : output})

# https://stackoverflow.com/questions/35030142/flask-restful-search-query
# Cooenct Flask API with MongoDB
#
# from flask_restful import Api, Resource
# @app.route('/search/<keyword>', methods = ['GET'])
# class UserSearch(Resource):
#     def get(self, keyword):
#         output = collection.find({"$text": {"$search": keyword}}).limit(10)
#         # User.query.filter(User.name.like('%'+search_term+'%')).all()
#
#         # serialize and return items...
#         return jsonify({'results' : output})
#
# Api.add_resource(UserSearch, '/search/<search_term>')

if __name__ == '__main__':
    app.run(debug=True)



# https://pythonbasics.org/flask-mongodb/
# import json
# from flask import Flask, request, jsonify
# from flask_mongoengine import MongoEngine
#
# app = Flask(__name__)
# app.config['MONGODB_SETTINGS'] = {
#     'db': 'es_addresses',
#     'host': 'localhost',
#     'port': 27017
# }
# db = MongoEngine()
# db.init_app(app)
#
# class User(db.Document):
#     name = db.StringField()
#     email = db.StringField()
#     def to_json(self):
#         return {"name": self.name,
#                 "email": self.email}
#
# @app.route('/', methods=['GET'])
# def query_records():
#     name = request.args.get('name')
#     user = User.objects(name=name).first()
#     if not user:
#         return jsonify({'error': 'data not found'})
#     else:
#         return jsonify(user.to_json())