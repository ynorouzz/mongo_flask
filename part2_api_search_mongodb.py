# Search addresses in MongoDB

from flask import jsonify
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
            {'_id': q['_id'], 'LON': q['LON'], 'LAT': q['LAT'], 'NUMBER': q['NUMBER'], 'STREET': q['STREET'],
             'UNIT': q['UNIT'], 'CITY': q['CITY'], 'DISTRICT': q['DISTRICT'], 'REGION': q['REGION'],
             'POSTCODE': q['POSTCODE'], 'ID': q['ID'], 'HASH': q['HASH']})

    return jsonify({'result': output})


@app.route('/search_street/<keyword>', methods=['GET'])
def search_street(keyword):
    output = []
    for q in collection.find({'STREET': keyword}).limit(10):
        output.append({'_id': q['_id'], 'LON': q['LON'], 'LAT': q['LAT'], 'NUMBER': q['NUMBER'], 'STREET': q['STREET'],
                       'UNIT': q['UNIT'], 'CITY': q['CITY'], 'DISTRICT': q['DISTRICT'], 'REGION': q['REGION'],
                       'POSTCODE': q['POSTCODE'], 'ID': q['ID'], 'HASH': q['HASH']})

    return jsonify({'results': output})


@app.route('/search/<keyword>', methods=['GET'])
def search_by_keyword(keyword):
    output = []
    for q in collection.find({"$text": {"$search": keyword}}).limit(10):
        output.append({'_id': q['_id'], 'LON': q['LON'], 'LAT': q['LAT'], 'NUMBER': q['NUMBER'], 'STREET': q['STREET'],
                       'UNIT': q['UNIT'], 'CITY': q['CITY'], 'DISTRICT': q['DISTRICT'], 'REGION': q['REGION'],
                       'POSTCODE': q['POSTCODE'], 'ID': q['ID'], 'HASH': q['HASH']})

    return jsonify({'results' : output})


if __name__ == '__main__':
    app.run(debug=True)
