from pymongo import MongoClient

CONNECTION_STRING = "mongodb+srv://root:5OkOyu53TE2xdC2i@cluster0.tnzynwo.mongodb.net"

client = MongoClient(CONNECTION_STRING)

# Substitua "test" pelo nome do seu banco de dados
db = 'db_test'
# Substitua "test_collection" pelo nome da sua coleção
collection = 'db.collection_teste'

database_list = client.list_database_names()
print(database_list)
