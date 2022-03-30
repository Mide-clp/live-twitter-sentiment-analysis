from pymongo import MongoClient

print("connecting to MongoDB")

conn = []
try:
    print("Successfully connected")
    conn = MongoClient()

except Exception:
    print("could not connect to MongoDB")

# connecting to database
db = conn.twitter

# create or use connection
connection = db.web3


# insert a new document into mongo
def insert_document(data):
    ids = connection.insert_one(data)
    print(f"successfully inserted {ids}")


