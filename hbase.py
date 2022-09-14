# To start hbase service on virtual machine (CLI command):
# /usr/hdp/current/hbase-master/bin/hbase-daemon.sh start rest -p 8003 --infoport 8001

from starbase import Connection
# starbase: the REST client for HBase with a Python wrapper on top of it.

c = Connection("127.0.0.1", "10015")
# create a Connection object with the service IP and port configured on VM and the HBase server, connecting to the REST server sitting on top of HBase.

ratings = c.table('ratings') # create a table named ratings.

if (ratings.exists()):
    print("Dropping existing ratings table\n")
    ratings.drop()

ratings.create('rating') # creating a column family on a given table.

print("Parsing the ml-100k ratings data...\n")
ratingFile = open("u.data", "r") # r - read only

batch = ratings.batch() # batch interface. Do the updating (batch.commit) all at once instead of row-wise update.

for line in ratingFile:
    (userID, movieID, rating, timestamp) = line.split()
    batch.update(userID, {'rating': {movieID: rating}})

ratingFile.close()

print("Committing ratings data to HBase via REST service\n")
batch.commit(finalize=True)

print("Get back ratings for some users...\n")
print("Ratings for user ID 1:\n")
print(ratings.fetch("1"))
print("Ratings for user ID 33:\n")
print(ratings.fetch("33"))

ratings.drop()