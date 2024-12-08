from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseData(data):
    fields = data.split(",")
    age = int(fields[2])
    friends = int(fields[3])
    return (age,friends)

friends = sc.textFile("C:\\spark\\Spark_Learnings\\Average_friends_by_Age\\friends.csv")

rdd = friends.map(parseData)

totalsByAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0]+y[0], y[0]+y[1]))

averagesByAge = totalsByAge.mapValues(lambda x: round(x[0]/x[1],2))

sortedByAge = averagesByAge.sortByKey().collect()

for record in sortedByAge:
    print(record)