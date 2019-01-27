from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as func

conf = SparkConf().setMaster("local").setAppName("sql_sample")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)


def friendPairs(x):
    friendPairs = list()
    if len(x) > 1:
      friends = x[1].split(",")
      for friend1 in friends:
          if x[0] == friend1 or str(x[0]) == '' or str(friend1) == '':
              continue
          pair =  (int(x[0]) < int(friend1) ) and [x[0], friend1] or [friend1, x[0]]
          for friend2 in friends:
              if friend2 != friend1:
                finalPair = [pair[0],pair[1],friend2]
                friendPairs.append(finalPair)
    return friendPairs

friend_dataframe = sc.textFile("soc-LiveJournal1Adj.txt").map(lambda line: line.split("\t")).flatMap(friendPairs).toDF()
friend_dataframe = friend_dataframe.select(friend_dataframe._1.alias('User_A'), friend_dataframe._2.alias('User_B'), friend_dataframe._3.alias("Mutual_Friends"))
friend_dataframe = friend_dataframe.groupBy('User_A', 'User_B', "Mutual_Friends").count()
friend_dataframe = friend_dataframe.filter(friend_dataframe["count"] == 2).groupBy("User_A", "User_B").count()
friend_dataframe = friend_dataframe.select(func.col("User_A"),func.col("User_B"),func.col("count").alias("Mutual_Friend_Count"))

friend_dataframe = friend_dataframe.orderBy(func.desc('Mutual_Friend_Count')).limit(10)
userdata_dataframe = sc.textFile("userdata.txt").map(lambda x: x.split(",")).toDF()
userdata_dataframe = userdata_dataframe.select(userdata_dataframe._1.alias("User_ID"),userdata_dataframe._2.alias("First Name"),userdata_dataframe._3.alias("Last Name"),userdata_dataframe._4.alias("Address"))

mutualfriend_dataframe = friend_dataframe.join(userdata_dataframe, friend_dataframe.User_A == userdata_dataframe.User_ID)
mutualfriend_dataframe = mutualfriend_dataframe.select( func.col('Mutual_Friend_Count'), func.col("First Name").alias("First Name of User A"), func.col("Last Name").alias("Last Name of User A"), func.col("Address").alias("address of User A"), func.col("User_B"))
mutualfriend_dataframe = mutualfriend_dataframe.join(userdata_dataframe, mutualfriend_dataframe.User_B == userdata_dataframe.User_ID)

output_dataframe = mutualfriend_dataframe.select(func.col('Mutual_Friend_Count').alias("Total Number of Common Friends"),func.col("First Name of User A"), func.col("Last Name of User A"),func.col("address of User A"), func.col("First Name").alias("First Name of User B"), func.col("Last Name").alias("Last Name of User B"), func.col("Address").alias("address of User B"))
#output_dataframe.show()
output_dataframe.rdd.repartition(1).saveAsTextFile("Output22")