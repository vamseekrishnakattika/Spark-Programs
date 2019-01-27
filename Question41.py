from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setMaster("local").setAppName("sql_sample")
sc = SparkContext(conf=conf)

business = sc.textFile("business.csv").map(lambda line: line.split('::'))
review = sc.textFile("review.csv").map(lambda line: line.split("::"))
business = business.map(lambda line: (line[0],line[1]+','+line[2])).distinct()
reviewTotal = review.map(lambda line: (line[2],float(line[3]))).reduceByKey(lambda x,y: x+y).distinct()
businessTotal = review.map(lambda line: (line[2],1)).reduceByKey(lambda x,y:x+y)
businessReviews = reviewTotal.join(businessTotal)
averageReview = businessReviews.map(lambda x: (x[0],x[1][0]/x[1][1]))
output = business.join(averageReview)
sortOutput = output.sortBy(lambda x: x[1][1],ascending=False)
top10 = sortOutput.take(10)
#sc.parallelize(top10).coalesce(1).foreach(print)
sc.parallelize(top10).coalesce(1).saveAsTextFile("Output41")
