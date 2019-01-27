from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setMaster("local").setAppName("sql_sample")
sc = SparkContext(conf=conf)

business = sc.textFile("business.csv").map(lambda line: line.split('::'))
review = sc.textFile("review.csv").map(lambda line: line.split("::"))

business = business.filter(lambda line: "Stanford" in line[1]).map(lambda line: (line[0],line[1]))
reviews = review.map(lambda line: (line[2],(line[1],line[3])))

output = reviews.join(business)
display = output.map(lambda line: (line[1][0]))
#display.foreach(print)
display.saveAsTextFile("Output31")