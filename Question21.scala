val file = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")
val splittedFile = file.map(x => x.split("\t")).filter(x => x.length == 2)
val listFile = splittedFile.map(x => (x(0), x(1).split(",")))
val mapped_output = listFile.map(x=>
      {
        val friend1 = x._1
        val friends = x._2.toList
        for(friend2<- friends) yield
        {
         if(friend1.toInt< friend2.toInt)
           (friend1+","+friend2 -> friends)
           else
          (friend2+","+friend1 -> friends)   
        } 
      }
        )
val flatten_output =   mapped_output.flatMap(identity).map(x=> (x._1,x._2)).distinct.reduceByKey((x,y)=> (x.intersect(y))).sortByKey()
val friend_count = flatten_output.map(x=> (x._1, x._2.length)).map(_.swap).sortByKey(false).take(10).map(x=> (x._1,(x._2.split(",")(0),x._2.split(",")(1))))    

val userdata_file = sc.textFile("FileStore/tables/userdata.txt")
val read_userdata = userdata_file.map(parseUserData)
val friend_countRDD = sc.parallelize(friend_count)
val RDD1 =friend_countRDD.map(x=> (x._2._1,(x._2._2,x._1))).join(read_userdata).map(x=> (x._2._1._1, (x._1,x._2._1._2,x._2._2)))
val RDD2 = RDD1.join(read_userdata).map(x=>(x._2._1._2 ,(x._2._1._3,x._2._2)))
val formatted_output = RDD2.map(x=>(x._1+"\t"+x._2._1._1+"\t"+x._2._1._2+"\t"+x._2._1._3+"\t"+x._2._2._1+"\t"+x._2._2._2+"\t"+x._2._2._3) )
def parseUserData(line:String) =
  {
    val fields = line.split(",")
    val userID = fields(0)
    val  firstname = fields(1)
    val lastname = fields(2)
    val address =fields(3)
    (userID,(firstname,lastname,address))
  }
//formatted_output.collect().foreach(println)
formatted_output.coalesce(1).saveAsTextFile("/FileStore/tables/Output21")
