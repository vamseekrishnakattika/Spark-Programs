val file = sc.textFile("FileStore/tables/soc_LiveJournal1Adj.txt")
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
val flatten_output =   mapped_output.flatMap(identity).map(x=> (x._1,x._2)).reduceByKey((x,y)=> (x.intersect(y))).sortByKey(true)
val friend_mutual = flatten_output.map(x=> (x._1, x._2)).filter(x=>x._2.length>0)
val formatted_output = friend_mutual.map(x=>s"${x._1}\t${x._2.length}")
//formatted_output.collect().foreach(println)
formatted_output.coalesce(1).saveAsTextFile("/FileStore/tables/Output11.txt")

