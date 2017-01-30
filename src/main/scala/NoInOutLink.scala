Iimport org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger
import org.apache.log4j.Level


// Do NOT use different Spark libraries.

object NoInOutLink {
    def main(args: Array[String]) {
        val MASTER_ADDRESS = "ec2-35-160-104-10.us-west-2.compute.amazonaws.com"
        val SPARK_MASTER = "spark://" + MASTER_ADDRESS + ":7077"
        val HDFS_MASTER = "hdfs://" + MASTER_ADDRESS + ":9000"
        val INPUT_DIR = HDFS_MASTER + "/hw2/input"
        
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
        
        val num_partitions = 10
        
        val conf = new SparkConf()
            .setAppName("NoInOutLink")
            .setMaster(SPARK_MASTER)

        val sc = new SparkContext(conf)
        
        /*
        parse links-simple-sorted into RDD, each element is(a,b)
        where a is the id of page, and b is the id of outlink page
        */
        val firstLinks = sc
            .textFile(INPUT_DIR+"/links-simple-sorted.txt", num_partitions)
        
        val links=firstLinks
            .map(remove_punctuation)
            .flatMap(count_num)   

        //reverse the tuple(a,b) in links
        val reverseLinks=links
            .map( word => (word._2, word._1) )

        //parse title
        val titles = sc
            .textFile(INPUT_DIR+"/titles-sorted.txt", num_partitions)
            .zipWithIndex()
            .map(word => (word._2+1,word._1))

        //find first ten pages with no outlinks in ascending order
        val no_outlinks=titles
            .leftOuterJoin(links)
            .map(unpackage)
            .filter(word => word._3 == None)
            .map(word => (word._1, word._2))
            .takeOrdered(10)(Ordering[Long].on(word =>word._1))

        println("[ NO OUTLINKS ]")

        val outLink=no_outlinks
            .foreach(println)

        //find first ten pages with no inlinks in ascending order
        val no_inlinks=titles
            .leftOuterJoin(reverseLinks)
            .map(unpackage)
            .filter(word => word._3 == None)
            .map(word => (word._1, word._2))
            .takeOrdered(10)(Ordering[Long].on(word =>word._1))

        println("\n[ NO INLINKS ]")

        no_inlinks
        .foreach(println)

    }
    def remove_punctuation(line: String):String={
        line.replaceAll(":", "")    
    }

    def count_num(line:String): Array[(Long,Long)]= {
        val list= line
                  .split(" ")
        
        val num:Int=list.length-1

        val count:Array[(Long,Long)]=new Array[(Long,Long)](num)

        for(i <- 1 to (list.length-1)  )
        {
            count(i-1)=( list(0).toLong,list(i).toLong)

        }
        count
    }

    def unpackage(line: (Long, (String, Option[Long]) ) ): (Long, String, Option[Long])={
        val a=line._2
        val b=a._1
        val c=a._2
        val d=line._1
        val count:(Long, String, Option[Long])=(d,b,c)
        count
    }

}

