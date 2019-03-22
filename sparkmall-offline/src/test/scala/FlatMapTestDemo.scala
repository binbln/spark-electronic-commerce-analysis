import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author xzb
  */
object FlatMapTestDemo {

  def main(args: Array[String]): Unit = {

    val array = Array((1, ("aaaa", "aa")), (2, ("bbbbb", "bb")))

    val sparkConf: SparkConf = new SparkConf().setAppName("FlatMapTestDemo").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(Int, (String, String))] = sc.makeRDD(array)

    val unit: RDD[Char] = rdd.flatMap {
      case (_, item) =>
        item._1
    }
    unit.foreach(println)
    println("---------------------------------------")


    val map1: RDD[String] = rdd.map {
      case (id, item) =>
        item._1
    }
    map1.foreach(println)
  }

}
