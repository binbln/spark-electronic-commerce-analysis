import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author xzb
  */
object FlatMapTestDemo {

  def main(args: Array[String]): Unit = {
    //
    //    val array = Array((1, ("aaaa", "aa")), (2, ("bbbbb", "bb")))
    //
    //    val sparkConf: SparkConf = new SparkConf().setAppName("FlatMapTestDemo").setMaster("local[*]")
    //    val sc = new SparkContext(sparkConf)
    //
    //    val rdd: RDD[(Int, (String, String))] = sc.makeRDD(array)
    //
    //    val unit: RDD[Char] = rdd.flatMap {
    //      case (_, item) =>
    //        item._1
    //    }
    //    unit.foreach(println)
    //    println("---------------------------------------")
    //
    //
    //    val map1: RDD[String] = rdd.map {
    //      case (_, item) =>
    //        item._1
    //    }
    //    map1.foreach(println)
    val map1 = Map("a" -> 1, "b" -> 2, "c" -> 3)
    val map2 = Map("a" -> 10, "d" -> 30, "b" -> 20)

    // a->11  b->2  c->3  d->30
    /*val map3: Map[String, Int] = map1 ++ map2.map {
        case (k, v) => (k, map1.getOrElse(k, 0) + v)
    }*/
    /*val map3: Map[String, Int] = map1.foldLeft(map2) {
        case (m, (k, v)) => {
            m + (k -> (m.getOrElse(k, 0) + v))
        }
    }
    */
    val map3: Map[String, Int] = (map2 /: map1) {
      case (m, (k, v)) => {
        m + (k -> (m.getOrElse(k, 0) + v))
      }
    }
    println(map3)
  }

}
