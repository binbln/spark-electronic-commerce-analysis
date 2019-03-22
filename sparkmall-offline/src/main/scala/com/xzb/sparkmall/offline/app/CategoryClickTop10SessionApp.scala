package com.xzb.sparkmall.offline.app

import com.xzb.sparkmall.common.bean.UserVisitAction
import com.xzb.sparkmall.common.util.JDBCUtil
import com.xzb.sparkmall.offline.bean.{CategoryCountInfo, CategorySession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author xzb
  *
  *         对于排名前 10 的品类，分别获取每个品类点击次数排名前 10 的 sessionId
  */
object CategoryClickTop10SessionApp {

  def statCategoryClickTop10Session(sparkSession: SparkSession, categoryTop10: List[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction], taskId: String) = {

    // 1 categoryTop10 取出 categoryId
    val top10CategoryId: List[String] = categoryTop10.map(_.categoryId)

    // 2 从userVisitActionRDD中过滤出 只包含top10品类的用户行为日志
    val filterUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(uva => top10CategoryId.contains(uva.click_category_id.toString))

    // 3 类型转换 uva => ((cid,sid),1) => ((cid,sid),count) => (cid,(sid,count))
    val groupByCategorySessionCountRDD: RDD[(Long, (String, Int))] = filterUserVisitActionRDD.map {
      uva => ((uva.click_category_id, uva.session_id), 1)
    }.reduceByKey(_ + _).map {
      case ((cid, sid), count) => (cid, (sid, count))
    }

    // 4 groupBy cid 后 迭代Iterable[(String, Int)]中的数据排序 取top10
    val categorySessionRDD: RDD[CategorySession] = groupByCategorySessionCountRDD.groupByKey.flatMap {
      case (cid, it) => {
        it.toList.sortBy(_._2)(Ordering.Int.reverse).take(10).map {
          case (sid, count) => CategorySession(taskId, cid.toString, sid, count)
        }
      }
    }
    println("品类点击次数排名  sessionId top10")
    categorySessionRDD.collect.foreach(println)

    val sqlArgs: Array[Array[Any]] = categorySessionRDD.map(info => Array(info.taskId, info.categoryId, info.sessionId, info.clickCount)).collect

    // 5 写入mysql
    JDBCUtil.executeUpdate("use sparkmall", null)
    JDBCUtil.executeUpdate("truncate table category_top10_session_count", null)
    JDBCUtil.executeBatchUpdate("insert into category_top10_session_count values(?, ?, ?, ?)", sqlArgs)

  }


}
