package com.xzb.sparkmall.offline.app

import com.xzb.sparkmall.common.bean.UserVisitAction
import com.xzb.sparkmall.offline.acc.MapAccumulator
import com.xzb.sparkmall.offline.bean.CategoryCountInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * @author xzb
  *
  *         热门品类行为TOP10
  *
  */
object CategoryActionTop10App {

  //统计
  def statCategoryTop10(sparkSession: SparkSession, userVisitActionRDD: RDD[UserVisitAction], taskId: String): List[CategoryCountInfo] = {

    // 1 注册累加器
    val acc: MapAccumulator = new MapAccumulator
    sparkSession.sparkContext.register(acc, "MapAccumulator")

    // 2 累加搞起
    //    userVisitActionRDD.foreach(
    //      visitAction => {
    //        if (visitAction.click_category_id != -1) {
    //          acc.add(visitAction.click_category_id.toString, "click")
    //        } else if (visitAction.order_category_ids != null) {
    //          visitAction.order_category_ids.split(",").foreach(x => acc.add(x, "order"))
    //        } else if (visitAction.pay_category_ids != null) {
    //          visitAction.pay_category_ids.split(",").foreach(x => acc.add(x, "pay"))
    //        }
    //      }
    //    )
    userVisitActionRDD.foreach {
      case visitAction => {
        if (visitAction.click_category_id != -1) {
          acc.add((visitAction.click_category_id.toString, "click"))
        } else if (visitAction.order_category_ids != null) { // 下单
          // 1,2,3
          visitAction.order_category_ids.split(",").foreach {
            case cid => acc.add((cid, "order"))
          }

        } else if (visitAction.pay_category_ids != null) { // 支付
          visitAction.pay_category_ids.split(",").foreach {
            case cid => acc.add((cid, "pay"))
          }
        }
      }
    }
    //    acc.value.foreach(print)
    // 累加器中的数据 Map((19,order),17)((10,order),10)((20,pay),10)((13,click),31))

    // 3 分组
    val categoryCountMap: mutable.Map[(String, String), Long] = acc.value
    val actionCountByCategoryIdMap: Map[String, mutable.Map[(String, String), Long]] = categoryCountMap.groupBy(_._1._1)
    /*
      (12,Map((12,click) -> 41, (12,pay) -> 7, (12,order) -> 19))
      (8,Map((8,pay) -> 7, (8,order) -> 6, (8,click) -> 30))
      (19,Map((19,order) -> 17, (19,pay) -> 8, (19,click) -> 46))
     */
    // 4 类型转换
    //    val categoryCountInfoList: List[CategoryCountInfo] = actionCountByCategoryIdMap.map(x => CategoryCountInfo(
    //      "",
    //      x._1,
    //      x._2.getOrElse((x._1, "click"), 0),
    //      x._2.getOrElse((x._1, "order"), 0),
    //      x._2.getOrElse((x._1, "pay"), 0)
    //    )).toList
    val categoryCountInfoList: List[CategoryCountInfo] = actionCountByCategoryIdMap.map {
      case (cid, actionMap) => {
        CategoryCountInfo(
          taskId,
          cid,
          actionMap.getOrElse((cid, "click"), 0),
          actionMap.getOrElse((cid, "order"), 0),
          actionMap.getOrElse((cid, "pay"), 0))
      }
    }.toList

    // 5 排序 top10
    val top10: List[CategoryCountInfo] = categoryCountInfoList.sortBy(
      info => (info.clickCount, info.orderCount, info.payCount))(Ordering.Tuple3(Ordering.Long.reverse, Ordering.Long.reverse, Ordering.Long.reverse)).take(10)
    top10.foreach(println)
    top10
  }
}

/*
  1.遍历全部行为日志 根据品类id和行为类型 统计他们的次数
      品类1 : click 次数
              order
              pay

  2.排序 取top10

  3.写入mysql
 */