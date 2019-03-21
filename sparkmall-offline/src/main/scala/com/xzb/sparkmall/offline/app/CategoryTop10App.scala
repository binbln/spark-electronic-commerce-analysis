package com.xzb.sparkmall.offline.app

import com.xzb.sparkmall.common.bean.UserVisitAction
import com.xzb.sparkmall.offline.acc.MapAccumulator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author xzb
  *
  *         热门品类TOP10
  *
  */
object CategoryTop10App {

  //统计
  def categoryTop10(sparkSession: SparkSession, userVisitActionRDD: RDD[UserVisitAction]): Unit = {

    // 1 注册累加器
    val acc: MapAccumulator = new MapAccumulator
    sparkSession.sparkContext.register(acc, "MapAccumulator")

    // 2 累加搞起
    userVisitActionRDD.foreach(
      visitAction => {
        if (visitAction.click_category_id != -1) {
          acc.add(visitAction.click_category_id.toString, "click")
        } else if (visitAction.order_category_ids != null) {
          visitAction.order_category_ids.split(",").foreach(x => acc.add(x, "order"))
        } else if (visitAction.pay_category_ids != null) {
          visitAction.pay_category_ids.split(",").foreach(x => acc.add(x, "pay"))
        }
      }
    )
    acc.value.foreach(print)
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