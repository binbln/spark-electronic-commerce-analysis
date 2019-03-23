package com.xzb.sparkmall

import java.util.UUID

import com.alibaba.fastjson.JSON
import com.xzb.sparkmall.common.bean.UserVisitAction
import com.xzb.sparkmall.common.util.ConfigurationUtil
import com.xzb.sparkmall.offline.app.{AreaClickApp, CategoryActionTop10App, CategoryClickTop10SessionApp, PageConversionApp}
import com.xzb.sparkmall.offline.bean.{CategoryCountInfo, Condition}
import com.xzb.sparkmall.offline.isNotEmpty
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author xzb
  */
object OfflineApp {


  def main(args: Array[String]): Unit = {

    // 1. 读取用户行为数据 存入RDD
    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("MockOffline")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop107:9000/user/hive/warehouse")
      .getOrCreate()

    val userVisitActionRDD: RDD[UserVisitAction] = readUserVisitActionRDD(sparkSession, readCondition)
    userVisitActionRDD.cache()

    val taskId: String = UUID.randomUUID().toString

    // 需求1 : 统计品类top10
    //val categoryTop10: List[CategoryCountInfo] = CategoryActionTop10App.statCategoryTop10(sparkSession, userVisitActionRDD, taskId)

    // 需求2 : 统计品类top10的点击session的top10
    // CategoryClickTop10SessionApp.statCategoryClickTop10Session(sparkSession, categoryTop10, userVisitActionRDD, taskId)

    // 需求3: 统计 单页跳转率
    // PageConversionApp.statPageConversion(sparkSession, userVisitActionRDD, readCondition.targetPageFlow, taskId)

    // 需求4: 统计地区商品的top3
    AreaClickApp.statAreaClickTop3Product(sparkSession, taskId)
  }

  //读取用户行为数据 @param condition 约束
  def readUserVisitActionRDD(sparkSeesion: SparkSession, condition: Condition): RDD[UserVisitAction] = {

    // 1 sql语句
    var sql: String =
      s"""
         |select
         |v.*
         |from user_visit_action v join user_info u
         |on v.user_id = u.user_id
         |where 1 = 1
     """.stripMargin

    if (isNotEmpty(condition.startDate)) {
      sql += s" and v.date >= '${condition.startDate}'"
    }
    if (isNotEmpty(condition.endDate)) {
      sql += s" and v.date <= '${condition.endDate}'"
    }
    if (condition.startAge > 0) {
      sql += s" and u.age >= ${condition.startAge}"
    }
    if (condition.endAge > 0) {
      sql += s" and u.age <= ${condition.endAge}"
    }

    import sparkSeesion.implicits._
    // 2 执行
    sparkSeesion.sql("use sparkmall")
    sparkSeesion.sql(sql).as[UserVisitAction].rdd
  }

  //读取过滤条件
  def readCondition: Condition = {
    //读取配置文件
    val jsonStr: String = ConfigurationUtil("conditions.properties").getString("condition.params.json")
    JSON.parseObject(jsonStr, classOf[Condition])
  }
}