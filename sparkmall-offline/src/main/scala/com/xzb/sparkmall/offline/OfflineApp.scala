package com.xzb.sparkmall.offline

import com.alibaba.fastjson.JSON
import com.xzb.sparkmall.common.bean.UserVisitAction
import com.xzb.sparkmall.common.util.ConfigurationUtil
import com.xzb.sparkmall.offline.bean.Condition
import org.apache.spark.sql.SparkSession

/**
  * @author xzb
  */
object OfflineApp {


  //读取用户行为数据 @param condition 约束
  def readUserVisitActionRDD(sparkSeesion: SparkSession, condition: Condition) = {

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
      sql += s" and u.age <= ${condition.startAge}"
    }

    import sparkSeesion.implicits._
    // 2 执行
    sparkSeesion.sql("use sparkmall")
    sparkSeesion.sql(sql).as[UserVisitAction].rdd.take(10).foreach(print)
  }

  //读取过滤条件
  def readCondition: Condition = {
    //读取配置文件
    val jsonStr: String = ConfigurationUtil("conditions.properties").getString("condition.params.json")
    JSON.parseObject(jsonStr, classOf[Condition])
  }

  def main(args: Array[String]): Unit = {

    // 1. 读取用户行为数据 存入RDD
    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("MockOffline")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop107:9000/user/hive/warehouse")
      .getOrCreate()


    readUserVisitActionRDD(sparkSession, readCondition)
  }
}