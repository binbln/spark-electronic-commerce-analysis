package com.xzb.sparkmall.offline.app

import java.text.DecimalFormat

import com.xzb.sparkmall.common.bean.UserVisitAction
import com.xzb.sparkmall.common.util.JDBCUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author xzb
  *
  *         统计单页跳转率
  */
object PageConversionApp {

  def statPageConversion(sparkSession: SparkSession, userVisitActionRDD: RDD[UserVisitAction], targetPages: String, taskId: String) = {

    // 1 处理目标页面 拉链 组成跳转流
    val pages: Array[String] = targetPages.split(",")
    val prePage: Array[String] = pages.slice(0, pages.length - 1)
    val postPage: Array[String] = pages.slice(1, pages.length)
    val targetJumpPages: Array[String] = prePage.zip(postPage).map {
      case (p1, p2) => p1 + "->" + p2
    }

    // 2 计算每个目标页面的点击次数
    // 2.1 获取包含目标页面的 userVisitActionRDD
    val prePagesUVA: RDD[UserVisitAction] = userVisitActionRDD.filter(uva => prePage.contains(uva.page_id.toString))

    // 2.2 按照page_id计算点击次数
    val targetPagesCountMap: collection.Map[Long, Long] = prePagesUVA.map(action => (action.page_id, 1)).countByKey // (page_id,count)

    // 3 统计跳转流次数
    val groupBySessionIdUVARDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(_.session_id) //(sid,Iterable[UserVisitAction]))

    // 3.1 按session分组,action_time排序 取出 前(行为总个数-1)个事件 与 后(行为总个数-1)个事件 然后zip拉链 map((前行为,后行为) => "前页面 -> 后页面"
    val jumpFlow: RDD[String] = groupBySessionIdUVARDD.flatMap {
      case (_, it) => {
        val actionsSortByActionTime: List[UserVisitAction] = it.toList.sortBy(_.action_time)
        val preActions: List[UserVisitAction] = actionsSortByActionTime.slice(0, actionsSortByActionTime.length - 1)
        val postActions: List[UserVisitAction] = actionsSortByActionTime.slice(1, actionsSortByActionTime.length)
        preActions.zip(postActions).map {
          case (preAction, postAction) => preAction.page_id + "->" + postAction.page_id
        }
      }
    }

    // 3.2 从跳转流过滤出目标跳转流
    val targetJumpFlow: RDD[String] = jumpFlow.filter(targetJumpPages.contains(_))

    // 3.3 统计跳转次数 ("1->2",count) ("2->3",count)
    val targetJumpFlowCount: collection.Map[String, Long] = targetJumpFlow.map((_, 1)).countByKey()

    // 4 计算跳转率 ("1->2",12.34%)
    val formatter = new DecimalFormat("0.00%") // 0.1233567 -> 23.34%
    val targetJumpRate: collection.Map[String, Any] = targetJumpFlowCount.map {
      case (jump, count) => {
        (jump, formatter.format(count.toDouble / targetPagesCountMap.getOrElse(jump.split("->")(0).toLong, 0L)))
      }
    }

    println("目标页面跳转率")
    targetJumpRate.foreach(print)

    // 5 写入mysql
    val sqlArgs: Iterable[Array[Any]] = targetJumpRate.map {
      case (jump, conversion_rate) => Array(taskId, jump, conversion_rate)
    }
    JDBCUtil.executeUpdate("use sparkmall", null)
    JDBCUtil.executeUpdate("truncate table page_conversion_rate", null)
    JDBCUtil.executeBatchUpdate("insert into  page_conversion_rate values(?, ?, ?)", sqlArgs)
  }

  //  def main(args: Array[String]): Unit = {
  //    statPageConversion(null,null,"1,2,3,4,5",null)
  //  }

}
