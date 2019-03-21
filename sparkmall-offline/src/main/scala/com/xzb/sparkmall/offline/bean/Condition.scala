package com.xzb.sparkmall.offline.bean

/**
  * @author xzb
  */

/**
  *  封装从 hive 中读数据时的过滤条件
  *
  * @param startDate
  * @param endDate
  * @param startAge
  * @param endAge
  * @param professionals
  * @param city
  * @param gender
  * @param keywords
  * @param categoryIds
  * @param targetPageFlow
  */
case class Condition(var startDate: String,
                     var endDate: String,
                     var startAge: Int,
                     var endAge: Int,
                     var professionals: String,
                     var city: String,
                     var gender: String,
                     var keywords: String,
                     var categoryIds: String,
                     var targetPageFlow: String)

/**
  *
  *   封装写入 Msyql 的数据.
  *
  * @param taskId
  * @param categoryId
  * @param clickCount
  * @param orderCount
  * @param payCount
  */
case class CategoryCountInfo(taskId: String,
                             categoryId: String,
                             clickCount: Long,
                             orderCount: Long,
                             payCount: Long)