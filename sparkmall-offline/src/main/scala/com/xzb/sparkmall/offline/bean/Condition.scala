package com.xzb.sparkmall.offline.bean

/**
  * @author xzb
  *
  *         封装从 hive 中读数据时的过滤条件
  *
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

