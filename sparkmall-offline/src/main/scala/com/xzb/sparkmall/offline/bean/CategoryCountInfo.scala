package com.xzb.sparkmall.offline.bean

/**
  * @author xzb
  *
  *         封装写入 mysql 的数据
  *
  */

case class CategoryCountInfo(taskId: String,
                             categoryId: String,
                             clickCount: Long,
                             orderCount: Long,
                             payCount: Long)
