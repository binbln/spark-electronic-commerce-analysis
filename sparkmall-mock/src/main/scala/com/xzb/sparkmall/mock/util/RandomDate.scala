package com.xzb.sparkmall.mock.util

import java.util.Date

/**
  * @author xzb
  */
object RandomDate {
  def apply(startDate: Date, stopDate: Date, step: Int) = {
    val randomDate = new RandomDate
    val avgStepTime = (stopDate.getTime - stopDate.getTime) / step
    randomDate.maxStepTime = 4 * avgStepTime
    randomDate.lastDateTIme = startDate.getTime
    randomDate
  }
}

class RandomDate {
  // 上次 action 的时间
  var lastDateTIme: Long = _
  // 每次最大的步长时间
  var maxStepTime: Long = _

  /**
    * 得到一个随机时间
    *
    * @return
    */
  def getRandomDate = {
    // 这次操作的相比上次的步长
    val timeStep = RandomNumUtil.randomLong(0, maxStepTime)
    lastDateTIme += timeStep
    new Date(lastDateTIme)
  }
}
