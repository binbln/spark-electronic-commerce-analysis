package com.xzb.sparkmall.offline.acc

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * @author xzb
  *
  *         累加器
  *
  *         输入类型: (品类id,action) => (品类1,"click")
  *         输出类型: Map[(品类1,"click"),100]
  */
class MapAccumulator extends AccumulatorV2[(String, String), mutable.Map[((String, String)), Long]] {

  private val map: mutable.Map[(String, String), Long] = mutable.Map[((String, String)), Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[(String, String), mutable.Map[(String, String), Long]] = {

    val newAcc = new MapAccumulator

    map.synchronized {

      newAcc.map ++= map

    }

    newAcc
  }

  override def reset(): Unit = map.clear

  override def add(v: (String, String)): Unit = {

    map(v) = map.getOrElse(v, 0L) + 1L

  }

  //合并
  override def merge(other: AccumulatorV2[(String, String), mutable.Map[(String, String), Long]]): Unit = {

//    for (elem <- other.value) {
//
//      if (map.get(elem._1).isEmpty) map(elem._1) = elem._2
//
//      else map(elem._1) += elem._2
//
//    }

    other.value.foreach {
      case (k, count) => {
        map.put(k, map.getOrElse(k, 0L) + count)
      }
    }

  }

  override def value: mutable.Map[(String, String), Long] = map
}
