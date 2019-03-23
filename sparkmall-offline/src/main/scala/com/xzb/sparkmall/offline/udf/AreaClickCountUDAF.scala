package com.xzb.sparkmall.offline.udf

import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * @author xzb
  *         自定义聚合函数
  */
class AreaClickCountUDAF extends UserDefinedAggregateFunction {

  // 输入的数据类型 String
  override def inputSchema: StructType = {
    StructType(StructField("city_name", StringType) :: Nil)
  }

  // 存储的数据类型 map-城市,次数 long-总次数
  override def bufferSchema: StructType = {
    StructType(StructField("city_count", MapType(StringType, LongType)) :: StructField("total_count", LongType) :: Nil)
  }

  // 输出的数据类型
  override def dataType: DataType = StringType

  // 相同的输入是否应该返回相同的输出
  override def deterministic: Boolean = true

  // 存储数据初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
    buffer(1) = 0L
  }

  // 分区内合并操作  executor内合并
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val map: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val city: String = input.getString(0)
    buffer(0) = map + (city -> (map.getOrElse(city, 0L) + 1L))
    buffer(1) = buffer.getLong(1) + 1L
  }

  // 输出 把存储的数据转换成字符串输出
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // (city,count)heb
    val map1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
    val map2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)
    buffer1(0) = map1.foldLeft(map2) {
      case (m, (k, v)) => {
        m + (k -> (m.getOrElse(k, 0L) + v))
      }
    }

    // 总点击 合并
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 输出 把存储的数据格式转换成字符串输出
  override def evaluate(buffer: Row): Any = {
    val cityCount: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val totalCount: Long = buffer.getLong(1)
    val cityRemarks: List[CityRemark] = cityCount.toList
      .sortBy(_._2)(Ordering.Long.reverse)
      .take(2)
      .map {
        case (city, count) => {
          CityRemark(city, count.toDouble / totalCount)
        }
      }

    val allRemarks = cityRemarks :+ CityRemark("其他", cityRemarks.foldLeft(1D)(_ - _.clickRatio))
    // 拼成字符串返回
    allRemarks.mkString(",")
  }
}

case class CityRemark(cityName: String, clickRatio: Double) {
  private val formatter = new DecimalFormat("0.00%")

  override def toString: String = s"$cityName:${formatter.format(clickRatio)}"
}
