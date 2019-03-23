package com.xzb.sparkmall.offline.app

import java.util.Properties

import com.xzb.sparkmall.common.util.ConfigurationUtil
import com.xzb.sparkmall.offline.udf.AreaClickCountUDAF
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author xzb
  */
object AreaClickApp {
  def statAreaClickTop3Product(spark: SparkSession, taskId: String) = {
    // 注册
    spark.udf.register("city_remark", new AreaClickCountUDAF)
    // 1.  查询出来所有的点击记录  和 城市表做了连接   t1
    spark.sql(
      """
        |select
        |    c.*,
        |    v.click_product_id
        |from city_info c join user_visit_action v on c.city_id = v.city_id
        |where click_category_id > -1
      """.stripMargin).createOrReplaceTempView("t1")
    // 2. 分组按照地区, 商品进行统计  t2
    spark.sql(
      """
        |select
        |    t1.area,
        |    t1.click_product_id,
        |    count(*) click_count,
        |    city_remark(t1.city_name) remark
        |from t1
        |group by t1.area, t1.click_product_id
      """.stripMargin).createOrReplaceTempView("t2")

    // 3.  进行排序  t3
    spark.sql(
      """
        |select
        |    *,
        |    rank() over(partition by t2.area order by click_count desc) rank
        |from t2
      """.stripMargin).createOrReplaceTempView("t3")

    // 4. top3
    val conf = ConfigurationUtil("config.properties")
    val props = new Properties()
    props.setProperty("user", conf.getString("jdbc.user"))
    props.setProperty("password", conf.getString("jdbc.password"))
    spark.sql(
      s"""
         |select
         |    *
         |from t3
         |where rank <= 3
            """.stripMargin)
      .write.mode(SaveMode.Overwrite)
      .jdbc(conf.getString("jdbc.url"), "area_click_top10", props)
  }
}
