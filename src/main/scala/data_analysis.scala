import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object data_analysis {

  def analysis(spark:SparkSession):Unit={
    val part_business=spark.sql("select " +
      "state,city,stars,review_count,explode(categories) as category " +
      "from df").cache()
    part_business.createOrReplaceTempView("part_business_1")


    val part_business_2=spark.sql("select " +
      "state," +
      "city," +
      "stars," +
      "review_count,replace(category,' ','') as new_category " +
      "from part_business_1")
    part_business_2.createOrReplaceTempView("part_business_2")
  }

  // all distinct categories
  // 商业类别
  def analysis2(spark:SparkSession):Unit={
    val sql_str1="select " +
      "business_id," +
      "explode(categories) category " +
      "from df"
    val all_categories=spark.sql(sql_str1)
    all_categories.createOrReplaceTempView("all_categories")
    val sql_str2="select " +
      "count(distinct(new_category)) count " +
      "from part_business_2"
    val distinct=spark.sql(sql_str2)
    distinct.show()
  }

  // Top 10 business categories
  // 美国10种主要的商业类别
  def analysis3(spark:SparkSession):Unit={
    val sql_str3="select " +
      "new_category," +
      "count(*) freq " +
      "from part_business_2 " +
      "group by new_category " +
      "order by freq desc"
    val top_cat=spark.sql(sql_str3)
    top_cat.show()
  }

  //Top business categories - in every city
  //每个城市各种商业类型的商家数量
  def analysis4(spark:SparkSession):Unit={
    val sql_str4="select " +
      "city," +
      "new_category," +
      "count(*) freq " +
      "from part_business_2 " +
      "group by city,new_category " +
      "order by freq desc"
    val top_cat_city=spark.sql(sql_str4)
    top_cat_city.show()
  }


  // Cities with most businesses
  // 商家数量最多的10个城市
  def analysis5(spark:SparkSession): Unit ={
    val sql_str5="select " +
      "city," +
      "count(business_id) as no_of_bus " +
      "from df " +
      "group by city " +
      "order by no_of_bus desc " +
      "limit 10"
    val bus_city=spark.sql(sql_str5)
    bus_city.show()
  }

  // 消费者评价最多的10种商业类别
  // Average review count by category
  def analysis6(spark:SparkSession)={
    val sql_str6="select " +
      "new_category," +
      "avg(review_count) as avg_review_count " +
      "from part_business_2 " +
      "group by new_category " +
      "order by avg_review_count desc " +
      "limit 10"
    val df2=spark.sql(sql_str6)
    df2.show()
  }

  // 最受消费者喜欢的前10种商业类型
  // Average stars by category
  def analysis7(spark:SparkSession)={
    val sql_str7="select " +
      "new_category," +
      "avg(stars) avg_stars " +
      "from part_business_2 " +
      "group by new_category " +
      "order by avg_stars desc"
    val df3=spark.sql(sql_str7)
    df3.show()
  }

  // 商业额外业务的评价情况
  // Data based on Attribute
  def analysis8(spark:SparkSession): Unit ={
    val for_att=spark.sql("select " +
      "attributes," +
      "stars," +
      "explode(categories) as category " +
      "from df")
    for_att.createOrReplaceTempView("for_att")
    val att=spark.sql("select " +
      "attributes.RestaurantsTakeOut as RestaurantsTakeOut," +
      "category," +
      "stars " +
      "from for_att").na.drop()
    att.createOrReplaceTempView("att")
    val att_group=spark.sql("select " +
      "RestaurantsTakeOut," +
      "avg(stars) as stars " +
      "from att " +
      "group by RestaurantsTakeOut " +
      "order by stars")
    att_group.show()
  }

  def analysis9(spark:SparkSession,aaa:String): Unit ={
    val sql_str="select"
  }

  def main(args: Array[String]): Unit = {
    // 读取文件
    val data_path="D:\\Yelp_data_analysis\\src\\main\\data\\yelp_business_etl"
    val conf=new SparkConf().setMaster("local[*]").setAppName("data_analysis")
    val sc=new SparkContext(conf)
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val df=spark.read.format("parquet").load(data_path).cache()
    df.createOrReplaceTempView("df")
    analysis(spark)
//    analysis2(spark)
//    analysis3(spark)
//    analysis4(spark)
//    analysis5(spark)
//    analysis6(spark)
//    analysis7(spark)
    analysis8(spark)
  }
}
