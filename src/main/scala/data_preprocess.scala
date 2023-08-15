import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

object data_preprocess {

  // 数据预处理函数
  // 使用“距离洲内商家平均位置的欧式距离”来除去离群值。
  def data_process(df:DataFrame, spark:SparkSession):Unit={
    // 对categories字段数据进行切分
    val split_col=functions.split(df("categories"),",")
    val df2=df.withColumn("categories",split_col).filter(df("city") =!= "").na.drop()
    // println(df2.show(10))
    df2.createOrReplaceTempView("business")


    val b_etl=spark.sql("select " +
      "business_id," +
      "name," +
      "city," +
      "state," +
      "latitude," +
      "longitude," +
      "stars," +
      "review_count," +
      "is_open," +
      "categories," +
      "attributes " +
      "from business")
    b_etl.createOrReplaceTempView("b_etl")


    val sql_str="select " +
      "b1.business_id," +
      "sqrt(power(b1.latitude-b2.avg_lat,2)+power(b1.longitude-b2.avg_long,2)) dist " +
      "from b_etl b1 " +
      "inner join " +
      "(select state,avg(latitude) avg_lat,avg(longitude) avg_long from b_etl group by state) b2 " +
      "on b1.state=b2.state " +
      "order by dist desc"
    val outlier=spark.sql(sql_str)
    outlier.createOrReplaceTempView("outlier")


    val sql_str2="select " +
      "b.* " +
      "from b_etl b " +
      "inner join outlier o " +
      "on b.business_id=o.business_id " +
      "where o.dist<10"
    val joined=spark.sql(sql_str2)
    // 将预处理好的数据，进行保存
    joined.write.format("parquet").mode(SaveMode.Overwrite).save("D:\\Yelp_data_analysis\\src\\main\\data\\yelp_business_etl")
  }



  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("yelp_data_analysis")
    val sc=new SparkContext(conf)
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val df=spark.read.format("json")
      .option("inferSchema","true")
      .load("D:\\Yelp_data_analysis\\src\\main\\data\\yelp_academic_dataset_business.json")
    //  println(df.printSchema())
    //  println(df.show(10))
    data_process(df,spark)
  }
}
