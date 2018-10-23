// trim down on these imports

// import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import sqlContext.implicits._
import spark.implicits._
import org.apache.spark.ml.feature.Bucketizer
import funcs.transposeUDF

val field_names_cat = List("State", "merchantname", "cardissueridencode")
val field_names_cont = List("transactionamount")

val df = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("inferSchema", "true").load("./data/data_mod.csv")

df.cache()

// Lift for categorical fields

var dfs = new ListBuffer[DataFrame]()

val noTrxn = df.count()
val noFraud = df.select(sum(df("response"))).collect()(0)(0).toString.toInt // access this more concisely

for (field <- field_names_cat) {

  // turn the below into a function (since re-use)
  var df_agg = df.groupBy(df.col(field))
                 .agg((count(df.col("response")) / noTrxn * 100).alias("trxn_percent"), (sum(df.col("response")) / noFraud * 100).alias("fraud_percent"))

  var df_agg_2 = df_agg.withColumn("lift", df_agg.col("trxn_percent") / df_agg.col("fraud_percent"))

  var df_agg_3 = df_agg_2.withColumn("field", lit(field))
  
  var df_agg_4 = df_agg_3.withColumnRenamed(field, "bin")
  
  dfs += df_agg_4
}

var df_pre_final = dfs.reduce(_.union(_))

val columns = df_pre_final.columns
val reordColumnNames = Array("field", "bin", "trxn_percent", "fraud_percent", "lift")
val df_final = df_pre_final.select(reordColumnNames.head, reordColumnNames.tail: _*)

// df_final.show(100)

// Lift for numeric (continuous) fields

// function to transpose dataframe

/*
def transposeUDF(transDF: DataFrame, transBy: Seq[String]): DataFrame = {
  val (cols, types) = transDF.dtypes.filter{ case (c, _) => !transBy.contains(c)}.unzip
  require(types.distinct.size == 1)

  val kvs = explode(array(
    cols.map(c => struct(lit(c).alias("column_name"), col(c).alias("column_value"))): _*
  ))

  val byExprs = transBy.map(col(_))

  transDF
    .select(byExprs :+ kvs.alias("_kvs"): _*)
    .select(byExprs ++ Seq($"_kvs.column_name", $"_kvs.column_value"): _*)
}
*/

val percentiles_choose = (1 to 9).toArray.map(_/10.toDouble)
// val percentiles_choose = (1 to 99).toArray.map(_/100.toDouble)
val pct_array_size = percentiles_choose.size

var dfs_cont = new ListBuffer[DataFrame]()

for (field_cont <- field_names_cont) {
  
  var sql_query = "SELECT 'dummy' AS dummy,"

  for ((p, idx) <- percentiles_choose.zipWithIndex) {
    if (idx != pct_array_size - 1) { 
          sql_query = sql_query + s" PERCENTILE(${field_cont}, ${p}) AS p_idx_${idx},"
        } else {
          sql_query = sql_query + s" PERCENTILE(${field_cont}, ${p}) AS p_idx_${idx} FROM df"
        }
  }

  df.createOrReplaceTempView("df")

  val percentiles = sqlContext.sql(sql_query)

  val percentiles_final = transposeUDF(percentiles, Seq("dummy")).drop("dummy")

  var pct_array = percentiles_final.select("column_value").collect().map(_(0)).toArray

  if (pct_array.head.toString.toDouble != 0 ) { // want to add: || pct_array.head.toString.toInt != 0
    pct_array = 0.0 +: pct_array // prepend
  }

  pct_array = pct_array :+ Double.PositiveInfinity // append

  var pct_array_dbl = pct_array.map(_.toString).map(_.toDouble) // pct_array.map(_.asInstanceOf[Double])
  var df_field_cont = df.select(field_cont, "response") //.collect().map(_(0)).toArray

  var bucketizer = new Bucketizer()
    .setInputCol(field_cont)
    .setOutputCol("bin")
    .setSplits(pct_array_dbl)

  // transform data into its bucket index
  var df_field_bucket = bucketizer.transform(df_field_cont)
  
  // forget the join, selected target variable above
  // var df_field_bucket_2 = df_field_bucket.join(df.select(field_cont, "response"), df_field_bucket(field_cont) === df(field_cont), "inner")
  
  var df_2 = df_field_bucket.drop(field_cont).select("bin", "response").withColumnRenamed("bin", field_cont)

  // turn the below into a function (since re-use)
  var df_agg = df_2.groupBy(df_2.col(field_cont))
                 .agg((count(df_2.col("response")) / noTrxn * 100).alias("trxn_percent"), (sum(df_2.col("response")) / noFraud * 100).alias("fraud_percent"))
  
  var df_agg_2 = df_agg.withColumn("lift", df_agg.col("trxn_percent") / df_agg.col("fraud_percent"))

  var df_agg_3 = df_agg_2.withColumn("field", lit(field_cont))
  
  var df_agg_4 = df_agg_3.withColumnRenamed(field_cont, "bin")
  
  dfs_cont += df_agg_4
}

var df_pre_final = dfs_cont.reduce(_.union(_))

val df_cont_final = df_pre_final.select(reordColumnNames.head, reordColumnNames.tail: _*)

// df_cont_final.show(100)

// Union of categorical and continuous fields dataframes

val df_lift = df_final.union(df_cont_final)

df_lift.show()
