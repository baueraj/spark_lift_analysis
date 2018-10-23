package funcs

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