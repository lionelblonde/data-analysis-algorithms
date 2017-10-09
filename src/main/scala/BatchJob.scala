import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import com.databricks.spark.avro._
import org.apache.spark.sql.functions.col

case class BatchJobConfig(
  // Hdfs path
  hdfsPath: String = "hdfs://hnn1-americano.di.uoa.gr:9000/topics/",
  // DataFrame shaping
  topic: String,
  selectedFields: Array[String],
  filter: Array[Any],
  content: Array[String]
)

class BatchJob() extends Serializable {

  def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap { f =>
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(new Column(colName))
      }
    }
  }

  def filterDataFrame(df: DataFrame, filter: Array[Any]): DataFrame = {
    // No choice here but to define a var
    // Filters will be successively applied to the df, since the sql conjonction syntax is too tricky
    var myFilteredDf = df
    val myFilters = filter.map { c =>
      c match {
        // Operators for String values
        case (k: String, v: String, "==") => df.col(s"`$k`") === s"$v"
        case (k: String, v: String, "!=") => df.col(s"`$k`") !== s"$v"
        // Operators for Double values
        case (k: String, v: Double, "==") => df.col(s"`$k`") === v
        case (k: String, v: Double, "!=") => df.col(s"`$k`") !== v
        case (k: String, v: Double, ">=") => df.col(s"`$k`") >= v
        case (k: String, v: Double, ">") => df.col(s"`$k`") > v
        case (k: String, v: Double, "<=") => df.col(s"`$k`") <= v
        case (k: String, v: Double, "<") => df.col(s"`$k`") < v
        case (k: String, v: Array[String], "isin") => df.col(s"`k`").isin(v: _*)
        case _ =>
          println("Unconventional filter. Read the doc.")
          throw new IllegalArgumentException
      }
    }.foreach(expr => myFilteredDf = myFilteredDf.filter(expr).toDF)

    myFilteredDf
  }

  def createDataFrame(sqlc: SQLContext, config: BatchJobConfig): DataFrame = {
    val fullHdfsPath = config.hdfsPath + config.topic
    // Pull data from hdfs
    val df = sqlc.read.avro(fullHdfsPath)
    // Format column names according to a our period-separated convention (e.g. header.time) to have the full path in the avro schema
    val flattenedSchema = flattenSchema(df.schema)
    val renamedCols = flattenedSchema.map(name => col(name.toString()).as(name.toString()))
    val flattenedDf = df.select(renamedCols:_*)
    // Filter data contained in the DataFrame according to the provided filter
    val filteredDf = filterDataFrame(flattenedDf, config.filter)
    // Organize columns according to the user specification
    val finalDf = filteredDf.select(config.content.toSeq.map(c => col(s"`$c`")): _*)

    finalDf
  }

}
