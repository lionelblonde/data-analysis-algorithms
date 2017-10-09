import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import com.databricks.spark.avro._

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
    val myFilter = filter.map { c =>
      c match {
        // Operators for String values
        case (k: String, v: String, "==") => df.col(s"`$k`").equalTo(s"$v")
        case (k: String, v: String, "!=") => df.col(s"`$k`").notEqual(s"$v")
        // Operators for Double values
        case (k: String, v: Double, "==") => df.col(s"`$k`").equalTo(v)
        case (k: String, v: Double, "!=") => df.col(s"`$k`").notEqual(v)
        case (k: String, v: Double, "<") => df.col(s"`$k`").lt(v)
        case (k: String, v: Double, ">") => df.col(s"`$k`").gt(v)
        case (k: String, v: Double, "<=") => df.col(s"`$k`").leq(v)
        case (k: String, v: Double, ">=") => df.col(s"`$k`").geq(v)
        case array: Array[Any] =>
           val tmpString = array.map { d =>
             d match {
               // Operators for String values
               case (k: String, v: String, "==") => df.col(s"`$k`").equalTo(s"$v")
               case (k: String, v: String, "!=") => df.col(s"`$k`").notEqual(s"$v")
               // Operators for Double values
               case (k: String, v: Double, "==") => df.col(s"`$k`").equalTo(v)
               case (k: String, v: Double, "!=") => df.col(s"`$k`").notEqual(v)
               case (k: String, v: Double, "<") => df.col(s"`$k`").lt(v)
               case (k: String, v: Double, ">") => df.col(s"`$k`").gt(v)
               case (k: String, v: Double, "<=") => df.col(s"`$k`").leq(v)
               case (k: String, v: Double, ">=") => df.col(s"`$k`").geq(v)
               case _ =>
                 println("Unconventional filter. Read the doc.")
                 throw new IllegalArgumentException
             }
           }.foldLeft("")(_ + "||" + _)
          "(" + tmpString.drop(2) + ")"
        case _ =>
          println("Unconventional filter. Read the doc.")
          throw new IllegalArgumentException
      }
    }.foldLeft("")(_ + "&&" + _)

    val myCleanedFilter = myFilter.drop(2)

    df.filter(s"""$myCleanedFilter""").toDF
  }

  def createDataFrame(sqlc: SQLContext, config: BatchJobConfig): DataFrame = {
    val fullHdfsPath = config.hdfsPath + config.topic
    // Pull data from hdfs
    val df = sqlc.read.avro(fullHdfsPath)
    // Format column names according to a our period-separated convention (e.g. header.time) to have the full path in the avro schema
    val flattenedSchema = flattenSchema(df.schema)
    val renamedCols = flattenedSchema.map(name => new Column(name.toString()).as(name.toString()))
    val flattenedDf = df.select(renamedCols:_*)
    // Filter data contained in the DataFrame according to the provided filter
    val filteredDf = filterDataFrame(flattenedDf, config.filter)
    // Organize columns according to the user specification
    // val finalDf = filteredDf.select(config.content.map(name => new Column(name.toString()).as(name.toString())).toSeq)
    val finalDf = filteredDf.select(config.content.toSeq.map(c => new Column(c)): _*)
    finalDf
  }

}
