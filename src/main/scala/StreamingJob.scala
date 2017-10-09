import io.confluent.kafka.serializers.KafkaAvroDecoder
import org.apache.avro.generic.GenericRecord
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import java.net.InetSocketAddress


case class StreamingJobConfig(
  // Network
  schemaRegistryHost: String =  "localhost",
  schemaRegistryPort: Int = 8081,
  kafkaBrokerHost: String = "localhost",
  kafkaBrokerPort: Int = 9092,
  carbonHost: String = "localhost",
  carbonPort: Int = 2003,
  // Spark Streaming
  cpDir: String,
  batchInterval: Long = 2,
  // Stream shaping
  topic: String,
  selectedFields: Array[String],
  filter: Array[Any],
  content: Array[String]
)

class StreamingJob() extends Serializable {

  // Function to implement in order to have a one-line wrapper
  def initiate(): Unit = {
    println("hi")
  }

  // Function that extracts values of fields and builds the map (fieldName -> fieldValue)
  def extractValues(avroRecord: Object, selectedFields: Array[String]): scala.collection.mutable.Map[String, String] = {
    // Create an empty map that will contain the mappings (fieldName -> fieldValue) by fetching its value from inside the record
    val name2value = collection.mutable.Map[String, String]()

    // Populate the name2value map for each field name in selectedFields
    selectedFields.foreach { fieldName =>
      // Split the field name according to the avro nested structure flattening convention, "." in our case
      println(fieldName)
      val steps = fieldName.split("\\.").map(_.trim)
      val depth = steps.length
      println(s"Depth: ${depth}")
      // Creation of the field value as a var since we will interatively go down the steps to grab the desired field (leaf)
      var fieldValue = avroRecord.asInstanceOf[GenericRecord].get(steps(0))
      // If it created here and not in the next loop as any fieldName has a least a depth of 1
      println(fieldValue)
      for (i <- (1 to depth - 1)) {
        // We loop over the remaning embedding levels and stop when we reach the leaf
        fieldValue = fieldValue.asInstanceOf[GenericRecord].get(steps(i))
      }
      // Print the leaf value corresponding to the value associated with the fieldName flattening
      println(s"Final value: ${fieldValue}")
      // Finally add the mapping (fieldName -> fieldValue) to name2value
      name2value += (fieldName -> fieldValue.toString)
    }
    name2value
  }

  // Function that uses the name to value mapping extracted from the avro record on the user-specified filter
  def applyMap2Filter(filter: Array[Any], map: scala.collection.mutable.Map[String,String]): Boolean = {
    val mappedFilter = filter.map { c =>
      c match {
        // Operators for String values
        case (k: String, v: String, "==") => map(k) == v
        case (k: String, v: String, "!=") => map(k) != v
        // Operators for Double values
        case (k: String, v: Double, "==") => map(k).toDouble == v
        case (k: String, v: Double, "!=") => map(k).toDouble != v
        case (k: String, v: Double, "<") => map(k).toDouble < v
        case (k: String, v: Double, ">") => map(k).toDouble > v
        case (k: String, v: Double, "<=") => map(k).toDouble <= v
        case (k: String, v: Double, ">=") => map(k).toDouble >= v
        case array: Array[Any] =>
          array.map { d =>
            d match {
              // Operators for String values
              case (k: String, v: String, "==") => map(k) == v
              case (k: String, v: String, "!=") => map(k) != v
              // Operators for Double values
              case (k: String, v: Double, "==") => map(k).toDouble == v
              case (k: String, v: Double, "!=") => map(k).toDouble != v
              case (k: String, v: Double, "<") => map(k).toDouble < v
              case (k: String, v: Double, ">") => map(k).toDouble > v
              case (k: String, v: Double, "<=") => map(k).toDouble <= v
              case (k: String, v: Double, ">=") => map(k).toDouble >= v
              case _ =>
                println("Unconventional filter. Read the doc.")
                throw new IllegalArgumentException
            }
          }.foldLeft(false)(_ || _) // start the fold with false: any boolean b is invariant by "false || b" operation
                                    // Note that .foldLeft(false)(_ || _) even works for Array.empty[Boolean] while a reduce(_ || _) does not
        case _ =>
          println("Unconventional filter. Read the doc.")
          throw new IllegalArgumentException
      }
    }.foldLeft(true)(_ && _) // start the fold with true: any boolean b is invariant by "true && b" operation
                             // Note that .foldLeft(true)(_ && _) even works for Array.empty[Boolean] while a reduce(_ && _) does not
    mappedFilter
  }

  // Function that uses the two previous auxiliary functions to calibrate a DStream for what is specified in the config
  def calibrate(stream: DStream[(Object,Object)], config: StreamingJobConfig): DStream[Map[String, String]] = {
    val calibratedStream = stream.map(_._2).filter { avroRecord =>
      // Extract the values of the fields whose names are in the selectedFields array
      // The returned object is a mapping (fieldName -> fieldValue), for all fieldName in config.selectedFields
      val extractionMap = extractValues(avroRecord, config.selectedFields)
      // Now that the values of the selected fields have been extracted from the current avro record,
      // we generate a Boolean which answers the question:
      // Should the avro record be accepted based on the criteria specified in the filter?
      val isValid = applyMap2Filter(config.filter, extractionMap)

      isValid
    }.map { avroRecord =>
      // Extract the values of the fields whose names are in the selectedFields array
      // The returned object is a mapping (fieldName -> fieldValue), for all fieldName in config.selectedFields
      val extractionMap = extractValues(avroRecord, config.selectedFields)
      // Arrange what the stream will output, which is what is specified in config.contents
      val output = config.content.map(k => k -> extractionMap(k)).toMap
      // The output is a map because it enables easy grabbing of any values knowing the field name

      output
    }
    calibratedStream
  }

  // Function that sends the content of a stream to a Grafana dashboard
  // It sends the content of a scalar field present in every record of the stream
  // The fields containing time information has to be specified to dsiplay the content as a time series
  def sendGrafana(stream: DStream[Map[String, String]], config: StreamingJobConfig, timeField: String, valueField: String, metric: String): Unit = {
    // Creation of a the Grafana dashboard connection parameter(s)
    val fullAddress = new InetSocketAddress(config.carbonHost, config.carbonPort)

    // Display the original data
    stream.foreachRDD { rdd =>
      if (!(rdd.isEmpty())) {
        rdd.foreachPartition { partitionOfRecords =>
          if (!(partitionOfRecords.isEmpty)) {
            val gc = new GraphiteConnection(fullAddress)
            partitionOfRecords.foreach { avroRecord =>
              gc.send(metric, avroRecord(valueField), avroRecord(timeField))
            }
            gc.close()
          }
        }
      }
    }
  }

  // Function that returns the DStream whose shape is specified in the StreamingJobConfig object passed in parameter
  def subscribeToKafkaStream(ssc: StreamingContext, myStreamingJobConfig: StreamingJobConfig): DStream[Map[String, String]] = {
    val brokerList = myStreamingJobConfig.kafkaBrokerHost + ":" + myStreamingJobConfig.kafkaBrokerPort.toString
    val schemaRegistry = "http://" + myStreamingJobConfig.schemaRegistryHost + ":" + myStreamingJobConfig.schemaRegistryPort.toString
    val topicSet = Set(myStreamingJobConfig.topic)
    val kafkaParams = Map(
      "group.id" -> "consumerGroup1",
      "auto.offset.reset" -> "largest",
      "bootstrap.servers" -> brokerList,
      "schema.registry.url" -> schemaRegistry
    )

    val dataStream = KafkaUtils.createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaParams, topicSet)
    val calibratedStream = calibrate(dataStream, myStreamingJobConfig)

    calibratedStream
  }

}
