// AnomalyDetectorConfig case class
case class AnomalyDetectorConfig(
  zkPort: Int = 2181,
  schemaRegistryAddress: String,
  schemaRegistryPort: Int = 8081,
  kafkaBrokerAddress: String,
  kafkaBrokerPort: Int = 9092,
  topic: String,
  field: String,
  carbonAddress: String,
  carbonPort: Int = 2003,
  carbonMetric: String,
  cpDir: String,
  batch: Long = 2,
  window: Long,
  slide: Long,
  numStd: Int
)

// AnomalyDetector class
class AnomalyDetector(sc: org.apache.spark.SparkContext, config: AnomalyDetectorConfig) {

  import io.confluent.kafka.serializers._
  import kafka.serializer.StringDecoder
  // import org.apache.spark._
  // import org.apache.spark.storage._
  import org.apache.spark.streaming._
  import org.apache.spark.streaming.dstream._
  import org.apache.spark.streaming.kafka._
  import java.net._
  // import org.apache.avro._
  import org.apache.avro.generic._
  // import org.apache.avro.generic.GenericData._
  import org.apache.spark.streaming.receiver._ // bonus line only needed for the DummySource

  // import the functions defined in the object
  // import AnomalyDetector._

  // Necessary for kafkaParams
  val zkQuorum = config.kafkaBrokerAddress + ":" + config.zkPort.toString
  val metadataBrokerList = config.kafkaBrokerAddress + ":" + config.kafkaBrokerPort.toString
  val schemaRegistry = "http://" + config.schemaRegistryAddress + ":" +
    config.schemaRegistryPort.toString
  val topicSet = Set(config.topic)

  // Core of the streaming anomaly detection via running average algorithm
  def runningAverageDetection(slide: Duration, window: Duration,
    stream: DStream[(Long, Double)], numstd: Int): DStream[(Long,Double)] = {
    val slideStream = stream
      .map(x => (x._1, x._2, 1.0))
      .reduceByWindow((x1,x2) => {
        (math.max(x1._1,x2._1), x1._2 + x2._2, x1._3 + x2._3)
      }, slide, slide) // only take new points
      .map(x => {
        val timestamp = x._1
        val mean = x._2/x._3
        (timestamp, mean)
      })
    val resultStream = stream
      .map(x => (x._1, x._2.toString, x._2, 1.0))
      .reduceByWindow((x1,x2) => {
        (math.max(x1._1,x2._1), x1._2.concat("!").concat(x2._2), x1._3 + x2._3, x1._4 + x2._4)
      }, window, slide)
      // (most recent timestamp, concatenation of the values, sum over window, count over window)
      .map(x => {
        val timestamp = x._1
        val values = x._2.split("!").map(_.toDouble).toList
        val mean = x._3/x._4
        // compute the variance
        val variance = values.map(value => math.pow(value - mean,2)).sum/x._4
        (timestamp, (mean, variance))
      })
      .join(slideStream).map(x => {
        val timestamp = x._1
        val meanWindow = x._2._1._1
        val varianceWindow = x._2._1._2
        val meanSlide = x._2._2
        val isAnomaly =
          if (math.abs(meanSlide-meanWindow) >
            numstd.toDouble*math.sqrt(varianceWindow)) 1.0 else 0.0
        (timestamp,isAnomaly)
      })
    return resultStream
  }

  // Stream analytics function
  def creatingFunc(): StreamingContext = {
    val ssc = new StreamingContext(sc, Seconds(config.batch))
    ssc.checkpoint(config.cpDir)

    val kafkaParams = Map(
      "group.id" -> "1",
      "auto.offset.reset" -> "largest",
      "metadata.broker.list" -> metadataBrokerList,
      "zookeeper.connect" -> zkQuorum,
      "schema.registry.url" -> schemaRegistry)

    // Creation of a the Graphite dashboard connection parameters
    val rawMetric = config.carbonMetric + ".raw"
    val anomalyMetric = config.carbonMetric + ".anomaly"
    val fullAddress = new InetSocketAddress(config.carbonAddress, config.carbonPort)

    // Streams creation
    val stream = ssc
      .receiverStream(new DummySource())
      .map(x => (System.currentTimeMillis()/1000, x.toDouble))

    //var offsetRanges = scala.Array[OffsetRange]()
    val dataStream = KafkaUtils.createDirectStream[
      Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaParams,topicSet)
      .map(_._2)
      .filter(avroRecord => { //filter correct source
        val uxv: String = avroRecord.asInstanceOf[GenericRecord]
          .get("header")
          .asInstanceOf[GenericRecord]
          .get("sourceSystem")
          .toString
        (uxv == "rawfie.mst.auv-1")
      })
      .filter(avroRecord => { //filter correct unit
        val unit: String = avroRecord.asInstanceOf[GenericRecord]
          .get("unit")
          .toString
        (unit == "KELVIN")
      })
      .map(avroRecord => {
        val time: String = avroRecord.asInstanceOf[GenericRecord]
          .get("header")
          .asInstanceOf[GenericRecord]
          .get("time")
          .toString
        val value: String = avroRecord.asInstanceOf[GenericRecord]
          .get(config.field)
          .toString
        (time, value)
      })

    // Sliding window
    val window: Duration = Seconds(config.window)
    // Sliding interval (interval after which the new DStream will generate RDDs)
    val slide: Duration = Seconds(config.slide)

    // val resultStream = stream
    //   .map(element => {
    //     if (element.toDouble > 7)
    //       (System.currentTimeMillis()/1000,1)
    //     else
    //       (System.currentTimeMillis()/1000,0)
    //   })

    val resultStream = runningAverageDetection(slide, window, stream, config.numStd)

    // Send the raw data to the dashboard
    stream.foreachRDD(rdd => {
      // Cast the rdd to an interface that lets us get an array of OffsetRange
      //val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      if (!(rdd.isEmpty())) {
        rdd.foreachPartition(partitionOfRecords => {
          if (!(partitionOfRecords.isEmpty)) {
            // index to get the correct offset range for the rdd partition we're working on
            //val osr: OffsetRange = offsetRanges(TaskContext.get.partitionId)
            val gc = new GraphiteConnection(fullAddress)
            partitionOfRecords.foreach(avroRecord => {
              gc.send(rawMetric, avroRecord._2.toDouble, avroRecord._1.toLong)
            })
            gc.close()
          }
        })
      }
    })

    // Send the processed data to the dashboard
    resultStream.foreachRDD(rdd => {
      // Cast the rdd to an interface that lets us get an array of OffsetRange
      //val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      if (!(rdd.isEmpty())) {
        rdd.foreachPartition(partitionOfRecords => {
          if (!(partitionOfRecords.isEmpty)) {
            // index to get the correct offset range for the rdd partition we're working on
            //val osr: OffsetRange = offsetRanges(TaskContext.get.partitionId)
            val gc = new GraphiteConnection(fullAddress)
            partitionOfRecords.foreach(avroRecord => {
              gc.send(anomalyMetric, avroRecord._2.toDouble, avroRecord._1.toLong)
              //System.currentTimeMillis()/1000
            })
            gc.close()
          }
        })
      }
    })

    ssc
  }
}

object AnomalyDetector {
  // contains all the functions that will be used in the class and applied to dstreams
  // it enables us not to have to make AnomalyDetector a serializable class

  // ERRATUM: COMPANION OBJECT NOT SUPORTED IN ZEPPELIN YET

}
