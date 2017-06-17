import java.net._
import java.util.HashMap
import io.confluent.kafka.serializers._
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.receiver._
import org.apache.avro._
import org.apache.avro.generic._
import org.apache.avro.generic.GenericData._
import org.json4s._
import org.json4s.jackson.JsonMethods._

// AnomalyDetectorConfig case class
case class AnomalyDetectorConfig(
  toy: Boolean = false,
  generator: String = "drift",
  eventsPerSec: Int = 5,
  publish: Boolean = false,
  schemaRegistryHost: String =  "localhost",
  schemaRegistryPort: Int = 8081,
  kafkaBrokerHost: String = "localhost",
  kafkaBrokerPort: Int = 9092,
  topic: String,
  field: String,
  carbonHost: String = "localhost",
  carbonPort: Int = 2003,
  carbonMetric: String,
  cpDir: String,
  batchInterval: Long = 2,
  slidingWindow: Long,
  slidingInterval: Long,
  numStd: Int = 2
)

// AnomalyDetector class
class AnomalyDetector(sc: SparkContext, config: AnomalyDetectorConfig) {

  // import the functions defined in the companion object
  // import AnomalyDetector._

  def subscribeToToyReceiver(ssc: StreamingContext, config: AnomalyDetectorConfig): DStream[(Long,Double)] = {
    ssc.receiverStream(new DummySource(config.generator, config.eventsPerSec))
      .map(x => (System.currentTimeMillis()/1000, x.toDouble))
  }

  def subscribeToKafkaStream(ssc: StreamingContext, config: AnomalyDetectorConfig): DStream[(Long,Double)] = {

    val brokerList = config.kafkaBrokerHost + ":" + config.kafkaBrokerPort.toString
    //TODO handle several brokers in config
    val schemaRegistry = "http://" + config.schemaRegistryHost + ":" + config.schemaRegistryPort.toString
    val topicSet = Set(config.topic)
    val kafkaParams = Map(
      "group.id" -> "consumerGroup1",
      "auto.offset.reset" -> "largest",
      "bootstrap.servers" -> brokerList,
      "schema.registry.url" -> schemaRegistry
    )

    val dataStream = KafkaUtils.createDirectStream[
      Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaParams, topicSet)

    val stream = reshape(dataStream, config)
      .map(x => (x._1.toLong/1000, x._2.toDouble))

    stream
  }

  def sendGrafana(stream: DStream[(Long,Double)], config: AnomalyDetectorConfig, metricSuffix: String): Unit = {

    // Creation of a the Grafana dashboard connection parameter(s)
    val fullAddress = new InetSocketAddress(config.carbonHost, config.carbonPort)

    // Display the original data
    stream.foreachRDD { rdd =>
      if (!(rdd.isEmpty())) {
        rdd.foreachPartition { partitionOfRecords =>
          if (!(partitionOfRecords.isEmpty)) {
            val gc = new GraphiteConnection(fullAddress)
            partitionOfRecords.foreach { avroRecord =>
              gc.send(config.carbonMetric + "." + metricSuffix, avroRecord._2, avroRecord._1)
            }
            gc.close()
          }
        }
      }
    }
  }

  def publishToKafka(stream: DStream[(Long,Double)], config: AnomalyDetectorConfig): Unit = {

    stream.foreachRDD { rdd =>
      if (!(rdd.isEmpty())) {
        rdd.foreachPartition { partitionOfRecords =>
          if (!(partitionOfRecords.isEmpty)) {
            val kafkaOpTopic = config.topic
            val schemaReg = "http://" + config.schemaRegistryHost + ":" + config.schemaRegistryPort.toString
            val broker = config.kafkaBrokerHost + ":" + config.kafkaBrokerPort.toString
            val producerProps = new HashMap[String, Object]()
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
            producerProps.put("schema.registry.url", schemaReg)
            val producer = new KafkaProducer[String, GenericRecord](producerProps)
            partitionOfRecords.foreach { record =>
              val url = schemaReg + "/subjects/" + kafkaOpTopic + "/versions/latest"
              val jsonString  = scala.io.Source.fromURL(url).mkString
              val json = org.json4s.jackson.JsonMethods.parse(jsonString)
              val schemaField = compact(json \\ "schema")
              val avroSchema = schemaField.substring(1, schemaField.length()-1).replaceAll("\\\\\"", "\"")
              val schema = new Schema.Parser().parse(avroSchema)
              val data = new GenericData.Record(schema)
              data.put(config.field, record._2.toFloat) // #2
              data.put("unit", "KELVIN")
              val headerData = new GenericData.Record(schema.getField("header").schema())
              headerData.put("sourceSystem", "dummy_source_system")
              headerData.put("sourceModule", "dummy_source_module")
              headerData.put("time", record._1 * 1000) // #1
              data.put("header", headerData)
              val message = new ProducerRecord[String, GenericRecord](kafkaOpTopic, null, data)
              producer.send(message)
            }
            producer.close()
          }
        }
      }
    }
  }

  def reshape(stream: DStream[(Object,Object)], config: AnomalyDetectorConfig): DStream[(String,String)] = {

    val resultStream = stream.map(_._2).filter { avroRecord => //filter correct source
      val uxv: String = avroRecord.asInstanceOf[GenericRecord]
        .get("header")
        .asInstanceOf[GenericRecord]
        .get("sourceSystem")
        .toString
      (uxv == "dummy_source_system")
    }.filter { avroRecord => //filter correct unit
      val unit: String = avroRecord.asInstanceOf[GenericRecord]
        .get("unit")
        .toString
      (unit == "KELVIN")
    }.map { avroRecord =>
      val time: String = avroRecord.asInstanceOf[GenericRecord]
        .get("header")
        .asInstanceOf[GenericRecord]
        .get("time")
        .toString
      val value: String = avroRecord.asInstanceOf[GenericRecord]
        .get(config.field)
        .toString
      (time, value)
    }

    return resultStream
  }

  // Core of the streaming anomaly detection via running average algorithm
  def runningAverageDetection(stream: DStream[(Long, Double)], config: AnomalyDetectorConfig): DStream[(Long,Double)] = {

    val window: Duration = Seconds(config.slidingWindow)
    val slide: Duration = Seconds(config.slidingInterval) //interval after which an RDD is  generated in DStream
    val slideStream = stream.map(x => (x._1, x._2, 1.0)).reduceByWindow(
      (x1,x2) => (math.max(x1._1,x2._1), x1._2 + x2._2, x1._3 + x2._3),
      slide, slide //only take new points: the one in the slide interval
    ).map { x =>  // (most recent timestamp, concatenation of the values, sum over slide, count over slide)
      val timestamp = x._1
      val mean = x._2/x._3
      (timestamp, mean)
    }

    val resultStream = stream.map(x => (x._1, x._2.toString, x._2, 1.0)).reduceByWindow(
      (x1,x2) => (math.max(x1._1,x2._1), x1._2.concat("!").concat(x2._2), x1._3 + x2._3, x1._4 + x2._4),
      window, slide
    ).map { x =>  // (most recent timestamp, concatenation of the values, sum over window, count over window)
      val timestamp = x._1
      val values = x._2.split("!").map(_.toDouble).toList
      val mean = x._3/x._4
      // compute the variance
      val variance = values.map(value => math.pow(value - mean,2)).sum/x._4
      (timestamp, (mean, variance))
    }.join(slideStream).map { x => // both streams have the same keys
      val timestamp = x._1
      val meanWindow = x._2._1._1
      val varianceWindow = x._2._1._2
      val meanSlide = x._2._2
      val isAnomaly =
        if (math.abs(meanSlide-meanWindow) >
              config.numStd.toDouble*math.sqrt(varianceWindow)) 1.0 else 0.0
      (timestamp, isAnomaly)
    }

    return resultStream
  }

  // Aggregator function
  def creatingFunc(): StreamingContext = {

    val ssc = new StreamingContext(sc, Seconds(config.batchInterval))

    ssc.checkpoint(config.cpDir)

    if (config.toy) {
      val toyStream = subscribeToToyReceiver(ssc, config)
      if (config.publish) {
        // Publish the toy data to kafka
        publishToKafka(toyStream, config)
        // Subscribe to the topic the toy data was published under
        val rawStream = subscribeToKafkaStream(ssc, config)
        // Process the stream
        val resultStream = runningAverageDetection(rawStream, config)
        // Send both unprocessed and processed data to the dashboard
        sendGrafana(rawStream, config, "raw")
        sendGrafana(resultStream, config, "result")
      } else {
        val resultStream = runningAverageDetection(toyStream, config)
      }
    } else {
      // Subscribe to the topic
      val rawStream = subscribeToKafkaStream(ssc, config)
      // Process the stream
      val resultStream = runningAverageDetection(rawStream, config)
    }

    // Make sure the DStream keeps the data around for at least as long as it takes for
    // the processing to complete over the batch
    ssc.remember(Seconds(60))

    ssc
  }

}

object AnomalyDetector {
  // contains all the functions that will be used in the class and applied to dstreams
  // it enables us not to have to make AnomalyDetector a serializable class

  // ERRATUM: COMPANION OBJECT NOT SUPORTED IN ZEPPELIN YET

}
