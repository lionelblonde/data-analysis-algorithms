import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

// AnomalyDetectorConfig case class
case class AnomalyDetectorConfig(
  slidingWindow: Long,
  slidingInterval: Long,
  numStd: Int = 2
)

// AnomalyDetector class
class AnomalyDetector(sc: SparkContext, config: AnomalyDetectorConfig) {

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
  def initiate(): StreamingContext = {

    val ssc = new StreamingContext(sc, Seconds(config.batchInterval))

    ssc.checkpoint(config.cpDir)

    // Subscribe to the kafka stream
    // TODO: Use subscribeToKafkaStream from a StreamingJob instance to subscribe to the kafka stream

    // Process the stream with runningAverageDetection
    val resultStream = runningAverageDetection(rawStream, config)

    // Send the results to the grafana dashboard
    // TODO: Use sendGrafana from a StreamingJob instance to send the results

    // Make sure the DStream keeps the data around for at least as long as it takes for
    // the processing to complete over the batch
    ssc.remember(Seconds(60))

    ssc
  }

}
