import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming.dstream.DStream

case class StreamingKmeansConfig(
  k: Int = 10,
  decayFactor: Double = 1.0,
  randomCentersWeight: Double = 0.0,
  randomCentersSeed: Long = 1L,
  features: Array[String]
)

class StreamingKmeans() {

  def train(stream: DStream[Map[String, String]], config: StreamingKmeansConfig):
    StreamingKMeans = {

    val vectorStream = stream.map { avroRecord =>
      val values = config.features.map(f => avroRecord(f).toDouble)
      Vectors.dense(values)
    }

    val streamingKmeans = new StreamingKMeans()
      .setK(config.k)
      .setDecayFactor(config.decayFactor)
      .setRandomCenters(config.features.length,
                        config.randomCentersWeight,
                        config.randomCentersSeed)

    streamingKmeans.trainOn(vectorStream)

    streamingKmeans
  }

}
