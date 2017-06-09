import org.apache.spark.streaming.receiver._

// This is a dummy receiver that will generate toy data
class DummySource extends Receiver[Int](org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_2) {

  import scala.util.Random
  import org.apache.spark._
  import org.apache.spark.storage._
  import org.apache.spark.streaming._

  // Start the thread that receives data over a connection
  def onStart() {
    new Thread("Dummy Source") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {  }

  // Periodically generate a random number from 0 to 9, and the timestamp
  def receive() {
    val gen = new Generator
    while(!isStopped()) {
      store(gen.next())
      Thread.sleep(200)
    }
  }
}

class Generator0 extends Iterator[Double] {
  def hasNext(): Boolean = {
    // nothing fancy here: the generator is meant to run indefinitely
    true
  }
  def next() = {
    val drift: Long = 400L
    val currentTime: Long = System.currentTimeMillis()/1000
    if ((currentTime/drift) % 2) { // no need for floor, since both are Long
      // we can fit an even number of drifts in the current time
      10.0 + Random.nextGaussian()
    } else {
      // we can fit an odd number of drifts in the current time
      -10.0 + Random.nextGaussian()
    }
  }
}
