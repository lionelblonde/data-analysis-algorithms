import org.apache.spark.streaming.receiver._

// This is a dummy receiver that will generate toy data
class DummySource extends Receiver[Int](org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_2) {

  import scala.util.Random
  import org.apache.spark._
  import org.apache.spark.storage._
  import org.apache.spark.streaming._

  // Start the thread that receives data over a connection
  def onStart() {
    new Thread("Dummy Source") { override def run() { receive() } }.start()
  }

  def onStop() {  }

  // Periodically generate a random number from 0 to 9, and the timestamp
  private def receive() {
    while(!isStopped()) {
      store(Iterator(scala.util.Random.nextInt(10)))
      Thread.sleep(200)
    }
  }
}
