import org.apache.spark.streaming.receiver._
import org.apache.spark.storage.StorageLevel._
import scala.util.Random

// Dummy receiver that will provide toy data
class DummySource(ratePerSec: Int) extends Receiver[Double](MEMORY_AND_DISK_2) {

  // Start the thread that receives data over a connection
  def onStart() {
    new Thread("Dummy Source") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {  }

  // Create a socket and receive data until receiver is stopped
  def receive() {
    val gen = new Generator0
    while(!isStopped()) {
      store(gen.next())
      Thread.sleep((1000.toDouble/ratePerSec).toInt)
    }
  }
}

// Dummy data generator #0
class Generator0 extends Iterator[Double] {
  def hasNext(): Boolean = {
    // nothing fancy here: the generator is meant to run indefinitely
    true
  }
  def next(): Double = {
    val drift: Long = 400L
    val currentTime: Long = System.currentTimeMillis()/1000
    ((currentTime/drift) % 3) match { // no need for floor, since both are Long
      case 0 => 10.0 + scala.util.Random.nextGaussian()
      case 1 => 5.0 + scala.util.Random.nextGaussian()
      case 2 => -2.0 + scala.util.Random.nextGaussian()
    }
  }
}

// Dummy data generator #1
class Generator1 extends Iterator[Double] {
  def hasNext(): Boolean = {
    // nothing fancy here: the generator is meant to run indefinitely
    true
  }
  def sineWave(frequency: Double, amplitude: Double, time : Long): Double = {
    val value = amplitude * math.sin(2.0 * math.Pi * frequency * time.toDouble)
    value
  }
  def next(): Double = {
    val currentTime: Long = System.currentTimeMillis()/1000
    val isSpike: Boolean = (Random.nextInt(30) == 0) // arbitrarily chose 0. Spike chance = 1/30.
    val frequency: Double = 0.01
    val amplitude: Double = 10.0
    val noiseLevel: Double = 40.0
    if (isSpike) {
      sineWave(frequency, amplitude, currentTime) + noiseLevel * (1.0 + Random.nextGaussian())
    } else {
      sineWave(frequency, amplitude, currentTime)
    }
  }
}
