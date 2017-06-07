class GraphiteConnection(address: java.net.InetSocketAddress) extends java.io.Closeable {

  import java.util.regex.Pattern
  import java.nio.charset.Charset
  import java.io.BufferedWriter
  import java.io.OutputStreamWriter
  import javax.net.SocketFactory

  private final val WHITESPACE = Pattern.compile("[\\s]+")
  private final val charset: Charset = Charset.forName("UTF-8")

  private lazy val socket = {
    val s = SocketFactory.getDefault
      .createSocket(address.getAddress, address.getPort)
    s.setKeepAlive(true)
    s
  }

  private lazy val writer = new BufferedWriter(
    new OutputStreamWriter(socket.getOutputStream, charset)
  )

  // Send measurement carbon server in a thread-safe fashion
  def send(name: String, value: Double, timestamp: Long): Unit = {
    //val timestamp: Long = System.currentTimeMillis()/1000
    val sb = new StringBuilder()
      .append(sanitize(name)).append(' ')
      .append(sanitize(value.toString())).append(' ')
      .append(timestamp.toString).append('\n')

    // The write calls below handle the string in-one-go (locking);
    // Whereas the metrics' implementation of the graphite client uses multiple `write` calls,
    // which could become interwoven, thus producing a wrong metric-line, when called by multiple threads.
    writer.write(sb.toString())
    writer.flush()
  }

  // Closes underlying connection
  def close(): Unit = {
    try socket.close() finally writer.close()
  }

  protected def sanitize(s: String): String = {
    WHITESPACE.matcher(s).replaceAll("-")
  }
}
