import java.net.InetSocketAddress
import java.io.Closeable

class GraphiteConnection(address: InetSocketAddress) extends Closeable {

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

  // Send record to the carbon server in a thread-safe fashion
  def send(metric: String, value: Double, timestamp: Long): Unit = {
    val sb = new StringBuilder()
      .append(sanitize(metric)).append(' ')
      .append(sanitize(value.toString())).append(' ')
      .append(timestamp.toString).append('\n')

    writer.write(sb.toString())
    writer.flush()
  }

  // Close underlying connection
  def close(): Unit = {
    try socket.close() finally writer.close()
  }

  protected def sanitize(s: String): String = {
    WHITESPACE.matcher(s).replaceAll("-")
  }
}
