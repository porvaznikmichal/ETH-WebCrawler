import org.jsoup.Jsoup
import org.jsoup.Connection
import org.jsoup.nodes.Document;
import scala.collection.JavaConverters._
import scala.collection.parallel.mutable.ParTrieMap
import java.util.concurrent.atomic.AtomicInteger
import java.net.URL

object webcrawler {
  type Node = String

  var urlCount = new AtomicInteger(0)
  var studentCount = new AtomicInteger(0)
  val detector = new DuplicateDetector(0.9, 3)

  val open = ParTrieMap[Node, Boolean]()
  val closed = ParTrieMap[Node, Boolean]()


  def main(args : Array[String]) {
    val url : URL = new URL(args(0))
    val base : String = new URL(url, "./").toString()

    open(url.toString()) = true
    closed(url.toString()) = true

    // greedy bfs path search
    while (open.nonEmpty) {
      for ((node, _) <- open) {

        def expand(next: Node) {
          if (!closed.contains(next)) {
            open(next) = true
            closed(next) = true
          }
        }

        println(node)

        try {
          val doc = Jsoup.connect(node).get()

          val docString = doc.text().toString()

          detector.preprocess(docString, node)

          val elements =
            doc.select("a[href~=.html$").iterator.asScala

          val neighbours =
            elements.map(_.attr("abs:href"))
            .filter(_.startsWith(base))

          neighbours.foreach(expand(_))
        }
        catch {
          case e: Exception => println(e)
        }
        open.remove(node)
      }

      detector.processBatch()

    }

    println("Distinct URLs found: " + urlCount)
    println("Term frequency of \"student\": " + studentCount)

  }




  //
  // Distinct URLs found: 5292
  // Term frequency of "student": 14248
  //
  // Distinct URLs found: 10388
  // Term frequency of "student": 14248
}
