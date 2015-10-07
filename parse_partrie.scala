import org.jsoup.Jsoup
import org.jsoup.Connection
import org.jsoup.nodes.Document;
import scala.collection.JavaConverters._
import scala.collection.parallel.mutable.ParTrieMap
import java.util.concurrent.atomic.AtomicInteger

type Node = String
type Parent = String

val base = "http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/"

var urlCount = new AtomicInteger(0)
var studentCount = new AtomicInteger(0)


val open = ParTrieMap[Node, Parent]()
val closed = ParTrieMap[Node, Parent]()

open(base + "de.html") = null

// greedy bfs path search (magic)
while (open.nonEmpty) {
  for ((node, parent) <- open) {

    def expand(next: Node) {
      if (!closed.contains(next)) {
        open(next) = node
      }
    }

    println(node)
    try {
      val doc = Jsoup.connect(node).get()
      val docString = doc.text().toString()
      val elements =
        doc.select("a[href~=.html$").iterator.asScala
      val studentFreq =
        docString.split("\\W+")
        .filter(_.equalsIgnoreCase("student"))
        .length

      studentCount.getAndAdd(studentFreq)
      urlCount.getAndIncrement()

      val neighbours =
        elements.map(_.attr("abs:href"))
        .filter(_.startsWith(base))

      neighbours.foreach(expand(_))
    }
    catch {
      case e: Exception => println(e)
    }
    closed(node) = parent
    open.remove(node)
  }
}


println("Distinct URLs found: " + urlCount)
println("Term frequency of \"student\": " + studentCount)
//
// Distinct URLs found: 5292
// Term frequency of "student": 14248
//
// Distinct URLs found: 10388
// Term frequency of "student": 14248
