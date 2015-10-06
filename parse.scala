import scala.io.Source
import scala.collection.mutable.Queue
import scala.collection.mutable.HashSet
import org.jsoup.Jsoup;
import org.jsoup.Connection;
import org.jsoup.nodes.Document;
import scala.collection.JavaConverters._

val base = "http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/"

val q : Queue[String] = Queue[String](base + "de.html")
val visited : HashSet[String] = HashSet[String](base)
var count = 0
var studentCount = 0

while (q.length != 0) {
  try {
    val url = q.dequeue()
    println(url.toString)
    val doc = Jsoup.connect(url).get()

    // Get the content from document
    val docString = doc.text().toString()

    val studentFreq =
      docString.split("\\W+")
      .filter(_.equalsIgnoreCase("student"))
      .length

    studentCount += studentFreq

    // Add this string to something that handles
    // 1. Language detection
    // 2. Duplicate detection

    // Follow only .html for now
    val elements =
      doc.select("a[href~=.html$").iterator.asScala

    val neighbours =
      elements.map(_.attr("abs:href"))
      .filter(_.startsWith(base))
      .filter(!visited.contains(_))

    count = count + 1

    neighbours.foreach { n =>
      visited += n;
      q       += n;
    }

  } catch {
    case e: Exception => println(e)
  }
}

println("Distinct URLs found: " + count)
println("Term frequency of \"student\": " + studentCount)
