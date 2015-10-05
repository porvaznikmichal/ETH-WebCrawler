import scala.io.Source
import scala.collection.mutable.Queue
import scala.collection.mutable.Map
import java.net.URI
import java.net.URL
import java.nio.file.{Paths, Files}

val atag = """(?i)<a\s([^>]+)>""".r
val link = """\s*(?i)href\s*=\s*\"([^"]*)\"""".r
val lowest = "http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/"
val base : URL = new URL(lowest)


val q : Queue[URL] = Queue.empty[URL]
q += base
val visited : Map[String,Boolean] = Map.empty[String,Boolean]
visited += (base.toString -> true)
var count = 0

while (q.length != 0) {
    val url = q.dequeue()
    println(url.toString)
  try {


    if (url.toString.startsWith(lowest)) {
      var node = Source.fromURL(url).mkString
      count = count + 1

      val neighbours =
        atag.findAllMatchIn(node)
        .flatMap(m => link.findFirstMatchIn(m.group(1)))
        .map(_.group(1))
        .filter(!_.startsWith("tel"))
        .filter(!_.startsWith("http"))
        .filter(!_.contains("#"))
        .map(new URL(url,_))

      val newNodes =
        neighbours.filter {
          n => !visited.getOrElse(n.toString,false)
        }

      newNodes.foreach { n =>
        visited += (n.toString -> true);
        q       += n
      }
    } else println("SKIPPING: " + url)

  } catch {
    case e: java.io.IOException => "EXCEPTION!! " + println(e)
  }
}
println("Distinct URLs found: " + count)
// println(base)
// println(test2)
// hrefs.foreach(str => println(str))
// indomain.foreach(println)
