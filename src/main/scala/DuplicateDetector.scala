import scala.collection.mutable.ListBuffer
import java.security.MessageDigest
import java.util.concurrent.ConcurrentLinkedQueue
import scala.io.Source
import scala.math.log
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;


class DuplicateDetector(t : Double, k : Int = 3) {

  val BITS = 128 // Number of bits in hash
  val K    = k   // Shingle size

  //val msgDigest = MessageDigest.getInstance("MD5")

  val nearDupThreshold = t // % threshold, above and its should be markt as near dup.

  // Counters
  var urlCount       = 0
  var exactDupCount  = 0
  var uniqueEngCount = 0
  var nearDupCount   = 0
  var studentCount   = 0

  // read in the language maps
  val languages = List((readNgrams("english.txt"), "english"),
                       (readNgrams("german.txt"), "german"))

  // For sim-hash
  val history = new ListBuffer[Page]()
  val batch        = new ConcurrentLinkedQueue[Page]();


  class Page(u : String, s : Array[Byte], sf : Int, l: String) {
    val url = u
    val simhash = s
    val studentFreq = sf
    val language = l
  }

  // Generate md5-hash of string
  def md5(s : String) : Array[Byte] = {
      MessageDigest.getInstance("MD5").digest(s.getBytes)
  }

  def shingles(doc : String) : Set[String] = {
    doc.split("""\W""").sliding(K)
       .map(_.mkString)
       .map(_.toLowerCase)
       .toSet
  }

  def simhash(shingles : Set[String]) : Array[Byte] = {
    try {
      val hashes = shingles.map(s => md5(s))
      var G = new Array[Int](BITS)
      for(i <- 0 to BITS - 1; h <- hashes) {
        G(i) += 2*getBit(h,i) - 1
      }

      // List of 0's and 1's, converted to Array[Byte]
      return G.map(g => (1.0/2 * (Math.signum(g) + 1 )).toByte).
          grouped(8).toArray.map(l => Integer.parseInt(l.mkString,2).toByte)
    } catch {
      case e: Exception => throw new Exception(s"simhash: ${e}")
    }
  }

  // Returns bit (0 or 1) at position p (starting at 0) from array byte ab
  def getBit(ab: Array[Byte], p: Int) : Int = {
    (ab(p / 8) >> p % 8) & 1
  }

  // Converts an Int to a binary string
  def toBinaryString(value : Int, l : Int = BITS) : String = {
    String.format(s"%${l}s", Integer.toBinaryString(value)).replace(' ', '0')
  }

  def similarity(a : Array[Byte], b: Array[Byte]) : Double = {
    try {
      val diffbytes = a.zip(b).map{case(x,y) => (x ^ y).toByte}
      var diff = 0
      for(j <- 0 to BITS - 1) {
        diff += getBit(diffbytes, j)
      }
      return 1.0 - diff*1.0 / BITS
    } catch {
      case e: Exception => throw new Exception(s"similarity: ${e}")
    }
  }

  // Read language distribution map from file
  def readNgrams(fileName : String) : Map[String, Double] = {
    val grams = Source.fromInputStream(getClass().getClassLoader().getResourceAsStream(fileName))
          .getLines
          .map(_.split("\t"))
          .map(x => x(0) -> x(1).toInt).toList
    val gramMap = Map(grams: _*)
    val total = gramMap.values.sum
    val distribution : Map[String, Double] = gramMap.map( x => (x._1, (x._2.toDouble) / total))
    return distribution
  }

  // Calculate the log likelihood of seeing a list of grams given a language
  def logLikelihood(grams: List[String], language: Map[String, Double]) : Double = {
    // define matching functon for looking up gram probabilities
    def gramProb(x: String, m: Map[String,Double]): Option[Double] = m.get(x) match {
      case None => Option(1E-320)
      case _ => m.get(x)
    }
    // calculate likelihood
    val logL = grams.flatMap(gramProb(_, language)).map(log).sum
    return logL
  }

  // Classify a string given a list of languages
  def classify(text: String, languages: List[(Map[String, Double], String)]) : String = {
    // clean text and extract grams
    val cleaned = text.replaceAll("[^a-zA-Züäö ]", "").toLowerCase
    val grams = (for (i <- 3 to 4) yield cleaned.sliding(i).toList).reduceLeft(_++_)
    val detect = languages.map(x => (logLikelihood(grams, x._1), x._2)).maxBy(_._1)._2
    return detect
  }

  def preprocess(doc : Document, url : String) {
    // get full text
    val fullText = doc.text().toString()
    if (fullText == "") return
    // Student freq.
    val studentFreq =
      fullText.split("\\W+")
      .filter(_.equalsIgnoreCase("student"))
      .length

    // get paragraph text and chcek if empty
    val parText = doc.select("p, h1, h2, h3").text().toString()

    var text = ""
    if (parText.size == 0) {
      text = fullText
    }
    else text = parText

    // Language detection
    val detectedLanguage = classify(fullText, languages)

    // Simhash
    val sh = simhash(shingles(text))
    batch.offer(new Page(url, sh, studentFreq, detectedLanguage))
  }

  // Exact duplicate verification
  def exactVerification(page1: Page, page2: Page) : Boolean = {
    try {
      val doc1 = Jsoup.connect(page1.url).get().body().text().toString()
      val doc2 = Jsoup.connect(page2.url).get().body().text().toString()
      doc1 == doc2
    }
    catch {
      case e: org.jsoup.HttpStatusException => {println(e); return false}
      case e: java.net.SocketTimeoutException => {println(e); return false}
    }
  }

  def processBatch() {
    while(!batch.isEmpty()) {

      val b = batch.poll()

      // Only visits unique urls
      urlCount += 1


      // Calculate similatiry score
      val (similarity, sim_page) = calcSimilatiryScore(b.simhash)

      // if not a near duplicate, increment student count and detect language
      if (similarity < nearDupThreshold) {
        // Increase student counter
        studentCount += b.studentFreq
        
        // Increment number of english language pages
        if (b.language == "english") uniqueEngCount += 1
      }

      // If similarity is 1.0, verify if if pages are exact duplicates
      if(similarity == 1.0 && exactVerification(b, history(sim_page))) {
        // println(s"Exact duplicate:\n${b.url}\n${history(sim_page).url}")
        exactDupCount += 1
      } else {
        // Add all pages that are not exact duplicates
        history += b


        if(similarity >= nearDupThreshold) {
          // println(s"Near duplicate: ~${similarity}\n${b.url}\n${history(sim_page).url}")
          nearDupCount += 1
        }
      }

    }
  }

  def calcSimilatiryScore(sh : Array[Byte]) : (Double, Int) = {
    try {
      var sim = 0.0
      var cid = -1
      for(i <- 0 to history.size - 1) {
        val s = similarity(sh, history(i).simhash)
        if( s > sim ) {
          cid = i
          sim = s
        }
      }
      return (sim, cid)
    } catch {
      case e: Exception => throw new Exception(s"calcSim: ${e}")
    }
  }

}
