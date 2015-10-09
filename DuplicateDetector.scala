import scala.collection.mutable.ListBuffer
import java.security.MessageDigest
import java.util.concurrent.ConcurrentLinkedQueue
import scala.io.Source
import scala.math.log

class DuplicateDetector(t : Double, k : Int = 3) {

  val BITS = 128 // Number of bits in hash
  val K    = k   // Shingle size

  val msgDigest = MessageDigest.getInstance("MD5")

  val nearDupThreshold = t // % threshold, above and its should be markt as near dup.


  // Counters
  var urlCount       = 0
  var exactDupCount  = 0
  var uniqueEngCount = 0
  var nearDupCount   = 0
  var studentCount   = 0


  // For sim-hash
  val ids          = new ListBuffer[String]()
  val fingerprints = new ListBuffer[String]()
  //val batch        = Collections.synchronizedList(new ListBuffer[String]())
  //val batch = new ListBuffer[String] with SynchronizedBuffer[String]
  val batch = new ConcurrentLinkedQueue[Page]();
  //val batch = new ConcurrentLinkedQueue<Page>();


  class Page(u : String, s : String, sf : Int) {
    val url = u
    val simhash = s
    val studentFreq = sf
  }


  def md5(s : String) : String = {
    // Converts byte array to binary string representation
    msgDigest.digest(s.getBytes).map( b => toBinaryString(b & 0xFF, 8)).mkString
  }

  def shingles(doc : String) : Set[String] = {
    doc.split("""\W""").sliding(K)
       .map(_.mkString)
       .map(_.toLowerCase)
       .toSet
  }

  def simhash(shingles : Set[String]) : String = {
    val hashes = shingles.map(s => md5(s))
    var G = new Array[Int](BITS)
    for(i <- 0 to BITS - 1; h <- hashes) {
      G(i) += 2*h.charAt(i).asDigit - 1
    }
    G.map(g => (1.0/2 * (Math.signum(g) + 1 )).toInt).mkString
  }

  // Converts an Int to a binary string
  def toBinaryString(value : Int, l : Int = BITS) : String = {
    String.format(s"%${l}s", Integer.toBinaryString(value)).replace(' ', '0')
  }

  def similarity(a : String, b: String) : Double = {
    var sim = 0
    for(i <- 0 to BITS - 1) {
      if(a.charAt(i) != b.charAt(i)) {
        sim += 1
      }
    }
    1.0 - sim*1.0 / BITS
  }

  // Read language distribution map from file
  def readNgrams(fileName : String) : Map[String, Double] = {
    val grams = Source.fromFile(fileName)
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

  def preprocess(doc : String, url : String) {

    // Student freq.
    val studentFreq =
      doc.split("\\W+")
      .filter(_.equalsIgnoreCase("student"))
      .length

    // Language.. TODO!

    // Simhash
    val sh = simhash(shingles(doc))
    batch.offer(new Page(url, sh, studentFreq))
  }

  def processBatch() {
    while(!batch.isEmpty()) {

      val b = batch.poll()

      // Only visits unique urls
      urlCount += 1

      // Increase student counter
      studentCount += b.studentFreq

      // Calculate similatiry score
      val (similarity, sim_id) = calcSimilatiryScore(b.simhash)

      // If exact duplicate increase counter and continue with next document
      if(similarity == 1.0) {
        exactDupCount += 1
      } else {

        // Language-detection-counting-things TODO!

        if(similarity >= nearDupThreshold) {
          nearDupCount += 1
        } else {
          ids          += b.url
          fingerprints += b.simhash
        }

      }

    }
  }

  def calcSimilatiryScore(sh : String) : (Double, Int) = {
    var sim = 0.0
    var cid = -1
    for(i <- 0 to fingerprints.size - 1) {
      val s = similarity(sh, fingerprints(i))
      if( s > sim ) {
        cid = i
        sim = s
      }
    }
    return (sim, cid)
  }

}
