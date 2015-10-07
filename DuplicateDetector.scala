import scala.collection.mutable.ListBuffer
import java.security.MessageDigest

class DuplicateDetector(t : Double, k : Int = 3) {

  val BITS = 128 // Number of bits in hash
  val K    = k   // Shingle size

  val msgDigest = MessageDigest.getInstance("MD5")

  val threshold = t // % threshold, above and its should be markt as near dup.

  val ids          = new ListBuffer[String]()
  val fingerprints = new ListBuffer[String]()

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

  def detect(id : String, doc : String) : Boolean = {
    val sh = simhash(shingles(doc))

    var sim = 0.0
    var cid = 0
    for(i <- 0 to fingerprints.size - 1) {
      val s = similarity(sh, fingerprints(i))
      if( s > sim ) {
        cid = i
        sim = s
      }
    }

    //println(s"${sh} ~ ${sim}")

    if(sim >= threshold) {
      println(s"${id} similar to:\n${ids(cid)} with:\n${sh}\n${fingerprints(cid)}\n ~ ${sim}")
      return true
    } else {
      ids          += id
      fingerprints += sh
      return false
    }
  }
}
