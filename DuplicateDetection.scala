import io.Source
import scala.util.Random
import scala.util.Sorting
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import java.security.MessageDigest
import java.nio.ByteBuffer

// Takes two files as inputs, and calculates Jaquard Coefficient by shingeling
object Shingles {
  def main(args: Array[String]) {
    val docs = List(Source.fromFile("samples/01.html").mkString,
                    Source.fromFile("samples/02.html").mkString,
                    Source.fromFile("samples/03.html").mkString,
                    Source.fromFile("samples/01.html").mkString)


    val sh = new DuplicateDetector(0.9) // 3-shingles and 2-permutations
    for(doc <- docs) {
      sh.test(doc)
    }
  }

  class DuplicateDetector(t : Double) {

    val BITS = 32 // Number of bits in hash
    val K    = 3   // Shingle size

    val threshold = t // % threshold, above and its should be markt as near dup.

    val fingerprints = new ListBuffer[Int]()

    def md5(s : String) : Int = {
      /*
      val bytes = MessageDigest.getInstance("MD5").digest(s.getBytes)

      // Converts bytes array to int
      ByteBuffer.wrap(bytes).getInt
      */
      s.hashCode
    }

    def shingles(doc : String) : Set[String] = {
      doc.split("""\W""").sliding(K)
         .map(_.mkString)
         .map(_.toLowerCase)
         .toSet
    }

    def simhash(shingles : Set[String]) : Int =   {
      val hashes = shingles.map(s => toBinaryString(md5(s))).toArray
      var G = new Array[Int](BITS)
      // TODO: i starts with at 1 so that the binary number begins with a 0,
      // else the parseInt functions gets a numberformatexception...
      for(i <- 1 to BITS - 1; h <- hashes) {
        G(i) += 2*h.charAt(i).asDigit - 1
      }
      Integer.parseInt(G.map(g => (1.0/2 * (Math.signum(g) + 1 )).toInt).mkString, 2)
    }

    // Converts an Int to a binary string
    def toBinaryString(value : Int) : String = {
      val str1 = Integer.toBinaryString(value)
      val str2 = String.format(s"%${BITS}s",str1)
      str2.replace(' ','0')
      //String.format(s"%${BITS}s", Integer.toBinaryString(value)).replace(' ', '0')
    }

    def similarity(a : Int, b: Int) : Double = {
      // Counts number of 1's in XOR:erd Int (^ = XOR)
      1.0 - Integer.bitCount(a ^ b)*1.0 / BITS
    }

    def test(doc : String) {
      val sh = simhash(shingles(doc))

      println(s"Query:\n${toBinaryString(sh)} threshold: ${threshold}\nResults:")

      var isDuplicate = false
      for(f <- fingerprints) {
        val sim = similarity(sh, f)
        if(sim >= threshold ) {
          isDuplicate = true
        }
        println(s"${toBinaryString(f)} => ${sim}: ${isDuplicate}")
      }

      if(isDuplicate == false) {
        fingerprints += sh
      }
    }
  }

}
