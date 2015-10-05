import scala.collection.mutable.{Map => MutMap}
import scala.io.Source
import java.io._
import scala.math.log

object ex2 {

  def normalize(raw: String) : String  =
    raw.replaceAll("[^a-zA-Züäö ]","")
    .toLowerCase

  def ngramsFromText(fileName : String, n : Int) : Map[String,Int] =
    Source.fromFile(fileName)
    .getLines
    .map(normalize)
    .flatMap(_.sliding(n))
    .foldLeft(MutMap.empty[String,Int]){
      (result, ngram) => result += (ngram -> (result.getOrElse(ngram, 0) + 1))
    }.toMap

  def ngramsStored(fileName : String, sep : util.matching.Regex) : Map[String, Int] =
    Source.fromFile(fileName)
    .getLines
    .foldLeft(MutMap.empty[String,Int]) {
      (result, line) =>
        line match {
          case sep(ngram,v) => result += (ngram -> v.toInt)
        }
    }.toMap

  def storeNgrams(fileName : String, ngrams : Map[String, Int]) : Unit = {
    val out = new PrintWriter(new File(fileName))
    ngrams.foreach{case (k,v) => out.write(k + '\t' + v + '\n')}
    out.close()
  }

  def pLang(input : String, ngrams : Map[String, Int], tLang : Int, n : Int) : Double =
    input.sliding(n).foldLeft(0.0) {
      (result, ngram) => result + log(ngrams.getOrElse(ngram,1).toDouble/tLang)
    }

  def main(args : Array[String]) {

    // var enGrams : Map[String,Int] =
    //   ngramsFromText("en-de/europarl-v6.de-en.en", 3)
    // storeNgrams("en-grams.txt", enGrams)

    // var deGrams : Map[String, Int] =
    //   ngramsFromText("en-de/europarl-v6.de-en.de", 3)
    // storeNgrams("de-grams.txt", deGrams)

    val tabSep = """(.*)\t(.*)""".r
    val enGrams : Map[String,Int] = ngramsStored("en-grams.txt", tabSep)
    val deGrams : Map[String,Int] = ngramsStored("de-grams.txt", tabSep)

    val tEn : Int = enGrams.foldLeft(0){(result,entry) => result + entry._2}
    val tDe : Int = deGrams.foldLeft(0){(result,entry) => result + entry._2}

    val input = "Hola que tal"

    val pEn : Double = pLang(normalize(input), enGrams, tEn, 3)
    val pDe : Double = pLang(normalize(input), deGrams, tDe, 3)


    if (pEn > pDe) println("English") else println("German")

  }
}
