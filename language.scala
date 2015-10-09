import scala.io.Source
import scala.math.log
//--------------------------------------------------------------------------------
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
//--------------------------------------------------------------------------------
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
//--------------------------------------------------------------------------------
// Classify a string given a list of languages
def classify(text: String, languages: List[(Map[String, Double], String)]) : String = {
	// clean text and extract grams
	val cleaned = text.replaceAll("[^a-zA-Züäö ]", "").toLowerCase
	val grams = (for (i <- 3 to 4) yield cleaned.sliding(i).toList).reduceLeft(_++_)
	val detect = languages.map(x => (logLikelihood(grams, x._1), x._2)).maxBy(_._1)._2
	return detect
}