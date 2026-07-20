package org.example.Util

case class Path(
               countryFilePath: String = "/Users/soumitrabanerjee/Desktop/SparkLearnings/data/csv/country_wise_latest.csv",
               dataManipulationFilePath: String = "/Users/soumitrabanerjee/Desktop/SparkLearnings/data/csv/country_wise_latest.csv"
               )
class Util extends Serializable {
  def calculateCountryAtRisk(Deaths: Int): String={
    if(Deaths > 1000){"YES"} else {"NO"}
  }

  var count: Int = 0
  val counter = () => count+=1

}