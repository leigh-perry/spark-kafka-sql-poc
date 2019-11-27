package poc.spark.deltas

import org.specs2.Specification
import org.specs2.matcher.ThrownExpectations

class DeltasSpec extends Specification with ThrownExpectations {

  def is =
    s2"""
      Deltas
        ummmm $sampleTest
    """

  private def sampleTest =
    "Hello world" must haveSize(11)

}
