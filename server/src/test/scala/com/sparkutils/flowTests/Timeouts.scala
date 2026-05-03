package com.sparkutils.flowTests

import com.sparkutils.flow._
import com.sparkutils.flowTests.utils.SharedPureConnectTests
import com.sparkutils.quality._
import org.apache.spark.sql.{Column, DataFrame}
import org.scalatest.Matchers

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class Timeouts extends SharedPureConnectTests with Matchers with CustomExtensionsUtils {

  test("step timeouts should trigger with timedOut true") {
    def doTimeout(tolerant: Boolean = false) =  {
      val flow = buildFlow(CustomApproach(classOf[ISleep].getName), options = Map(stepTimeoutName -> "2s"))

      val s = sparkSession
      import s.implicits._

      val data = Seq(
        Tuple2("c", 1)
      ).toDF("c", "d")

      val ir = flow.run(s, _ => Some(data), tolerant = tolerant)
      ir
    }

    intercept[StepException[RuleSuite]](doTimeout()).timedOut shouldBe true

    doTimeout(true).stepResults.head._2.fold(_ => false)(_.timedOut) shouldBe true
  }

}

class ISleep() extends CustomResultApproach {

  override def apply[RP: RuleSuiteParam](input: DataFrame, function: Column, engineInputs: RunnerInputs, resultProcessInputs: ResultProcessInputs, step: Step[RP]): DataFrame = {
    Thread.sleep((4 seconds).toMillis)
    input
  }

}