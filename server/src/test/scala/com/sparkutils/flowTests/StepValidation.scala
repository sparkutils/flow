package com.sparkutils.flowTests

import com.sparkutils.flow.impl.util.FlowExceptionConstants.{CycleDetected, DefaultViewNamesMultipleParents, DuplicateNames, EmptyFlow, EmptyStepName, InvalidDQResultApproach, InvalidViewNames, MissingStep}
import com.sparkutils.flow.{AsIs, Flow, FlowDataHandling, FlowException, MergeFields, Operation, OutputFieldOnly, OutputFieldsOnly, ResultApproach, StarOnly, Step, StepData, fromDatasets, toDatasets}
import com.sparkutils.flowTests.RulesGen.{rulesRaw, testData}
import com.sparkutils.flowTests.utils.SharedPureConnectTests
import com.sparkutils.quality.{DataFrameLoader, DefaultProcessor, ExpressionRule, Id, LambdaFunction, OutputExpression, Rule, RuleSet, RuleSuite, RuleSuiteGroupResults, RunOnPassProcessor, ViewRow, registerLambdaFunctions}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.Matchers

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

class StepValidation extends SharedPureConnectTests with Matchers {

  test("Empty steps should throw") {
    val r = intercept[FlowException]{
      val flow =
        new Flow(Id(1,1), Seq(
        ))
      doSimpleEngine(flow)
    }
    r.msg shouldBe EmptyFlow
  }

  test("Duplicate step names should throw") {
    val r = intercept[FlowException]{
      val flow =
        new Flow(Id(1,1), Seq(
          Step("a",Set.empty, Operation(rulesRaw(Seq(
            )), "engine", "view1E", AsIs),
            data = StepData("view1", "view2")),
          Step("b",Set("a"), Operation(rulesRaw(Seq(
            )).copy(id = Id(2,1)), "engine", "view2E", AsIs),
            data = StepData("view2", "view3")),
          Step("a",Set.empty, Operation(rulesRaw(Seq(
            )).copy(id = Id(3,1)), "engine", "view3E", StarOnly),
            data = StepData("view3", "view4")),
          Step("d",Set("c", "b"), Operation(rulesRaw(Seq(
            )).copy(id = Id(4,1)), "engine", "view4E", OutputFieldOnly),
            data = StepData("filteredView4", "view5")),
          Step("c",Set("c", "b"), Operation(rulesRaw(Seq(
            )).copy(id = Id(4,1)), "engine", "view4E", OutputFieldOnly),
            data = StepData("filteredView4", "view5")),
          Step("d",Set("c", "b"), Operation(rulesRaw(Seq(
            )).copy(id = Id(4,1)), "engine", "view4E", OutputFieldOnly),
            data = StepData("filteredView4", "view5"))
        ))
      doSimpleEngine(flow)
    }
    r.msg shouldBe s"$DuplicateNames (a,d)"
  }

  test("Missing steps should throw") {
    val r = intercept[FlowException]{
      val flow =
        new Flow(Id(1,1), Seq(
          Step("a",Set.empty, Operation(rulesRaw(Seq()), "engine", "view1E", AsIs),
            data = StepData("view1", "view2")),
          Step("b",Set("a"), Operation(rulesRaw(Seq( )).copy(id = Id(2,1)), "engine", "view2E", AsIs),
            data = StepData("view2", "view3")),
          Step("d",Set("c", "b"), Operation(rulesRaw(Seq()).copy(id = Id(4,1)), "engine", "view4E", OutputFieldOnly),
            data = StepData("filteredView4",  "view5"))
        ))
      doSimpleEngine(flow)
    }
    r.msg shouldBe MissingStep("d","c")
  }

  test("Empty step names should throw") {
    def test(name: String) = {
      val r = intercept[FlowException] {
        val flow =
          new Flow(Id(1, 1), Seq(
            Step("a", Set.empty, Operation(rulesRaw(Seq()), "engine", "view1E", AsIs),
              data = StepData("view1", "view2")),
            Step(name, Set("a"), Operation(rulesRaw(Seq(
              )).copy(id = Id(2, 1)), "engine", "view2E", AsIs),
              data = StepData( "view2", "view3")),
            Step("d", Set("c", "b"), Operation(rulesRaw(Seq(
              )).copy(id = Id(4, 1)), "engine", "view4E", OutputFieldOnly),
              data = StepData("filteredView4", "view5"))
          ))
        doSimpleEngine(flow)
      }
      r.msg shouldBe EmptyStepName
    }
    test("")
    test(null)
  }

  def testNames(name: String, parents: Set[String] = Set("c"), err: Step => String = InvalidViewNames) = {
    val d = Step("d", parents,
      Operation(rulesRaw(Seq()).copy(id = Id(4, 1)), "engine", "view3", OutputFieldOnly),
      data = StepData(name, "view5")
    )

    val r = intercept[FlowException] {
      val flow =
        new Flow(Id(1, 1), Seq(
          Step("a", Set.empty, Operation(rulesRaw(Seq()), "engine", "view1E", AsIs),
            data = StepData("view1", "view2")),
          Step("b", Set("a"), Operation(rulesRaw(Seq()).copy(id = Id(2, 1)), "engine", "view2E", AsIs),
            data = StepData("view2", "view3")),
          d
        ))
      doSimpleEngine(flow)
    }
    r.msg shouldBe err(d)
  }

  test("Missing step view names should throw") {
    testNames("")
    testNames(null)
  }

  test("Missing step view names with multiple parents should throw") {
    testNames("", Set("c", "b"), DefaultViewNamesMultipleParents)
    testNames(null, Set("c", "b"), DefaultViewNamesMultipleParents)
  }

  test("Cycles should throw") {
    val r = intercept[FlowException]{
      val flow =
        new Flow(Id(1,1), Seq(
          Step("a",Set.empty, Operation(rulesRaw(Seq()), "engine", "view1E", AsIs),
            data = StepData("view1", "view2")),
          Step("b",Set("a","c"), Operation(rulesRaw(Seq()).copy(id = Id(2,1)), "engine", "view2E", AsIs),
            data = StepData("view2", "view3")),
          Step("c",Set("c", "b"), Operation(rulesRaw(Seq()).copy(id = Id(4,1)), "engine", "view4E", OutputFieldOnly),
            data = StepData("filteredView4", "view5")),
          Step("d",Set("c", "b"), Operation(rulesRaw(Seq()).copy(id = Id(4,1)), "engine", "view4E", OutputFieldOnly),
            data = StepData("filteredView4", "view5"))
        ))
      doSimpleEngine(flow)
    }
    r.msg should include(CycleDetected)
  }

  test("dq with certain processing combinations should fail") { // correct dq is in the "dq mixes in..." test
    def withOutput(resultApproach: ResultApproach) = {
      val step = Step("d", Set("c", "b"),
        Operation(rulesRaw(Seq()).copy(id = Id(4, 1)), "dq", "view4E", resultApproach),
        data = StepData("filteredView4", "view5"))
      val r = intercept[FlowException] {
        val flow =
          new Flow(Id(1, 1), Seq(
            Step("a", Set.empty, Operation(rulesRaw(Seq()), "engine", "view1E", AsIs),
              data = StepData("view1", "view2")),
            Step("b", Set("a"), Operation(rulesRaw(Seq()).copy(id = Id(2, 1)), "engine", "view2E", AsIs),
              data = StepData("view2", "view3")),
            Step("c", Set(), Operation(rulesRaw(Seq()).copy(id = Id(4, 1)), "engine", "view4E", OutputFieldOnly),
              data = StepData("filteredView4", "view5")),
            step
          ))
        doSimpleEngine(flow)
      }
      (r,step)
    }

    val (a1,a2) = withOutput(OutputFieldsOnly)
    a1.msg shouldBe InvalidDQResultApproach(a2)
    val (b1,b2) = withOutput(MergeFields)
    b1.msg shouldBe InvalidDQResultApproach(b2)

  }

  def doSimpleEngine(flow: Flow, sessionOnly: Boolean = false): Unit = {
    val s = sparkSession
    import s.implicits._
    val ires = if (sessionOnly) flow.run(sparkSession) else flow.run(sparkSession, _ => Some(testData.toDF()))
  }
}
