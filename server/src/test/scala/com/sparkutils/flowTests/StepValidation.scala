package com.sparkutils.flowTests

import com.sparkutils.flow.impl.util.FlowExceptionConstants.{CycleDetected, DefaultViewNamesMultipleParents, DuplicateNames, EmptyFlow, EmptyStepName, InvalidDQResultApproach, InvalidViewNames, MissingStep}
import com.sparkutils.flow.{AsIs, DQ, Engine, Flow, FlowDataHandling, FlowException, MergeFields, Operation, OutputFieldOnly, OutputFieldsOnly, ResultApproach, StarOnly, Step, StepData, fromDatasets, toDatasets}
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
            )), Engine, AsIs, "view1E"),
            data = StepData("view1", "view2")),
          Step("b",Set("a"), Operation(rulesRaw(Seq(
            )).copy(id = Id(2,1)), Engine, AsIs, "view2E"),
            data = StepData("view2", "view3")),
          Step("a",Set.empty, Operation(rulesRaw(Seq(
            )).copy(id = Id(3,1)), Engine, StarOnly, "view3E"),
            data = StepData("view3", "view4")),
          Step("d",Set("c", "b"), Operation(rulesRaw(Seq(
            )).copy(id = Id(4,1)), Engine, OutputFieldOnly, "view4E"),
            data = StepData("filteredView4", "view5")),
          Step("c",Set("c", "b"), Operation(rulesRaw(Seq(
            )).copy(id = Id(4,1)), Engine, OutputFieldOnly, "view4E"),
            data = StepData("filteredView4", "view5")),
          Step("d",Set("c", "b"), Operation(rulesRaw(Seq(
            )).copy(id = Id(4,1)), Engine, OutputFieldOnly, "view4E"),
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
          Step("a",Set.empty, Operation(rulesRaw(Seq()), Engine, AsIs, "view1E"),
            data = StepData("view1", "view2")),
          Step("b",Set("a"), Operation(rulesRaw(Seq( )).copy(id = Id(2,1)), Engine, AsIs, "view2E"),
            data = StepData("view2", "view3")),
          Step("d",Set("c", "b"), Operation(rulesRaw(Seq()).copy(id = Id(4,1)), Engine, OutputFieldOnly, "view4E"),
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
            Step("a", Set.empty, Operation(rulesRaw(Seq()), Engine, AsIs, "view1E"),
              data = StepData("view1", "view2")),
            Step(name, Set("a"), Operation(rulesRaw(Seq(
              )).copy(id = Id(2, 1)), Engine, AsIs, "view2E"),
              data = StepData( "view2", "view3")),
            Step("d", Set("c", "b"), Operation(rulesRaw(Seq(
              )).copy(id = Id(4, 1)), Engine, OutputFieldOnly, "view4E"),
              data = StepData("filteredView4", "view5"))
          ))
        doSimpleEngine(flow)
      }
      r.msg shouldBe EmptyStepName
    }
    test("")
    test(null)
  }

  def doTestNames(name: String, err: Step[RuleSuite] => String, parents: Set[String] = Set("b")) = {
    val d = Step("d", parents,
      Operation[RuleSuite](rulesRaw(Seq()).copy(id = Id(4, 1)), Engine, OutputFieldOnly, "view3"),
      data = StepData(name, "view5")
    )

    val r = intercept[FlowException] {
      val flow =
        new Flow(Id(1, 1), Seq(
          Step("a", Set.empty, Operation(rulesRaw(Seq()), Engine, AsIs, "view1E"),
            data = StepData("view1", "view2")),
          Step("b", Set("a"), Operation(rulesRaw(Seq()).copy(id = Id(2, 1)), Engine, AsIs, "view2E"),
            data = StepData("view2", "view3")),
          d
        ))
      doSimpleEngine(flow)
    }
    r.msg shouldBe err(d)
  }

  test("Missing step view names should throw") {
    doTestNames("", err = InvalidViewNames)
  }

  test("Missing step view names with multiple parents should throw") {
    doTestNames("", DefaultViewNamesMultipleParents, Set("c", "b"))
    doTestNames(null, DefaultViewNamesMultipleParents, Set("c", "b"))
  }

  test("Cycles should throw") {
    val r = intercept[FlowException]{
      val flow =
        new Flow(Id(1,1), Seq(
          Step("a",Set.empty, Operation(rulesRaw(Seq()), Engine, AsIs, "view1E"),
            data = StepData("view1", "view2")),
          Step("b",Set("a","c"), Operation(rulesRaw(Seq()).copy(id = Id(2,1)), Engine, AsIs, "view2E"),
            data = StepData("view2", "view3")),
          Step("c",Set("c", "b"), Operation(rulesRaw(Seq()).copy(id = Id(4,1)), Engine, OutputFieldOnly, "view4E"),
            data = StepData("filteredView4", "view5")),
          Step("d",Set("c", "b"), Operation(rulesRaw(Seq()).copy(id = Id(4,1)), Engine, OutputFieldOnly, "view4E"),
            data = StepData("filteredView4", "view5"))
        ))
      doSimpleEngine(flow)
    }
    r.msg should include(CycleDetected)
  }

  test("dq with certain processing combinations should fail") { // correct dq is in the "dq mixes in..." test
    def withOutput(resultApproach: ResultApproach) = {
      val step = Step("d", Set("c", "b"),
        Operation[RuleSuite](rulesRaw(Seq()).copy(id = Id(4, 1)), DQ, resultApproach, "view4E"),
        data = StepData("filteredView4", "view5"))
      val r = intercept[FlowException] {
        val flow =
          new Flow(Id(1, 1), Seq(
            Step("a", Set.empty, Operation(rulesRaw(Seq()), Engine, AsIs, "view1E"),
              data = StepData("view1", "view2")),
            Step("b", Set("a"), Operation(rulesRaw(Seq()).copy(id = Id(2, 1)), Engine, AsIs, "view2E"),
              data = StepData("view2", "view3")),
            Step("c", Set(), Operation(rulesRaw(Seq()).copy(id = Id(4, 1)), Engine, OutputFieldOnly, "view4E"),
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
