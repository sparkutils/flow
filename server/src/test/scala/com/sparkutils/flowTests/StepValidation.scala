package com.sparkutils.flowTests

import com.sparkutils.flow.impl.util.FlowExceptionConstants.{CycleDetected, DuplicateNames, EmptyFlow, EmptyStepName, InvalidDQResultApproach, InvalidViewNames, MissingStep}
import com.sparkutils.flow.{AsIs, Flow, FlowDataHandling, FlowException, MergeFields, Operation, OutputFieldOnly, OutputFieldsOnly, ResultApproach, StarOnly, Step, fromDatasets, toDatasets}
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
          Step("a",Set.empty,rulesRaw(Seq(
          )), "view1", Seq.empty, Operation("engine", "view1E", AsIs), Map.empty, "view2"),
          Step("b",Set("a"),rulesRaw(Seq(
          )).copy(id = Id(2,1)), "view2", Seq.empty, Operation("engine", "view2E", AsIs), Map.empty, "view3"),
          Step("a",Set.empty,rulesRaw(Seq(
          )).copy(id = Id(3,1)), "view3", Seq.empty, Operation("engine", "view3E", StarOnly), Map.empty, "view4"),
          Step("d",Set("c", "b"),rulesRaw(Seq(
          )).copy(id = Id(4,1)), "filteredView4", Seq(),
            Operation("engine", "view4E", OutputFieldOnly), Map.empty, "view5"),
          Step("c",Set("c", "b"),rulesRaw(Seq(
          )).copy(id = Id(4,1)), "filteredView4", Seq(),
            Operation("engine", "view4E", OutputFieldOnly), Map.empty, "view5"),
          Step("d",Set("c", "b"),rulesRaw(Seq(
          )).copy(id = Id(4,1)), "filteredView4", Seq(),
            Operation("engine", "view4E", OutputFieldOnly), Map.empty, "view5")
        ))
      doSimpleEngine(flow)
    }
    r.msg shouldBe s"$DuplicateNames (a,d)"
  }

  test("Missing steps should throw") {
    val r = intercept[FlowException]{
      val flow =
        new Flow(Id(1,1), Seq(
          Step("a",Set.empty,rulesRaw(Seq(
          )), "view1", Seq.empty, Operation("engine", "view1E", AsIs), Map.empty, "view2"),
          Step("b",Set("a"),rulesRaw(Seq(
          )).copy(id = Id(2,1)), "view2", Seq.empty, Operation("engine", "view2E", AsIs), Map.empty, "view3"),
          Step("d",Set("c", "b"),rulesRaw(Seq(
          )).copy(id = Id(4,1)), "filteredView4", Seq(),
            Operation("engine", "view4E", OutputFieldOnly), Map.empty, "view5")
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
            Step("a", Set.empty, rulesRaw(Seq(
            )), "view1", Seq.empty, Operation("engine", "view1E", AsIs), Map.empty, "view2"),
            Step(name, Set("a"), rulesRaw(Seq(
            )).copy(id = Id(2, 1)), "view2", Seq.empty, Operation("engine", "view2E", AsIs), Map.empty, "view3"),
            Step("d", Set("c", "b"), rulesRaw(Seq(
            )).copy(id = Id(4, 1)), "filteredView4", Seq(),
              Operation("engine", "view4E", OutputFieldOnly), Map.empty, "view5")
          ))
        doSimpleEngine(flow)
      }
      r.msg shouldBe EmptyStepName
    }
    test("")
    test(null)
  }

  test("Missing step view names should throw") {
    def test(name: String) = {
      val d = Step("d", Set("c", "b"), rulesRaw(Seq(
      )).copy(id = Id(4, 1)), name, Seq(),
        Operation("engine", "view3", OutputFieldOnly), Map.empty, "view5"
      )

      val r = intercept[FlowException] {
        val flow =
          new Flow(Id(1, 1), Seq(
            Step("a", Set.empty, rulesRaw(Seq(
            )), "view1", Seq.empty, Operation("engine", "view1E", AsIs), Map.empty, "view2"),
            Step("b", Set("a"), rulesRaw(Seq(
            )).copy(id = Id(2, 1)), "view2", Seq.empty, Operation("engine", "view2E", AsIs), Map.empty, "view3"),
            d
          ))
        doSimpleEngine(flow)
      }
      r.msg shouldBe InvalidViewNames(d)
    }
    test("")
    test(null)
  }

  test("Cycles should throw") {
    val r = intercept[FlowException]{
      val flow =
        new Flow(Id(1,1), Seq(
          Step("a",Set.empty,rulesRaw(Seq(
          )), "view1", Seq.empty, Operation("engine", "view1E", AsIs), Map.empty, "view2"),
          Step("b",Set("a","c"),rulesRaw(Seq(
          )).copy(id = Id(2,1)), "view2", Seq.empty, Operation("engine", "view2E", AsIs), Map.empty, "view3"),
          Step("c",Set("c", "b"),rulesRaw(Seq(
          )).copy(id = Id(4,1)), "filteredView4", Seq(),
            Operation("engine", "view4E", OutputFieldOnly), Map.empty, "view5"),
          Step("d",Set("c", "b"),rulesRaw(Seq(
          )).copy(id = Id(4,1)), "filteredView4", Seq(),
            Operation("engine", "view4E", OutputFieldOnly), Map.empty, "view5")
        ))
      doSimpleEngine(flow)
    }
    r.msg should include(CycleDetected)
  }

  test("dq with certain processing combinations should fail") { // correct dq is in the "dq mixes in..." test
    def withOutput(resultApproach: ResultApproach) = {
      val step = Step("d", Set("c", "b"), rulesRaw(Seq(
        )).copy(id = Id(4, 1)), "filteredView4", Seq(),
          Operation("dq", "view4E", resultApproach), Map.empty, "view5")
      val r = intercept[FlowException] {
        val flow =
          new Flow(Id(1, 1), Seq(
            Step("a", Set.empty, rulesRaw(Seq(
            )), "view1", Seq.empty, Operation("engine", "view1E", AsIs), Map.empty, "view2"),
            Step("b", Set("a"), rulesRaw(Seq(
            )).copy(id = Id(2, 1)), "view2", Seq.empty, Operation("engine", "view2E", AsIs), Map.empty, "view3"),
            Step("c", Set(), rulesRaw(Seq(
            )).copy(id = Id(4, 1)), "filteredView4", Seq(),
              Operation("engine", "view4E", OutputFieldOnly), Map.empty, "view5"),
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
