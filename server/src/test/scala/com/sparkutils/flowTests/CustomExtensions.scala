package com.sparkutils.flowTests

import com.sparkutils.flow._
import com.sparkutils.flow.impl.util.FlowExceptionConstants.FlowEarlyExitException
import com.sparkutils.flowTests.RulesGen.{rulesRaw, testData}
import com.sparkutils.flowTests.utils.SharedPureConnectTests
import com.sparkutils.quality._
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Dataset, SaveMode, SparkSession}
import org.scalatest.Matchers

import scala.concurrent.ExecutionContext.Implicits.global

class CustomExtensions extends SharedPureConnectTests with Matchers {

  def buildFlow(resultApproach: ResultApproach = StarOnly, runner: Runner = Engine): Flow =
    new Flow(Id(1, 1), Seq(
      Step("a", Set.empty, Operation(rulesRaw(Seq(
        (ExpressionRule("true"), RunOnPassProcessor(1000, Id(1040, 1),
          OutputExpression("2")))
      )), runner, resultApproach)
      )
    ))

  def doFlowTest(flow: Flow, answer: Int = 2, process: DataFrame => DataFrame = _.select("result")): Unit = {

    val s = sparkSession
    import s.implicits._

    val data = Seq(
      Tuple2("c", 1)
    ).toDF("c", "d")

    val ir = process(flow.run(s, _ => Some(data)).head._2._2).as[Int]
    ir.head() shouldBe answer
  }

  test("custom result approach should have the same behaviour") {
    // default logic
    doFlowTest(buildFlow())
    // the custom should reply 1
    doFlowTest(buildFlow(CustomApproach(classOf[IStar].getName)), answer = 1, process = identity)
  }

  test("custom runner should have the same behaviour") {
    // default logic as part of the above test case

    // custom runner only
    doFlowTest(buildFlow(runner = CustomRunnerEngine(classOf[IRun].getName)))
    // custom runner and approach
    doFlowTest(buildFlow(CustomApproach(classOf[IStar].getName), runner = CustomRunnerEngine(classOf[IRun].getName)),
      answer = 1, process = identity)
  }

  test("custom result approach serialisation works") {
    val flow = buildFlow(CustomApproach(classOf[IStar].getName))
    val dses = toDatasets(sparkSession, flow)

    val (steps, _) = fromDatasets(sparkSession, dses, flow.flowId)
    steps shouldBe flow.steps
  }

  test("custom runner serialisation works") {
    val flow = buildFlow(runner = CustomRunnerEngine(classOf[IRun].getName))
    val dses = toDatasets(sparkSession, flow)

    val (steps, _) = fromDatasets(sparkSession, dses, flow.flowId)
    steps shouldBe flow.steps
  }
}

class IStar() extends CustomResultApproach {

  override def apply(input: DataFrame, function: Column, engineInputs: EngineInputs, resultProcessInputs: ResultProcessInputs, step: Step): DataFrame =
    input.select(function).select(resultProcessInputs.children: _*).selectExpr("result - 1")

}

class IRun() extends CustomRunner {

  override def apply(dataFrame: DataFrame, engineInputs: EngineInputs, step: Step): RunnerOutput =
    RunnerOutput(expr("named_struct('result',2)").as(step.defaultFieldName), Seq("result"), engineInputs.dataRefTypeFields)

}