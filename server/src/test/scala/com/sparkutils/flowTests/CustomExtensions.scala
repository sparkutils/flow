package com.sparkutils.flowTests

import com.sparkutils.flow._
import com.sparkutils.flow.impl.util.FlowExceptionConstants.FlowEarlyExitException
import com.sparkutils.flowTests.RulesGen.{rulesRaw, testData}
import com.sparkutils.flowTests.utils.SharedPureConnectTests
import com.sparkutils.quality._
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Dataset, Encoder, SaveMode, SparkSession}
import org.scalatest.Matchers
import com.sparkutils.flow.implicits._
import frameless.TypedEncoder

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

class CustomExtensions extends SharedPureConnectTests with Matchers {

  def buildFlow[FG](resultApproach: ResultApproach = StarOnly, runner: Runner = Engine,
                options: Map[String,String] = Map.empty, flowRuleGroup: Option[FlowRuleGroup[FG]] = None): FlowT[FG] =
    new FlowT[FG](Id(1, 1), Seq(
      Step("a", Set.empty, Operation(rulesRaw(Seq(
        (ExpressionRule("true"), RunOnPassProcessor(1000, Id(1040, 1),
          OutputExpression("2")))
      )), runner, resultApproach), options = options
      )
    ), flowRuleGroup = flowRuleGroup)

  def doFlowTest(flow: Flow, answer: Int = 2, process: DataFrame => DataFrame = _.select("result")): Unit = {

    val s = sparkSession
    import s.implicits._

    val data = Seq(
      Tuple2("c", 1)
    ).toDF("c", "d")

    val ir = process(flow.run(s, _ => Some(data)).stepResults.head._2.output).as[Int]
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

  test("custom runner throws are re-thrown") {
    val t = intercept[FlowException] {
      doFlowTest(buildFlow(runner = CustomRunnerEngine(classOf[IRun].getName), options = Map("throw" -> "true")))
    }
    t.msg shouldBe "I AM THROWING"
  }

  test("custom result approach serialisation works") {
    val flow = buildFlow[CombinedRuleSuiteRows](CustomApproach(classOf[IStar].getName))
    val dses = toDatasets(sparkSession, flow)

    val flowData = fromDatasets(sparkSession, dses, flow.flowId)
    flowData.steps shouldBe flow.steps
  }

  test("custom runner serialisation works") {
    val flow = buildFlow[CombinedRuleSuiteRows](runner = CustomRunnerEngine(classOf[IRun].getName))
    val dses = toDatasets(sparkSession, flow)

    val flowData = fromDatasets(sparkSession, dses, flow.flowId)
    flowData.steps shouldBe flow.steps
  }
//(
//    implicit ffenc: Encoder[FullFlow[T]], fgenc: Encoder[FlowRuleGroup[T]])
  def doNoOpRunnerGroup[T: TypedEncoder: FlowRuleGroupProcessing](
      grpF: RuleSuite => FlowRuleGroup[T])( flowF: (FlowDataSets[T], RuleSuite) => FlowDataSets[T]): Unit={
    import com.sparkutils.flow.implicits._

    // add the test suite...
    val rs = rulesRaw(Seq(
      (ExpressionRule("true"), RunOnPassProcessor(1000, Id(1040, 1),
        OutputExpression("4")))
    )).copy(id = Id(200,1))

    val grp = grpF(rs)

    val flow = buildFlow(runner = NoOp, flowRuleGroup = Some(grp))
    val odses = toDatasets(sparkSession, flow)

    val s = sparkSession
    import s.implicits._

    // TODO Cache needed to stop Spark optimiser bug - make a simpler test case and raise
    val dses = flowF(odses, rs)

    val flowData = fromDatasets(sparkSession, dses, flow.flowId)
    toFullFlow(sparkSession, new FlowT[T](Id(flowData.flowRow.flowId, flowData.flowRow.flowVersion), flowData.steps,
      flowRuleGroup = flowData.flowRow.flowRuleGroup)).write.mode(SaveMode.Overwrite).json(outputDir + "/" + grp.ruleGroupName)
    val readBack =
      s.read.schema(typedFullFlowExpEnc[T].schema).json(outputDir + "/" + grp.ruleGroupName).as[FullFlow[T]].head()
    // do the rule group id serde
    readBack.flowRow.flowRuleGroup.toSet shouldBe flowData.flowRow.flowRuleGroup.toSet

    val data = Seq(
      Tuple2("c", 1)
    ).toDF("c", "d")

    // use the data which should have been registered
    val r = data.withColumn("runner",expr(s"rule_engine_runner(rule_suite_from(${grp.ruleGroupName}, 200))")).
      selectExpr("runner.result").as[Int].head()
    r shouldBe 4
  }

  test("NoOp runner and var rule group loading works id's") {
    doNoOpRunnerGroup(_ => FlowRuleGroup("noOpFlowId", Seq(Id(200,1)))){
      (odses, rs) =>

        val s = sparkSession
        import s.implicits._

        odses.copy(
          ruleRows = odses.ruleRows union toDS(rs),
          outputExpressionRows = Some(
            odses.outputExpressionRows.fold(toOutputExpressionDS(rs))(d => (d union toOutputExpressionDS(rs)).cache())
          ),
          ruleSuites = Some(
            odses.ruleSuites.fold(Seq(toRuleSuiteRow(rs)._1).toDS)(d => (d union Seq(toRuleSuiteRow(rs)._1).toDS()).cache())
          )
        )
      }
  }

  test("NoOp runner and var rule group loading works with combined") {
    doNoOpRunnerGroup{rs =>
      val s = sparkSession
      import s.implicits._

      FlowRuleGroup("noOpFlowCombined", combined_rows(rs).collect().toSeq )
    }((odses, _) => odses)
  }
}

class IStar() extends CustomResultApproach {

  override def apply(input: DataFrame, function: Column, engineInputs: EngineInputs, resultProcessInputs: ResultProcessInputs, step: Step): DataFrame =
    input.select(function).select(resultProcessInputs.children: _*).selectExpr("result - 1")

}

class IRun() extends CustomRunner {

  override def apply(dataFrame: DataFrame, engineInputs: EngineInputs, step: Step): RunnerOutput =
    if (step.options.get("throw").fold(
      false
    ) { s =>
      val b = Try(s.toBoolean).getOrElse(true)
      b
    })
      throw FlowException("I AM THROWING")
    else
      RunnerOutput(expr("named_struct('result',2)").as(step.defaultFieldName), Seq("result"), engineInputs.dataRefTypeFields)

}