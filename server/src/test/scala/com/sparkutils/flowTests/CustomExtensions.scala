package com.sparkutils.flowTests

import com.sparkutils.flow.FlowGroupAndOperationType.WritableRuleSuite
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

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

trait CustomExtensionsUtils { thisType: SharedPureConnectTests with Matchers =>

  def buildFlow[FG](resultApproach: ResultApproach = StarOnly, runner: Runner = Engine,
                    options: Map[String,String] = Map.empty, flowRuleGroup: Option[FlowRuleGroup[FG]] = None): FlowT[FG, RuleSuite] =
    new FlowT[FG, RuleSuite](Id(1, 1), Seq(
      Step("a", Set.empty, Operation[RuleSuite](rulesRaw(Seq(
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

}

class CustomExtensions extends SharedPureConnectTests with Matchers with CustomExtensionsUtils {

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

  test("bad class name custom result approach should throw") {
    val clzz = "classOf[IStar].getName"
    // the custom should reply 1
    val e = intercept[FlowExceptionType] {
      doFlowTest(buildFlow(CustomApproach(clzz)), answer = 1, process = identity)
    }
    e.msg should include(clzz)
  }

  test("bad custom runner name should throw") {
    val clzz = "classOf[IRun].getName"
    // custom runner only
    val e = intercept[FlowExceptionType] {
      doFlowTest(buildFlow(runner = CustomRunnerEngine(clzz)))
    }
    e.msg should include(clzz)
  }

  test("custom runner throws are re-thrown") {
    val t = intercept[FlowExceptionType] {
      doFlowTest(buildFlow(runner = CustomRunnerEngine(classOf[IRun].getName), options = Map("throw" -> "true")))
    }
    t.msg should include( "I AM THROWING")
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
  def doNoOpRunnerGroup[T: TypedEncoder: FlowRuleGroupProcessing, OP <: OperationProcessing](process: OP)(
      grpF: RuleSuite => FlowRuleGroup[T])( flowF: (FlowDataSets[T], RuleSuite) => FlowDataSets[T])(op: OP)(
      implicit rtp: RuleSuiteTypeParam[OP#ResType], rp : RuleSuiteParam[OP#ResType],
      operation: FlowGroupAndOperationType[T, OP], toDSImplFG: FlowGroupAndOperationType[T,RuleSuiteFromDataset.type],
      toDSImplEnc: Encoder[FullFlow[T,CombinedRuleSuiteRows]], toFlowEnc: Encoder[FullFlow[T,OP#StorageType]],
      storageParam: RuleSuiteStorageType[OP#StorageType], storageTypedEnc: TypedEncoder[OP#StorageType]
    ): Unit = {
    import com.sparkutils.flow.implicits._

    // add the test suite...
    val rs = rulesRaw(Seq(
      (ExpressionRule("true"), RunOnPassProcessor(1000, Id(1040, 1),
        OutputExpression("4")))
    )).copy(id = Id(200,1))

    val grp = grpF(rs)

    val flow = buildFlow[T](runner = NoOp, flowRuleGroup = Some(grp))
    val odses = toDatasets(RuleSuiteFromDataset)(sparkSession, flow)

    val s = sparkSession
    import s.implicits._

    // TODO Cache needed to stop Spark optimiser bug - make a simpler test case and raise
    val dses = flowF(odses, rs)

    val flowData = fromDatasets(op)(sparkSession, dses, flow.flowId)
    toFullFlow(process)(sparkSession, new FlowT[T, OP#ResType](Id(flowData.flowRow.flowId, flowData.flowRow.flowVersion), flowData.steps,
      flowRuleGroup = flowData.flowRow.flowRuleGroup)).write.mode(SaveMode.Overwrite).json(outputDir + "/" + grp.ruleGroupName)
    val readBack =
      s.read.schema(typedFullFlowExpEnc[T, OP#StorageType].schema).json(outputDir + "/" + grp.ruleGroupName).
        as[FullFlow[T, OP#StorageType]]

    // do the rule group id serde
    readBack.head().flowRow.flowRuleGroup.toSet shouldBe flowData.flowRow.flowRuleGroup.toSet

    val data = Seq(
      Tuple2("c", 1)
    ).toDF("c", "d")

    // use the data which should have been registered
    val r = data.withColumn("runner",expr(s"rule_engine_runner(rule_suite_from(${grp.ruleGroupName}, 200))")).
      selectExpr("runner.result").as[Int].head()
    r shouldBe 4
    val fromFull = fromFullFlow(process)(readBack, flow.flowId)
    val reread = new FlowT(flow.flowId, fromFull.steps, fromFull.flowRow.flowAuditColName,
      flowRuleGroup = fromFull.flowRow.flowRuleGroup)
    val fr = flow.run(s, _ => Some(data))
    // enough to prove it worked as non-tolerant
  }

  test("NoOp runner and var rule group loading works id's") {
    doNoOpRunnerGroup(RuleSuiteFromDataset)(_ => FlowRuleGroup("noOpFlowId", Seq(Id(200,1)))){
      (odses, rs) =>

        val s = sparkSession
        import s.implicits._

        odses.copy(
          ruleRows = odses.ruleRows union toDS(rs),
          outputExpressionRows = Some(
            odses.outputExpressionRows.fold(toOutputExpressionDS(rs))(d => (d union toOutputExpressionDS(rs)).cache())
          ),
          ruleSuites = Some(
            odses.ruleSuites.fold(Seq(toRuleSuiteRow(rs)._1).toDS())(d => (d union Seq(toRuleSuiteRow(rs)._1).toDS()).cache())
          )
        )
      }(RuleSuiteFromDataset)
  }

  test("NoOp runner and var rule group loading works with combined") {
    doNoOpRunnerGroup(RuleSuiteFromDataset){rs =>
      val s = sparkSession
      import s.implicits._

      FlowRuleGroup("noOpFlowCombined", combined_rows(rs).collect().toSeq )
    }((odses, _) => odses)(RuleSuiteFromDataset)
  }

  test("id not present on RuleSuiteFromDataset") {
    val fe = intercept[FlowException] {
      doNoOpRunnerGroup(RuleSuiteFromDataset) { rs =>
        val s = sparkSession
        import s.implicits._

        FlowRuleGroup("noOpFlowCombined", combined_rows(rs).collect().toSeq)
      }{(odses, _) =>
        odses.copy(steps = odses.steps.map(s => s.copy(ruleSuiteId = 1200202)))
      }(RuleSuiteFromDataset)
    }

    fe.msg should include("is missing")
  }
}

class IStar() extends CustomResultApproach {

  override def apply[RP: RuleSuiteParam](input: DataFrame, function: Column, engineInputs: RunnerInputs, resultProcessInputs: ResultProcessInputs, step: Step[RP]): DataFrame =
    input.select(function).select(resultProcessInputs.children: _*).selectExpr("result - 1")

}

class IRun() extends CustomRunner {

  override def apply[RP: RuleSuiteParam](dataFrame: DataFrame, engineInputs: RunnerInputs, step: Step[RP]): RunnerOutput =
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