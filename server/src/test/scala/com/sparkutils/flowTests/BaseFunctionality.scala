package com.sparkutils.flowTests

import com.sparkutils.flow.{OperationProcessing, _}
import com.sparkutils.flow.impl.util.FlowExceptionConstants.FlowEarlyExitException
import com.sparkutils.flow.impl.util.Utils.MapOps
import com.sparkutils.flowTests.RulesGen.{rulesRaw, testData}
import com.sparkutils.flowTests.utils.SharedPureConnectTests
import com.sparkutils.quality._
import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.{AnalysisException, DataFrame, Encoder, SaveMode, SparkSession}
import org.scalatest.Matchers
import com.sparkutils.quality.implicits._
import com.sparkutils.flow.implicits._

import scala.concurrent.ExecutionContext.Implicits.global

case class TestOn(product: String, account: String, subcode: Int)

case class NewPosting(transfer_type: String, account: String, product: String, subcode: Int)
case class Posting(transfer_type: String, account: String)

object RulesGen {

  val testData = Seq(
    TestOn("edt", "4201", 40),
    TestOn("otc", "5201", 40),
    TestOn("fi", "4251", 50),
    TestOn("fx", "4206", 90),
    TestOn("fxotc", "4201", 40),
    TestOn("eqotc", "4201", 60)
  )

  def rulesRaw(expressionRules: Seq[(ExpressionRule, RunOnPassProcessor)]) = {
    registerLambdaFunctions(Seq(
      LambdaFunction("account_row", "(transfer_type, account) -> named_struct('transfer_type', transfer_type, 'account', account, 'product', product, 'subcode', subcode)", Id(123, 23)),
      LambdaFunction("account_row", "transfer_type -> account_row(transfer_type, account)", Id(123, 24)),
      LambdaFunction("subcode", "(transfer_type, sub) -> updateField(account_row(transfer_type, account), 'subcode', sub)", Id(123, 25))
    ))

    val rules =
      for { ((exp, processor), idOffset) <- expressionRules.zipWithIndex }
        yield Rule(Id(100 * idOffset, 1), exp, processor)

    val rsId = Id(1, 1)
    val ruleSuite = RuleSuite(rsId, Seq(
      RuleSet(Id(50, 1), rules
      )))

    ruleSuite
  }
}

class BaseFunctionality extends SharedPureConnectTests with Matchers {

  test("multiple roots and paths should work, along with default columns, views and json serialisation") {
    // a and c are roots, d joins both c and b
    val flowOG = new FlowT[Id, RuleSuite](Id(1,1), Seq(
      Step("a",Set.empty, Operation(rulesRaw(Seq(
        (ExpressionRule("product = 'edt'"), RunOnPassProcessor(1000, Id(1040, 1),
          OutputExpression("array(account_row('from'), account_row('to', 'other_account1'))")))
        )), Engine, AsIs)),
      Step("b",Set("a"), Operation(rulesRaw(Seq(
        (ExpressionRule("product like 'fx%'"), RunOnPassProcessor(1000, Id(1040, 1),
          OutputExpression("array(account_row('from'), account_row('from', 'other_account2'))")))
        )).copy(id = Id(2,1)), Engine, AsIs)),
      Step("c",Set.empty, Operation(rulesRaw(Seq(
        (ExpressionRule("product == 'edt'"),  RunOnPassProcessor(1000, Id(1040, 1),
          OutputExpression("null")))
        )).copy(id = Id(3,1)), Engine, AsIs)),
      Step("d",Set("c", "b"), Operation(rulesRaw(Seq(
        (ExpressionRule("true"),  RunOnPassProcessor(1000, Id(1041, 1),
          OutputExpression("a.result")))
        )).copy(id = Id(4,1)), Engine, AsIs, combineAuditWith = Set("view3_audit")),
        initConfiguration = StepInitConfiguration(viewConfig = Seq(
          ViewRow(ruleSuiteId = 4, ruleSuiteVersion = 1, name = "filteredView4", token = None, filter = None,
          sql = Some("select v4.*, v3.flow_audit view3_audit, v3.a from c v4 join b v3 on " +
            "v4.product = v3.product where v4.c.salientRule is not null")))),
        data = StepData(inputView = Some("filteredView4"))
        ) // flow_audit here is from v4
    ))//, showInterim = true)
    val s = sparkSession
    import s.implicits._

    // write out to the full version and read back again
    val ds = toFullFlow(s, flowOG)
    ds.write.mode(SaveMode.Overwrite).json(outputDir + "/multipleRoots.json")
    val rds = s.read.schema(typedFullFlowExpEnc[Id, CombinedRuleSuiteRows].schema).json(outputDir + "/multipleRoots.json").
      as[FullFlow[Id, CombinedRuleSuiteRows]]
    val flowData = fromFullFlow(rds, flowOG.flowId)
    val flow = new Flow(flowOG.flowId, flowData.steps, flowData.flowRow.flowAuditColName)

    import s.implicits._
    val ires = flow.run(sparkSession, _ => Some(testData.toDF()))
    ires.stepResults.size shouldBe 4
    ires.stepResults.keys.toSet shouldBe Set("a", "b", "c", "d")

    // completion with the full set of responses is main test, 2nd is to ensure combineAuditWith works across
    // multiple parents

    import com.sparkutils.quality.implicits._
    val rgs = ires.stepResults("d").output.selectExpr("flow_audit.*").as[RuleSuiteGroupResults].collect()
    verifyRuleSuites(rgs, Set(Id(1,1), Id(2,1), Id(3,1), Id(4,1)))(identity)
  }

  // c depends on both a and b
  def engineFlow(dataHandling: Boolean = false, nullFieldCheck: String = "salientRule", runner: Runner = Engine) = new Flow(Id(1,1), Seq(
    Step("a",Set.empty, Operation(rulesRaw(Seq(
      (ExpressionRule("product = 'edt'"), RunOnPassProcessor(1000, Id(1040, 1),
        OutputExpression("array(account_row('from'), account_row('to', 'other_account1'))")))
    )), runner, AsIs, "view1E"), data = StepData( "view1", "view2", cacheResults = true)),
    Step("b",Set("a"), Operation(rulesRaw(Seq(
      (ExpressionRule("product like 'fx%'"), RunOnPassProcessor(1000, Id(1040, 1),
        OutputExpression("array(account_row('from'), account_row('from', 'other_account2'))")))
    )).copy(id = Id(2,1)),runner, AsIs, "view2E"), data = StepData("view2", "view3")),
    Step("c",Set("a", "b"), Operation(rulesRaw(Seq(
      (ExpressionRule("view1E.result is not null"), RunOnPassProcessor(1000, Id(1040, 1),
        OutputExpression("view1E.result"))),
      (ExpressionRule("view2E.result is not null"), RunOnPassProcessor(1000, Id(1041, 1),
        OutputExpression("view2E.result")))
    )).copy(id = Id(3,1)),runner, StarOnly, "view3E"), data = StepData("view3",  "view4")),
    Step("d",Set("c"), Operation(rulesRaw(Seq(
      (ExpressionRule("true"), RunOnPassProcessor(1000, Id(1041, 1),
        OutputExpression("result")))
    )).copy(id = Id(4,1)), runner, OutputFieldOnly, "view4E"),
      initConfiguration = StepInitConfiguration(viewConfig = Seq(
        ViewRow(ruleSuiteId = 4, ruleSuiteVersion = 1, name = "filteredView4", token = None, filter = None,
          sql = Some(s"select * from view4 where $nullFieldCheck is not null")))),
      data = StepData("filteredView4", "view5"))
  )) with FlowDataHandling[CombinedRuleSuiteRows, RuleSuite] {
    override protected def loadData(sparkSession: SparkSession, token: String): DataFrame = {
      if (dataHandling)
        // same as the real code, just forces the default to work
        sparkSession.sql(s"select * from `${token}` ")
      else
        super.loadData(sparkSession: SparkSession, token: String)
    }
  }

  test("Simple chain of engines should work with filter") {
    val flow = engineFlow()

    doSimpleEngine(flow)

    // verify the default route delegating to loadData
    val flow2 = engineFlow(true)

    doSimpleEngine(flow2, sessionOnly = true)
  }

  test("Simple chain of collectors should work with filter") {
    val flow = engineFlow(runner = Collect, nullFieldCheck = "result")

    doSimpleEngine(flow, expectedCount = 6)
  }

  def doSimpleEngine(flow: Flow, sessionOnly: Boolean = false, expectedCount: Int = 3): Unit = {

    val s = sparkSession
    import s.implicits._
    val ires = if (sessionOnly) flow.run(sparkSession) else flow.run(sparkSession, _ => Some(testData.toDF()))
    ires.stepResults.size shouldBe 4
    ires.stepResults.keys.toSeq shouldBe Seq("a", "b", "c", "d")

    val res = ires.stepResults("d").output
    // after removing the nulls in step 4's filter
    res.count() shouldBe expectedCount

    val rows = res.selectExpr("view4E.result").as[Seq[NewPosting]].collect()
    rows.filterNot(_.isEmpty) shouldBe Seq(
      Seq(NewPosting("from", "4201", "edt", 40), NewPosting("to", "other_account1", "edt", 40)),
      Seq(NewPosting("from", "4206", "fx", 90), NewPosting("from", "other_account2", "fx", 90)),
      Seq(NewPosting("from", "4201", "fxotc", 40), NewPosting("from", "other_account2", "fxotc", 40))
    )
  }

  def verifyRuleSuites[T](r: Seq[T], expected: Set[Id] = Set(Id(1,1), Id(2,0)))(
    ruleSuiteGroup: T => RuleSuiteGroupResults): Unit = {
    r.map(ruleSuiteGroup(_).ruleSuiteResults.keys.toSet).distinct shouldBe Seq(
      expected
    )
  }

  def folderFlow(extraSteps: Seq[Step[RuleSuite]] = Seq.empty) = new Flow(Id(1,1), Seq(
    Step("a",Set.empty, Operation[RuleSuite](rulesRaw(Seq(
      (ExpressionRule("product = 'edt'"), RunOnPassProcessor(1000, Id(1040, 1),
        OutputExpression("set(subcode = 10)")))
      )), Folder, MergeFields, "view1E"), // identity should be added automatically
      data = StepData("view1", "view2")),
    Step("b",Set("a"), Operation[RuleSuite](rulesRaw(Seq(
      (ExpressionRule("product like 'fx%'"), RunOnPassProcessor(1000, Id(1040, 1),
        OutputExpression("set(account = 'newacc')")))
      )).copy(defaultProcessor = DefaultProcessor(Id(1041,1), OutputExpression("row -> row")),
      id = Id(2,0)), Folder, MergeFields, "view2E"),
      data = StepData("view2", "view3")
    )
  ) ++ extraSteps)

  test("folders should correctly nest and merge fields with chained audit") {
    val flow = folderFlow()

    doFolderTest(flow)
  }

  def doFolderTest[T,P](flow: FlowT[T,P]): Unit = {
    val s = sparkSession
    import s.implicits._
    val res = flow.run(sparkSession, _ => Some(testData.toDF())).stepResults("b").output
    val rows = res.drop("view1E", "view2E", "flow_audit").as[TestOn].collect()
    rows shouldBe Seq(
      TestOn("edt", "4201", 10),
      TestOn("otc", "5201", 40),
      TestOn("fi", "4251", 50),
      TestOn("fx", "newacc", 90),
      TestOn("fxotc", "newacc", 40),
      TestOn("eqotc", "4201", 60)
    )

    import com.sparkutils.quality.implicits._
    val rgs = res.selectExpr("flow_audit.*").as[RuleSuiteGroupResults].collect()
    verifyRuleSuites(rgs)(identity)
  }

  test("Serialising for engine example") {
    val flow = engineFlow()
    val dses = toDatasets(sparkSession, flow)

    val flowData = fromDatasets(sparkSession, dses, flow.flowId)
    doSimpleEngine(new Flow(flow.flowId, flowData.steps, flowData.flowRow.flowAuditColName))
  }

  test("Serialising for folder example") {
    val flow = folderFlow()
    val dses = toDatasets(sparkSession, flow)

    val flowData = fromDatasets(sparkSession, dses, flow.flowId)
    doFolderTest(new Flow(flow.flowId, flowData.steps, flowData.flowRow.flowAuditColName, flowRuleGroup = flowData.flowRow.flowRuleGroup))
  }

  test("Serialising for folder full id example") {
    val flow = folderFlow()
    val ogds = toDatasets(sparkSession, flow)
    val full = toFullFlow(sparkSession, flow)
    val s = sparkSession
    import s.implicits._

    val ids = full.map(convertToIds)

    val flowData = fromFullFlow(IdFromFlows)(ids, flow.flowId)
    val idf = new FlowT[CombinedRuleSuiteRows, GroupRuleId](flow.flowId, flowData.steps, flowData.flowRow.flowAuditColName,
      flowRuleGroup = flowData.flowRow.flowRuleGroup)
    doFolderTest(idf)

    // use converted with datasets via rulesuites
    val dses = toDatasets(IdFromFlows)(sparkSession, idf)
    val flowData2 = fromDatasets(sparkSession, dses, flow.flowId)
    doFolderTest(new Flow(flow.flowId, flowData2.steps, flowData.flowRow.flowAuditColName,
      flowRuleGroup = flowData2.flowRow.flowRuleGroup))

    // from datasets using IdFromFlows
    val flowData3 = fromDatasets(IdFromFlows)(sparkSession, dses, flow.flowId)
    doFolderTest(new FlowT(flow.flowId, flowData3.steps, flowData.flowRow.flowAuditColName,
      flowRuleGroup = flowData3.flowRow.flowRuleGroup))

    // from datasets using IdFromFlows via rsname
    val idf4 = new FlowT[CombinedRuleSuiteRows, GroupRuleId](flow.flowId,
      flowData.steps.map(s => s.copy(options = s.options + (useFlowRuleGroupLevelRows -> "false"))),
      flowData.flowRow.flowAuditColName,
      flowRuleGroup = flowData.flowRow.flowRuleGroup)
    val dses4 = toDatasets(IdFromFlows)(sparkSession, idf4)

    val flowData4 = fromDatasets(IdFromFlows)(sparkSession, dses4, flow.flowId)
    doFolderTest(new FlowT(flow.flowId, flowData4.steps, flowData.flowRow.flowAuditColName,
      flowRuleGroup = flowData4.flowRow.flowRuleGroup))

    // from datasets using IdFromFlows with specific name
    val idf5 = new FlowT[CombinedRuleSuiteRows, GroupRuleId](flow.flowId,
      flowData.steps.map(s => s.copy(options = s.options + (useRuleGroupName -> defaultConvertGroupName))),
      flowData.flowRow.flowAuditColName,
      flowRuleGroup = flowData.flowRow.flowRuleGroup)
    val dses5 = toDatasets(IdFromFlows)(sparkSession, idf5)

    val flowData5 = fromDatasets(IdFromFlows)(sparkSession, dses5, flow.flowId)
    doFolderTest(new FlowT(flow.flowId, flowData5.steps, flowData.flowRow.flowAuditColName,
      flowRuleGroup = flowData5.flowRow.flowRuleGroup))

    // from datasets using IdFromFlows but fallback to rsname as there is no group
    val flowData6 = fromDatasets(IdFromFlows)(sparkSession,
      ogds.copy(flows = Seq.empty[FlowRow[CombinedRuleSuiteRows]].toDS), flow.flowId)
    val flow6 = new FlowT(flow.flowId, flowData6.steps, flowData.flowRow.flowAuditColName,
      flowRuleGroup = flowData6.flowRow.flowRuleGroup)
    doFolderTest(flow6)
    // check fromFullRow default
    val full6 = toFullFlow(IdFromFlows)(sparkSession, flow6)
    val fromFull6 = fromFullFlow(IdFromFlows)(full6, flow.flowId)
    val fe = intercept[FlowException] {
      doFolderTest(new FlowT(flow.flowId, fromFull6.steps, flowData.flowRow.flowAuditColName,
        flowRuleGroup = fromFull6.flowRow.flowRuleGroup))
    }
    fe.msg should include("no matching RuleSuite was found")
  }

  test("merge fields with rules using fields which are not in the output") {

    def flow(map: Map[String, String], resultApproach: ResultApproach = MergeFields): Unit = {
      val flow = new Flow(Id(1, 1), Seq(
        Step("a", Set.empty, Operation(rulesRaw(Seq(
          (ExpressionRule("true"), RunOnPassProcessor(1000, Id(1040, 1),
            OutputExpression("struct('a' as a, (c || cast(d as string)) as b)")))
          )), Engine, resultApproach, "view1E"), // identity should be added automatically
          options = map, data = StepData("view1", "view2")
        )
      ))

      val s = sparkSession
      import s.implicits._

      val data = Seq(
        Tuple2("c", 1)
      ).toDF("c", "d")

      val ir = flow.run(s, _ => Some(data))
      val cols = Set("a", "b", "flow_audit", "view1E") ++ (
        resultApproach match {
          case OutputFieldsOnly => Seq()
          case MergeFields => Set("c", "d")
        }
      )

      ir.stepResults.head._2.output.schema.map(_.name).toSet shouldBe cols
      ir.stepResults.head._2.output.collect().length shouldBe 1
    }

    val resultType = Map(
      resultDataType -> "struct<a: string, b: string>"
    )
    // using quicker path
    flow(resultType)
    // extra projection
    flow(Map.empty)

    val forcedType = resultType + (forceMergeProjection -> "true")

    // using resultType but forcing extra projection
    flow(forcedType)

    // using quicker path
    flow(resultType, OutputFieldsOnly)
    // extra projection
    flow(Map.empty, OutputFieldsOnly)

    // using resultType but forcing extra projection
    flow(forcedType, OutputFieldsOnly)
  }

  test("dq mixes in") {
    val flow = folderFlow(Seq(
      Step(
        "c", Set("b"), Operation(rulesRaw(Seq(
          (ExpressionRule("true"), NoOpRunOnPassProcessor.noOp)
        )).copy(Id(100,1)), DQ, AsIs, "dq"),
        data = StepData("view3", "view4")
      )
    ))

    val s = sparkSession
    import com.sparkutils.quality.implicits._
    import s.implicits._
    val res = flow.run(sparkSession, _ => Some(testData.toDF())).stepResults("c").output

    val cols = Set("view1E","view2E","product","account","subcode","dq","flow_audit")
    res.schema.map(_.name).toSet shouldBe cols
    val cres = res.selectExpr("flow_audit.*").as[RuleSuiteGroupResults].collect()
    // all true
    cres.map(r => r.ruleSuiteResults(Id(100,1)).overallResult).exists(r => r != Passed) shouldBe false
  }

  test("lca works - row_number") {

    def lca(view: String = "", resultApproach: ResultApproach = OutputFieldsOnly, forceStar: Boolean = false) = {
      val flow = new Flow(Id(1, 1), Seq(
        Step("a", Set.empty, Operation(rulesRaw(Seq(
          (ExpressionRule("true"), RunOnPassProcessor(1000, Id(1040, 1),
            OutputExpression(s"set(d = (row_number() OVER (ORDER BY ${view}c)) + 1 )"))),
          (ExpressionRule("true"), RunOnPassProcessor(1000, Id(1041, 1),
            OutputExpression(s"set(c = if(${view}d = 2, 'a', 'b'))")))
          )), Folder, resultApproach, "view1E"), // identity should be added automatically
          options = Map(
              //wrapInputFields -> wrap,
              forceMergeProjection -> forceStar.toString
          ),
          data = StepData("view1", "view2")
        )
      ))

      val s = sparkSession
      import s.implicits._

      val data = Seq(
        Tuple2("c", 1)
      ).toDF("c", "d")

      val ir = flow.run(s, _ => Some(data))

      ir.stepResults.head._2.output.columns.toSet shouldBe Set("view1E", "flow_audit", "c", "d")
      val d = ir.stepResults.head._2.output.selectExpr("c", "d").as[(String,Option[Int])]

      val r = d.collect()
      r.length shouldBe 1
      r.head shouldBe ("b",Some(2))
      ir.stepResults.head._2.output
    }

    lca()
    lca(resultApproach = MergeFields)
    // force star adds a projection so it's always present
    lca(forceStar = true)
    lca(resultApproach = MergeFields, forceStar = true)

    val view1 = "view1."

    // The alias is c, so although it's the expression for d AND both c + d are required, only c actually needs it
    val r = lca(view1)
    //r.show
    lca(view1, MergeFields)
    // for completeness, but don't really add any functional testing
    lca(view1, forceStar = true)
    lca(view1, MergeFields, forceStar = true)
  }

  test("bad ddl") {
    val e = intercept[FlowException] {
      Map("s" -> "*").dataType("s")
    }
    e.msg should include("is invalid")
  }

  test("bad struct type") {
    val e = intercept[FlowException] {
      Map("s" -> "string").structType("s")
    }
    e.msg should include("is not a StructType")
  }

  test("maps should load and run") {
    val flow = new Flow(Id(1, 1), Seq(
      Step("a", Set.empty, Operation(rulesRaw(Seq(
        (ExpressionRule("true"), RunOnPassProcessor(1000, Id(1041, 1),
          OutputExpression(s"set(c = if(d = 2, 'a', 'b'))")))
      )), Folder, MergeFields), options = Map(inputSchema -> "struct<c: String, d: Int>", resultDataType -> "struct<c: String, d: Int>")
      ),
      Step("b", Set("a"), Operation(rulesRaw(Seq(
        (ExpressionRule("true"), RunOnPassProcessor(1000, Id(1041, 1),
          OutputExpression(s"set(c = if(map_lookup('thed', c, themaps) = 2, 'a', 'b'))")))
      )), Folder, MergeFields), initConfiguration = StepInitConfiguration(
        mapConfig = Seq(
          MapRow(Id(1,1), "thed", token = None, filter = None, sql = Some(s"select * from a$rootInputSuffix"), key = "c", value = "d")
        ),
        mapName = Some("themaps")
      ))
    ))

    val s = sparkSession
    import s.implicits._

    val data = Seq(
      Tuple2("c", 1),
      Tuple2("c", 1),
      Tuple2("c", 1),
      Tuple2("c", 1),
      Tuple2("c", 1),
      Tuple2("c", 1),
      Tuple2("c", 1)
    )

    val ir = flow.run(s, _ => Some(data.toDF("c", "d")))

    val d = ir.stepResults("b").output.selectExpr("c", "d").as[(String, Option[Int])]

    val r = d.collect()
    // the test is simply does the map lookup work.
  }

  test("configured flowEarlyExitSQL should appropriately run and fail") {

    def earlyExit(options: Map[String, String]) = {
      val flow = new Flow(Id(1, 1), Seq(
        Step("a", Set.empty, Operation(rulesRaw(Seq(
          (ExpressionRule("true"), RunOnPassProcessor(1000, Id(1041, 1),
              OutputExpression(s"set(c = if(d = 2, 'a', 'b'))")))
          )), Folder, MergeFields),
          options = options
        ),
        Step("b", Set("a"), Operation(rulesRaw(Seq(
          (ExpressionRule("true"), RunOnPassProcessor(1000, Id(1041, 1),
              OutputExpression(s"set(c = if(d = 2, 'a', 'b'))")))
          )), Folder, MergeFields)
        )
      )) {
        // wierd view bug with connect, could be due to local views on connect not being visible to name resolution, needs adding to test in
        override protected def startStep(input: DataFrame, step: Step[RuleSuite], previousSteps: Set[StepResult[RuleSuite]]): DataFrame = {
          input.write.mode(SaveMode.Overwrite).parquet(outputDir+s"/flowearly${step.name}")
          input.sparkSession.read.parquet(outputDir+s"/flowearly${step.name}")
        }
      }

      val s = sparkSession
      import s.implicits._

      val data = Seq(
        Tuple2("c", 1),
        Tuple2("c", 1),
        Tuple2("c", 1),
        Tuple2("c", 1),
        Tuple2("c", 1),
        Tuple2("c", 1),
        Tuple2("c", 1)
      ).toDF("c", "d")

      val ir = flow.run(s, _ => Some(data))

      val d = ir.stepResults("b").output.selectExpr("c", "d").as[(String, Option[Int])]

      val r = d.collect()

    }

    // no flow, same case as all the other tests but control for the ones below

    def shouldExit(sql: String, stepName: String = "", msg: Option[String] = None): Unit = {
      val thrown = intercept[FlowException] {
        earlyExit(Map(flowEarlyExitSQL -> sql) ++ msg.fold(Map.empty[String,String])(
          m => Map( flowEarlyExitException -> m )
        ))
      }
      val expected = msg.getOrElse(FlowEarlyExitException(stepName))
      thrown.msg shouldBe expected
    }

    // false so should exit
    shouldExit(s"select false $flowEarlyExitColumn from a")
    // bad sql so should exit
    shouldExit(s"iIzBad")
    // bad sql so should exit, with this message
    shouldExit(s"iIzBad", msg = Some("I WAS BAD"))

  }

}
