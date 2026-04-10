package com.sparkutils.flowTests

import com.sparkutils.flow.impl.util.FlowExceptionConstants.{CycleDetected, DuplicateNames, EmptyFlow, EmptyStepName, InvalidViewNames, MissingStep}
import com.sparkutils.flow.{AsIs, DQRunnerName, Flow, FlowDataHandling, FlowException, FolderRunnerName, MergeFields, Operation, OutputFieldOnly, OutputFieldsOnly, ResultApproach, StarOnly, Step, forceMergeProjection, fromDatasets, resultDataType, toDatasets, wrapInputFields}
import com.sparkutils.flowTests.RulesGen.{rulesRaw, testData}
import com.sparkutils.flowTests.utils.SharedPureConnectTests
import com.sparkutils.quality.{DataFrameLoader, DefaultProcessor, ExpressionRule, Id, LambdaFunction, NoOpRunOnPassProcessor, OutputExpression, Passed, Rule, RuleSet, RuleSuite, RuleSuiteGroupResults, RunOnPassProcessor, ViewRow, registerLambdaFunctions}
import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.scalatest.Matchers

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

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

  test("multiple roots and paths should work") {
    // a and c are roots, d joins both c and b
    val flow = new Flow(Id(1,1), Seq(
      Step("a",Set.empty,rulesRaw(Seq(
        (ExpressionRule("product = 'edt'"),  RunOnPassProcessor(1000, Id(1040, 1),
          OutputExpression("array(account_row('from'), account_row('to', 'other_account1'))")))
      )), "view1", Seq.empty, Operation("engine", "view1E", AsIs), Map.empty, "view2"),
      Step("b",Set("a"),rulesRaw(Seq(
        (ExpressionRule("product like 'fx%'"),  RunOnPassProcessor(1000, Id(1040, 1),
          OutputExpression("array(account_row('from'), account_row('from', 'other_account2'))")))
      )).copy(id = Id(2,1)), "view2", Seq.empty, Operation("engine", "view2E", AsIs), Map.empty, "view3"//,
        //combineAuditWith = Some(Set("view1E"))
      ),
      Step("c",Set.empty,rulesRaw(Seq(
        (ExpressionRule("product == 'edt'"),  RunOnPassProcessor(1000, Id(1040, 1),
          OutputExpression("null")))
      )).copy(id = Id(3,1)), "view3", Seq.empty, Operation("engine", "view3E", AsIs), Map.empty, "view4"),
      Step("d",Set("c", "b"),rulesRaw(Seq(
        (ExpressionRule("true"),  RunOnPassProcessor(1000, Id(1041, 1),
          OutputExpression("view1E.result")))
      )).copy(id = Id(4,1)), "filteredView4", Seq(
        ViewRow(ruleSuiteId = 4, ruleSuiteVersion = 1, name = "filteredView4", token = None, filter = None,
          sql = Some("select v4.*, v3.flow_audit view3_audit, v3.view1E from view4 v4 join view3 v3 on v4.product = v3.product where v4.view3E.salientRule is not null"))),
        Operation("engine", "view4E", AsIs), Map.empty, "view5",
        combineAuditWith = Some(Set("view3_audit"))) // flow_audit here is from v4
    ))//, showInterim = true)

    val s = sparkSession
    import s.implicits._
    val ires = flow.run(sparkSession, _ => Some(testData.toDF()))
    ires.size shouldBe 4
    ires.keys.toSeq shouldBe Seq("a", "b", "c", "d")

    // completion with the full set of responses is main test, 2nd is to ensure combineAuditWith works across
    // multiple parents

    import com.sparkutils.quality.implicits._
    val rgs = ires("d")._2.selectExpr("flow_audit.*").as[RuleSuiteGroupResults].collect()
    verifyRuleSuites(rgs, Set(Id(1,1), Id(2,1), Id(3,1), Id(4,1)))(identity)
  }

  // c depends on both a and b
  def engineFlow(dataHandling: Boolean = false) = new Flow(Id(1,1), Seq(
    Step("a",Set.empty,rulesRaw(Seq(
      (ExpressionRule("product = 'edt'"),  RunOnPassProcessor(1000, Id(1040, 1),
        OutputExpression("array(account_row('from'), account_row('to', 'other_account1'))")))
    )), "view1", Seq.empty, Operation("engine", "view1E", AsIs), Map.empty, "view2", cacheResults = true),
    Step("b",Set("a"),rulesRaw(Seq(
      (ExpressionRule("product like 'fx%'"),  RunOnPassProcessor(1000, Id(1040, 1),
        OutputExpression("array(account_row('from'), account_row('from', 'other_account2'))")))
    )).copy(id = Id(2,1)), "view2", Seq.empty, Operation("engine", "view2E", AsIs), Map.empty, "view3"),
    Step("c",Set("a", "b"),rulesRaw(Seq(
      (ExpressionRule("view1E.result is not null"),  RunOnPassProcessor(1000, Id(1040, 1),
        OutputExpression("view1E.result"))),
      (ExpressionRule("view2E.result is not null"),  RunOnPassProcessor(1000, Id(1041, 1),
        OutputExpression("view2E.result")))
    )).copy(id = Id(3,1)), "view3", Seq.empty, Operation("engine", "view3E", StarOnly), Map.empty, "view4"),
    Step("d",Set("c"),rulesRaw(Seq(
      (ExpressionRule("true"),  RunOnPassProcessor(1000, Id(1041, 1),
        OutputExpression("result")))
    )).copy(id = Id(4,1)), "filteredView4", Seq(
      ViewRow(ruleSuiteId = 4, ruleSuiteVersion = 1, name = "filteredView4", token = None, filter = None,
        sql = Some("select * from view4 where salientRule is not null"))),
      Operation("engine", "view4E", OutputFieldOnly), Map.empty, "view5")
  )) with FlowDataHandling {
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

  def doSimpleEngine(flow: Flow, sessionOnly: Boolean = false): Unit = {

    val s = sparkSession
    import s.implicits._
    val ires = if (sessionOnly) flow.run(sparkSession) else flow.run(sparkSession, _ => Some(testData.toDF()))
    ires.size shouldBe 4
    ires.keys.toSeq shouldBe Seq("a", "b", "c", "d")

    val res = ires("d")._2
    // after removing the nulls in step 4's filter
    res.count() shouldBe 3

    val rows = res.selectExpr("view4E.result").as[Seq[NewPosting]].collect()
    rows shouldBe Seq(
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

  def folderFlow(extraSteps: Seq[Step] = Seq.empty) = new Flow(Id(1,1), Seq(
    Step("a",Set.empty,rulesRaw(Seq(
      (ExpressionRule("product = 'edt'"),  RunOnPassProcessor(1000, Id(1040, 1),
        OutputExpression("set(subcode = 10)")))
    )), // identity should be added automatically
      "view1", Seq.empty, Operation("folder", "view1E", MergeFields), Map.empty, "view2"),
    Step("b",Set("a"),rulesRaw(Seq(
      (ExpressionRule("product like 'fx%'"),  RunOnPassProcessor(1000, Id(1040, 1),
        OutputExpression("set(account = 'newacc')")))
    )).copy(defaultProcessor = DefaultProcessor(Id(1041,1), OutputExpression("row -> row")),
      id = Id(2,0)), // force an identity default and Id change to verify combineAuditWith
      "view2", Seq.empty, Operation("folder", "view2E", MergeFields), Map.empty, "view3",
    //  combineAuditWith = Some(Set("view1E"))
    )
  ) ++ extraSteps)

  test("folders should correctly nest and merge fields with chained audit") {
    val flow = folderFlow()

    doFolderTest(flow)
  }

  def doFolderTest(flow: Flow): Unit = {
    val s = sparkSession
    import s.implicits._
    val res = flow.run(sparkSession, _ => Some(testData.toDF()))("b")._2
    val rows = res.drop("view1E", "view2E", "flow_audit").as[TestOn].collect()
    rows shouldBe Seq(
      TestOn("edt", "4201", 10),
      TestOn("otc", "5201", 40),
      TestOn("fi", "4251", 50),
      TestOn("fx", "newacc", 90),
      TestOn("fxotc", "newacc", 40),
      TestOn("eqotc", "4201", 60)
    )

    import frameless._
    import com.sparkutils.quality.implicits._
    val rgs = res.selectExpr("flow_audit.*").as[RuleSuiteGroupResults].collect()
    verifyRuleSuites(rgs)(identity)
  }

  test("Serialising for engine example") {
    val flow = engineFlow()
    val dses = toDatasets(sparkSession, flow)

    val (steps, name) = fromDatasets(sparkSession, dses, flow.flowId)
    doSimpleEngine(new Flow(flow.flowId, steps, name))
  }

  test("Serialising for folder example") {
    val flow = folderFlow()
    val dses = toDatasets(sparkSession, flow)

    val (steps, name) = fromDatasets(sparkSession, dses, flow.flowId)
    doFolderTest(new Flow(flow.flowId, steps, name))
  }

  test("merge fields with rules using fields which are not in the output") {

    def flow(map: Map[String, String], resultApproach: ResultApproach = MergeFields) = {
      val flow = new Flow(Id(1, 1), Seq(
        Step("a", Set.empty, rulesRaw(Seq(
          (ExpressionRule("true"), RunOnPassProcessor(1000, Id(1040, 1),
            OutputExpression("struct('a' as a, (c || cast(d as string)) as b)")))
        )), // identity should be added automatically
          "view1", Seq.empty, Operation("engine", "view1E", resultApproach), map, "view2")
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

      ir.head._2._2.schema.map(_.name).toSet shouldBe cols
      ir.head._2._2.collect().length shouldBe 1
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
        "c", Set("b"), rulesRaw(Seq(
          (ExpressionRule("true"), NoOpRunOnPassProcessor.noOp)
        )).copy(Id(100,1)), "view3", Seq.empty, Operation(DQRunnerName, "dq", AsIs), Map.empty, "view4")
    ))

    val s = sparkSession
    import com.sparkutils.quality.implicits._
    import s.implicits._
    val res = flow.run(sparkSession, _ => Some(testData.toDF()))("c")._2

    val cols = Set("view1E","view2E","product","account","subcode","dq","flow_audit")
    res.schema.map(_.name).toSet shouldBe cols
    val cres = res.selectExpr("flow_audit.*").as[RuleSuiteGroupResults].collect()
    // all true
    cres.map(r => r.ruleSuiteResults(Id(100,1)).overallResult).exists(r => r != Passed) shouldBe false
  }

  test("lca works - row_number") {

    def rca(wrap: String = "", resultApproach: ResultApproach = OutputFieldsOnly, forceStar: Boolean = false) = {
      val flow = new Flow(Id(1, 1), Seq(
        Step("a", Set.empty, rulesRaw(Seq(
          (ExpressionRule("true"), RunOnPassProcessor(1000, Id(1040, 1),
            OutputExpression("set(d = row_number() OVER (ORDER BY c))")))
        )), // identity should be added automatically
          "view1", Seq.empty, Operation("folder", "view1E", resultApproach), Map(
            wrapInputFields -> wrap,
            forceMergeProjection -> forceStar.toString
          ), "view2")
      ))

      val s = sparkSession
      import s.implicits._

      val data = Seq(
        Tuple2("c", 1)
      ).toDF("c", "d")

      val ir = flow.run(s, _ => Some(data))

      ir.head._2._2.collect().length shouldBe 1
    }

    def testRCA(resultApproach: ResultApproach = OutputFieldsOnly) = {
      val r = intercept[AnalysisException] {
        rca(resultApproach = resultApproach)
      }
      r.getMessage should include("UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS_IN_WINDOW")
      r.getMessage should include("lateralAliasReference(c)")
    }
    testRCA()
    testRCA(resultApproach = MergeFields)
    // force star adds a projection so it's always present
    rca(forceStar = true)
    rca(resultApproach = MergeFields, forceStar = true)

    // The alias is c, so although it's the expression for d AND both c + d are required, only c actually needs it
    rca("c")
    rca("c", MergeFields)
    // for completeness, but don't really add any functional testing
    rca("c", forceStar = true)
    rca("c", MergeFields, forceStar = true)

  }
}
