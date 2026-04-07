package com.sparkutils.flowTests.manual

import com.sparkutils.flow.{Flow, MergeFields, Operation, Step}
import com.sparkutils.flowTests.RulesGen.rulesRaw
import com.sparkutils.flowTests.utils.{SharedPureConnectTests, TestSetup}
import com.sparkutils.quality
import com.sparkutils.quality.impl.CollectRunner
import com.sparkutils.quality.{DefaultProcessor, ExpressionRule, Id, OutputExpression, Rule, RuleSet, RuleSuite, RunOnPassProcessor}
import com.sparkutils.testing.{ClassicOnly, ConnectionType, Sessions, TestUtils}
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{ArrayType, LongType}
import org.apache.spark.sql.{Column, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.scalameter.api.{Bench, _}

import scala.concurrent.ExecutionContext.Implicits.global

object Args {
  val args = List(
    "-Xmx10g","-Xms10g",// 16GB on github runners, 10gb ok on 21 (12 blows), 12gb fine on jdk 8.
    "-ea",
    "-XX:+IgnoreUnrecognizedVMOptions",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
  )
}

object CombineAuditDataSetup extends SharedPureConnectTests {
  val partitions = 8

  override val runWith: ConnectionType = ClassicOnly

  override def sessions: Sessions = createSparkSessions(connectionType)
}

object CombineAuditBenchmark extends Bench.OfflineReport with TestUtils {

  override def loggingLevel: String = "ERROR"
/*
  override val currentSessionsHolder: SessionsStateHolder = GlobalSession

  override val runWith: ConnectionType = ClassicOnly
*/
  // below is enough on Spark 3.5 to force compilation
  def df(n: Int) = {
    val s = sparkSession
    quality.registerQualityFunctions()

    s.range(n).selectExpr("id as lower", "id+5 as higher").repartition(CombineAuditDataSetup.partitions).persist(StorageLevel.MEMORY_ONLY)
  } //sparkSession.read.parquet(outputDir + "/collectTestData")

  def evaluate[T](alternate: Boolean)(params: (Int, Int)) = {
    new Flow(Id(1,1), Seq(
      Step("0", Set.empty, rulesRaw(Seq(
        (ExpressionRule("(lower % 2) = 0"),  RunOnPassProcessor(1000, Id(1040, 1),
          OutputExpression("set(higher = lower + 10)")))
      )), "view1", Seq.empty, Operation("folder", "view1E", Map.empty, MergeFields), Map.empty, "view2")) ++
      stepsGen(params._1)).
      run(sparkSession, _ => Some(df(params._2))).
      head._2._2.write.format("noop").mode(Overwrite).save()
  }

  def stepsGen(size: Int) =
    for{i <- 1 until size} yield
      Step(s"$i", Set.empty, rulesRaw(Seq(
        (ExpressionRule("(higher % 5) = 0"),  RunOnPassProcessor(1000, Id(1040, 1),
          OutputExpression("set(lower = lower - 3)")))
      )).copy(id = Id(i + 1,0)),// Id change to verify combineAuditWith
        s"view${i+1}", Seq.empty, Operation("folder", s"view${i+1}E", Map.empty, MergeFields), Map.empty, s"view${i+2}")

  val rows = Gen.range("rows")(200000, 200000, 25000)
  val stepsCount = Gen.range("steps")(14, 14, 1)
  val generator = Gen.crossProduct(stepsCount, rows)

  performance of "Processing Array Collection Transformations" config (
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.jvmcmd -> (System.getProperty("java.home")+"/bin/java"),
    exec.jvmflags -> Args.args
    //  verbose -> true
  ) in {

    measure method "flow default" in {
      val s = sparkSession
      sparkSession.sparkContext.setLogLevel(loggingLevel) // set to debug to get actual code lines etc.

      using(generator) in evaluate(false)
    }

    measure method "flow alternate" in {
      val s = sparkSession
      sparkSession.sparkContext.setLogLevel(loggingLevel) // set to debug to get actual code lines etc.

      using(generator) in evaluate(true)
    }
  }

//  override def sparkSession: SparkSession = classicSparkSession.get

  override def sessions: Sessions = createSparkSessions(ClassicOnly)
}
