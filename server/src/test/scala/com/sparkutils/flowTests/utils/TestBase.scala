package com.sparkutils.flowTests.utils

import com.sparkutils.quality
import com.sparkutils.testing.SparkTestUtils.{connectMemory, scoverageClassPathsConfig}
import com.sparkutils.testing.{ConnectionType, SessionsStateHolder, SparkTestSuite, UseBoth}
import com.sparkutils.testing.markers.ConnectSafe
import com.sparkutils.testing.sessionStrategies.{GlobalSession, SharedSessions}
import org.scalatest.{FunSuite, TestSuite}


trait TestSetup extends SparkTestSuite with SharedSessions { self: TestSuite =>

  override def beforeAll(): Unit = {
    // no-op to force it to be created
    forceLoad
    super.beforeAll()

    cleanupOutput()

    withClassicAsActive({
      quality.registerQualityFunctions()
    })
  }

  override def connectServerLoggingLevel = "DEBUG"

  override def loggingLevel: String = "INFO"

  override def sparkConnectServerConfig(): Map[String, String] =
    super.sparkConnectServerConfig() + //useDebugConnectLogs +
      scoverageClassPathsConfig + connectMemory("4g") +
      (("spark.sql.extensions", "com.sparkutils.quality.impl.extension.QualitySparkExtension")) + // text used for connect only tests in dbr
      (("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=connect_metastore_db;create=true")) +
      (("spark.sql.codegen.factoryMode", "NO_CODEGEN")) /*+
      (("spark.sql.queryExecutionListeners", "com.sparkutils.quality.impl.extension.EchoListener"))*/

}

trait SharedPureConnectTests extends FunSuite with TestSetup with ConnectSafe {

  override val currentSessionsHolder: SessionsStateHolder = GlobalSession

  override val runWith: ConnectionType = UseBoth

}