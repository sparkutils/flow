package com.sparkutils.flowTests

import com.sparkutils.testing.TestRunner
import com.sparkutils.testing.TestUtilsEnvironment.setupDefaultsViaCurrentSession

object FlowTestRunner extends TestRunner {

  val packageName: String = "com.sparkutils"

  val projectName: String = "Flow"

  override val classLoader: ClassLoader = classOf[BaseFunctionality].getClassLoader

  // when on fabric or databricks disables cluster tests
  setupDefaultsViaCurrentSession()

  def main(args: Array[String]): Unit = test(args)
}
