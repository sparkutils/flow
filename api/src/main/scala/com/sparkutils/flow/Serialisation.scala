package com.sparkutils.flow

import com.sparkutils.quality.VersionedId
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.col

@SerialVersionUID(1L)
case class StepRow(ruleSuiteId: VersionedId, inputView: String, operation: Operation,
                   properties: Map[String, String], outputView: String, combineAuditWith: Option[String] = None)

@SerialVersionUID(1L)
case class FlowRow(ruleSuiteGroupId: VersionedId, flowAuditColName: String)

trait Serialisation {

  def loadSteps(dataFrame: DataFrame, ruleSuiteId: Column, ruleSuiteVersion: Column, inputView: Column, operation: Column,
                properties: Column, outputView: Column, combineAuditWith: Column): Dataset[StepRow] = {
    import frameless._
    import implicits._
    import com.sparkutils.quality.implicits._

    implicit val typed = TypedExpressionEncoder[StepRow]

    dataFrame.select(
      ruleSuiteId.as("ruleSuiteId"), ruleSuiteVersion.as("ruleSuiteVersion"), inputView.as("inputView"), operation.as("operation"),
      properties.as("properties"), outputView.as("outputView"), combineAuditWith.as("combineAuditWith")
    ).as[StepRow]
  }

  def loadSteps(dataFrame: DataFrame): Dataset[StepRow] =
    loadSteps(dataFrame, col("ruleSuiteId"), col("ruleSuiteVersion"), col("inputView"), col("operation"),
      col("properties"), col("outputView"), col("combineAuditWith"))

}