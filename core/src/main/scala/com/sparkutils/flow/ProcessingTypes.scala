package com.sparkutils.flow

import com.sparkutils.quality.impl.Encoders
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StructType

/**
 * Inputs provided to runners derived from both the input dataset and any present [[Step.options]] [[resultDataType]] entry
 * @param struct
 * @param withoutFlowAudit
 * @param dataRefTypeFields
 */
@SerialVersionUID(1L)
case class RunnerInputs(struct: StructType, withoutFlowAudit: Seq[String], dataRefTypeFields: Option[Set[String]]) extends Serializable {

  /**
   * Similar to an option.fold, uses the struct information to verify if an audit column is present for a given Flow's Step
   *
   * @param flowAuditColName provided by the Flow
   * @param auditF called when an audit field is present
   * @param noAudit called when no audit field is present
   * @tparam T
   * @return
   */
  def hasAudit[T](flowAuditColName: String)(auditF: => T)(noAudit: => T): T =
    if (struct.exists(s => s.name == flowAuditColName && s.dataType == Encoders.ruleSuiteGroupResultsTypedEnc.catalystRepr))
      auditF
    else
      noAudit

}

/**
 * Returned by runners specified by [[Operation.function]]
 *
 * @param column         The column containing the runner output
 * @param runnerChildren Direct children of the runner (e.g. result, ruleSuiteResults etc.)
 * @param outputFields   Any declared output, e.g. typically just [[RunnerInputs.dataRefTypeFields]]
 */
@SerialVersionUID(1L)
case class RunnerOutput(column: Column, runnerChildren: Seq[String], outputFields: Option[Set[String]]) extends Serializable

/**
 * Represents the processing inputs needed to handle an engines results
 * @param outputFields
 * @param starterColumns
 * @param fieldName
 * @param children
 * @param group_auditF
 */
@SerialVersionUID(1L)
case class ResultProcessInputs(outputFields: Option[Set[String]], starterColumns: Seq[Column],
                               fieldName: String, children: Seq[Column], group_auditF: Column) extends Serializable
