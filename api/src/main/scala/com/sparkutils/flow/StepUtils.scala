package com.sparkutils.flow

import com.sparkutils.flow.impl.util.Utils.MapOps
import com.sparkutils.quality.RuleSuiteParam
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{expr, col => scol}
import org.apache.spark.sql.types.StructType
import com.sparkutils.quality.functions.group_audit

/**
 * Utility functions for processing Steps
 */
object StepUtils {

  // if fields are present, either by default in the folder case or by providing a result type that is not an
  // array we can remove two calls to columns action, 632.73ms vs 554.88ms on using 14 chained engines 1 rule each
  // forceMergeProjection on 200,000 rows

  /**
   * Processes output fields for [[MergeFields]] and [[OutputFieldsOnly]]
   *
   * @param fieldName the field name to be used by the runner column
   * @param runner the runnerColumn for interface completeness
   * @param group_audit the group_audit column
   * @param input the dataframe to add the output fields to
   * @param outputFields a set of output fields which will be mapped to {{{s"$fieldName.result.$n"}}}
   * @param extraFields any extra columns to be added
   * @return
   */
  def withOutputFields(fieldName: String, runner: Column, group_audit: Column, input: DataFrame,
                       outputFields: Set[String], extraFields: Set[Column] = Set.empty,
                       useRunnerColumn: Boolean = false): DataFrame =
    // cannot be run directly on dataFrame as otherwise an lca will be added to any repetitive expressions
    // this stops row number plus another of other queries running correctly in all circumstances
    input.select(
      Seq(if (useRunnerColumn) runner else scol(fieldName), group_audit)
        ++ extraFields ++
        outputFields.map{n =>
          scol(s"$fieldName.result.$n").as(n)
        } :_*)

  /**
   * Ueed by MergeFields and OutputFieldsOnly to add the runner, group_audit, and extra columns and the children of
   * the runner.  This requires at least a single .schema action, but still only processes the remaining data in a single
   * projection.
   *
   * In the MergeFields case extra is the original fields - the children of the runner and requires an additional .schema call.
   *
   *
   * @param fieldName
   * @param runner
   * @param group_audit
   * @param input
   * @param extra
   * @return
   */
  def withoutOutputFields(fieldName: String, runner: Column, group_audit: Column, input: DataFrame,
                          extra: Set[String] => Set[String] = _ => Set.empty): DataFrame = {

    val starter = input.select(runner)
    val childrenNames = starter.schema.fields.head.dataType.asInstanceOf[StructType].
      fields.find(_.name == "result").map(f => f.dataType.asInstanceOf[StructType].
        fields.map(f => f.name).toSet).getOrElse(Set.empty[String])
    val children = childrenNames.map(name => expr(s"$fieldName.result.${name}").as(name))

    input.select(Seq(runner, group_audit) ++ extra(childrenNames).map(scol).toSeq ++ children: _*)

  }

  protected[flow] def resultProcessInputs[RP: RuleSuiteParam](step: Step[RP], runner: Column, childrenRaw: Seq[String],
                                                              outputFieldsI: Option[Set[String]], ei: RunnerInputs,
                                                              flow: FlowT[_,_], starterColumnsForLCA: Boolean = false): ResultProcessInputs = {
    import flow._

    val outputFields: Option[Set[String]] =
      outputFieldsI.flatMap { s =>
        if (step.options.boolean(forceMergeProjection))
          None
        else
          Some(s)
      }.map(_ - flowAuditColName)

    val (starterColumns, existingAudit) =
      // if flowAuditColName is present and the correct type, select all the others, assuming via having a parent step
      // isn't enough if StarOnly or OutputFieldOnly is provided
      //
      (
        if (starterColumnsForLCA)
          Seq(runner)
        else
          Seq(expr("*"), runner),
        ei.hasAudit(flowAuditColName)(
          Seq(scol(flowAuditColName))
        )(
          Seq.empty
        )
      )

    val fieldName = step.defaultFieldName

    val children = childrenRaw.map(n => scol(fieldName).getField(n).as(n))

    val group_auditF =
      group_audit(scol(fieldName),
        step.operation.combineAuditWith.map { fnames =>
          fnames.map { fname => expr(fname) }.toSeq
        }.getOrElse(Seq.empty) ++ existingAudit
          : _*).as(flowAuditColName)

    ResultProcessInputs(outputFields, starterColumns, fieldName, children, group_auditF)
  }

  protected[flow] def runnerInputs[RP: RuleSuiteParam](options: Map[String, String], dataFrame: DataFrame,
                                                       step: Step[RP], flow: FlowT[_,RP]): RunnerInputs = {
    import flow._
    val struct = flow.inputSchema(dataFrame, step)
    val withoutFlowAudit = struct.filterNot(_.name == flowAuditColName).map(_.name)

    val dataRefTypeFields =
      options.dataType(resultDataType) match {
        case Some(s: StructType) => Some(s.fields.map(_.name).filterNot(_ == flowAuditColName).toSet)
        case _ => None
      }
    RunnerInputs(struct, withoutFlowAudit, dataRefTypeFields)
  }

  /**
   * For cases which require a pre-resolved field such as LCA and row_number another projection is required.  It
   * is always a correct result
   */
  protected[flow] def processProjectionResult[RP: RuleSuiteParam](dataFrame: DataFrame, step: Step[RP], engine: RunnerOutput,
                                                  flow: FlowT[_,RP]): DataFrame = {
    import flow._
    import step.operation._
    import step.options

    val RunnerOutput(runner, childrenRaw, outputFieldsI) = engine

    val ei@RunnerInputs(struct, withoutFlowAudit, _) = runnerInputs(options, dataFrame, step, flow)

    val ri@ResultProcessInputs(outputFields, starterColumns, fieldName, children, group_auditF) =
      resultProcessInputs(step, runner, childrenRaw, outputFieldsI, ei, flow)

    resultApproach match {
      case AsIs =>
        dataFrame.select(starterColumns: _*).select(
          ei.hasAudit(flowAuditColName)(
            withoutFlowAudit.map(scol) ++ Seq(scol(fieldName), group_auditF)
          )(
            Seq(expr("*"), group_auditF)
          ): _*)

      case MergeFields =>

        val starter = dataFrame.select(starterColumns: _*)
        outputFields.fold {

          val og = starter.columns.toSet
          val nested = starter.selectExpr(s"$fieldName.result.*").columns
          starter.select((og -- nested).map(scol).toSeq ++ Seq(expr(s"$fieldName.result.*"), group_auditF): _*)

        } { outputFields =>

          val fields = struct.map(_.name).toSet -- outputFields - flowAuditColName
          withOutputFields(fieldName, runner, group_auditF, starter, outputFields, extraFields = fields.map(scol))

        }

      case OutputFieldsOnly =>

        val starter = dataFrame.select(starterColumns: _*)

        outputFields.fold {

          val og = dataFrame.columns.toSet
          val startCols = starter.columns.toSet
          starter.select((og -- startCols).map(scol).toSeq ++
            Seq(scol(fieldName), group_auditF, expr(s"$fieldName.result.*")): _*)

        } { o =>
          withOutputFields(fieldName, runner, group_auditF, starter, outputFields = o)
        }

      case StarOnly => dataFrame.select(runner).select(children: _*)
      case OutputFieldOnly => dataFrame.select(runner)
      case c: CustomApproach => c.customResultApproach(dataFrame, runner, ei, ri, step)
    }
  }


  /**
   * For cases which require a pre-resolved field such as LCA and row_number another projection is required.  It
   * is always a correct result
   */
  protected[flow] def processResult[RP: RuleSuiteParam](dataFrame: DataFrame, step: Step[RP], engine: RunnerOutput,
                                                                  flow: FlowT[_,RP]): DataFrame = {
    import flow._
    import step.operation._
    import step.options

    val RunnerOutput(runner, childrenRaw, outputFieldsI) = engine

    val ei@RunnerInputs(struct, withoutFlowAudit, _) = runnerInputs(options, dataFrame, step, flow)

    val ri@ResultProcessInputs(outputFields, starterColumns, fieldName, children, group_auditF) =
      resultProcessInputs(step, runner, childrenRaw, outputFieldsI, ei, flow, starterColumnsForLCA = true)

    resultApproach match {
      case AsIs =>
        dataFrame.select(starterColumns ++
          ei.hasAudit(flowAuditColName)(
            withoutFlowAudit.map(scol) ++ Seq(group_auditF)
          )(
            Seq(expr("*"), group_auditF)
          ): _*)

      case MergeFields =>

        outputFields.fold {

          withoutOutputFields(fieldName, runner, group_auditF, dataFrame, extra = childrenNames => {
            val og = dataFrame.columns.toSet
            og -- childrenNames // only remove the result fields
          })

        } { outputFields =>

          val fields = struct.map(_.name).toSet -- outputFields - flowAuditColName
          withOutputFields(fieldName, runner, group_auditF, dataFrame, outputFields, extraFields = fields.map(scol),
            useRunnerColumn = true)

        }

      case OutputFieldsOnly =>

        outputFields.fold {

          withoutOutputFields(fieldName, runner, group_auditF, dataFrame)

        } { o =>
          withOutputFields(fieldName, runner, group_auditF, dataFrame, outputFields = o,
            useRunnerColumn = true)
        }

      case StarOnly => dataFrame.select(Seq(runner) ++ children: _*)
      case OutputFieldOnly => dataFrame.select(runner)
      case c: CustomApproach => c.customResultApproach(dataFrame, runner, ei, ri, step)
    }
  }
}
