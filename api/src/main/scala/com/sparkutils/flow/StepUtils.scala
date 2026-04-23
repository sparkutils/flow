package com.sparkutils.flow

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col => scol}

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
   * @param runner the runnerColumn for interface completeness - this is reserved for future use
   * @param group_audit the group_audit column
   * @param input the dataframe to add the output fields to
   * @param outputFields a set of output fields which will be mapped to {{{s"$fieldName.result.$n"}}}
   * @param extraFields any extra columns to be added
   * @return
   */
  def withOutputFields(fieldName: String, runner: Column, group_audit: Column, input: DataFrame, outputFields: Set[String], extraFields: Set[Column] = Set.empty) =
    // cannot be run directly on dataFrame as otherwise an lca will be added to any repetitive expressions
    // this stops row number plus another of other queries running correctly in all circumstances
    input.select(
      Seq(scol(fieldName), group_audit)
        ++ extraFields ++
        outputFields.map{n =>
          scol(s"$fieldName.result.$n").as(n)
        } :_*)

}
