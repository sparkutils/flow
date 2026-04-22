package com.sparkutils.flow.impl.util

import com.sparkutils.flow.Step

object FlowExceptionConstants {
  val EmptyFlow = "Empty Flow provided"
  val DuplicateNames= "Flow Steps provided with duplicate names"
  val CycleDetected = "Flow Steps have cyclic dependency"
  val EmptyStepName = "Flow Steps must have a non-null and non-empty name"

  def MissingStep(stepName: String, dependency: String) =
    s"Step ${stepName} refers to a dependency $dependency Step which does not exist"
  def InvalidViewNames(step: Step) =
    s"Step ${step.name} has an empty view name input `(${step.data.inputView})` output `(${step.data.outputView})`"
  def DefaultViewNamesMultipleParents(step: Step) =
    s"Step ${step.name} has an undefined inputView name but has multiple parents `(${step.dependencies.mkString(",")})`"
  def InvalidDQResultApproach(step: Step) =
    s"Step ${step.name} has an invalid resultApproach for DQ Steps `${step.operation.resultApproach}` provided, (MergeFields, OutputFieldsOnly) are not permitted"
  def FlowEarlyExitException(step: Step) =
    s"Step ${step.name} had flowEarlyExitSQL configured which returned false - the Flow will be stopped"

}
