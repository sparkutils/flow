package com.sparkutils.flow.impl.util

import com.sparkutils.flow.Step

object FlowExceptionConstants {
  val EmptyFlow = "Empty Flow provided"
  val DuplicateNames= "Flow Steps provided with duplicate names"
  val CycleDetected = "Flow Steps have cyclic dependency"
  val EmptyStepName = "Flow Steps must have a non-null and non-empty name"

  def MissingStep(stepName: String, dependency: String) =
    s"Step ${stepName} refers to a dependency $dependency Step which does not exist"
  def InvalidViewNames[RP](step: Step[RP]) =
    s"Step ${step.name} has an empty view name input `(${step.data.inputView})` output `(${step.data.outputView})`"
  def DefaultViewNamesMultipleParents[RP](step: Step[RP]) =
    s"Step ${step.name} has an undefined inputView name but has multiple parents `(${step.dependencies.mkString(",")})`"
  def InvalidDQResultApproach[RP](step: Step[RP]) =
    s"Step ${step.name} has an invalid resultApproach for DQ Steps `${step.operation.resultApproach}` provided, (MergeFields, OutputFieldsOnly) are not permitted"
  def FlowEarlyExitException[RP](step: Step[RP]): String =
    FlowEarlyExitException(step.name)
  def FlowEarlyExitException(stepName: String): String =
    s"Step stepName had flowEarlyExitSQL configured which returned false - the Flow will be stopped"

  def MissingPersistorParameters(step: Step[_], clazz: Class[_], params: Seq[String]): String =
    s"Step ${step.name} could not use Persistor ${clazz.getName} as it had missing required parameters (${params.mkString(",")})"
}
