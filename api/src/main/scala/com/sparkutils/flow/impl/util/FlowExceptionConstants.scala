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
    s"Step ${step.name} has a missing or empty view name input `(`${step.inputView}` output `(`${step.outputView}`"
}
