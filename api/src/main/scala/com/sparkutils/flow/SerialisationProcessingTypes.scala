package com.sparkutils.flow

import com.sparkutils.flow.impl.util.Utils.{MapOps, defaultCombinedFolder, defaultFolder}
import com.sparkutils.quality
import com.sparkutils.quality.{CombinedRuleSuiteRows, GroupRuleId, Id, RuleSuite, VersionedId, combined_rows, register_rule_suite_group_variable, rule_suite, rule_suite_group}
import org.apache.spark.sql.{Dataset, ShimUtils}
import org.apache.spark.sql.functions.{col, lit, struct}

import scala.util.Try

/**
 * Which types are allowed to be stored in [[FullStep]]
 * instance exists.
 */
sealed trait RuleSuiteStorageType[T] {
  def id(t: T): Id
}

object RuleSuiteStorageType {

  implicit val rsParam: RuleSuiteStorageType[CombinedRuleSuiteRows] = new RuleSuiteStorageType[CombinedRuleSuiteRows] {
    override def id(t: CombinedRuleSuiteRows): Id = Id(t.ruleSuiteId, t.ruleSuiteVersion)
  }

  implicit val idNameParam: RuleSuiteStorageType[Id] = new RuleSuiteStorageType[Id] {
    override def id(t: Id): Id = t
  }

}

/**
 * When the FG type is Id it filters all entries in the [[FlowRuleGroup.ruleGroup]] against the provided dataset
 * and registers them with the provided [[FlowRuleGroup.ruleGroupName]].
 *
 * Conversely, when the type is CombinedRuleSuiteRows, they are loaded directly to [[FlowRuleGroup.ruleGroupName]]
 *
 * @tparam FG
 */
sealed trait FlowRuleGroupProcessing[FG] {
  def load(stepIds: Map[Id, StepRow], datasetRows: Dataset[CombinedRuleSuiteRows], flowRuleGroup: FlowRuleGroup[FG]): Unit

  def combinedRuleSuiteRows(flowRuleGroup: FlowRuleGroup[FG]): Seq[CombinedRuleSuiteRows] = Seq.empty
}

object FlowRuleGroupProcessing {

  implicit val ofIds: FlowRuleGroupProcessing[Id] = new FlowRuleGroupProcessing[Id] {
    override def load(stepIds: Map[Id, StepRow], rsRows: Dataset[CombinedRuleSuiteRows], flowRuleGroup: FlowRuleGroup[Id]): Unit = {
      val filtered =
        if (flowRuleGroup.ruleGroup.isEmpty)
          rsRows
        else
          rsRows.filter(
            ShimUtils.callFunction("in", Seq(struct(col("ruleSuiteId"), col("ruleSuiteVersion"))) ++
              flowRuleGroup.ruleGroup.map(id => struct(lit(id.id), lit(id.version))): _*
            )
          )

      register_rule_suite_group_variable(filtered, flowRuleGroup.ruleGroupName)
    }
  }

  implicit val ofCombinedRows: FlowRuleGroupProcessing[CombinedRuleSuiteRows] = new FlowRuleGroupProcessing[CombinedRuleSuiteRows] {
    override def load(stepIds: Map[Id, StepRow], rsRows: Dataset[CombinedRuleSuiteRows], flowRuleGroup: FlowRuleGroup[CombinedRuleSuiteRows]): Unit = {
      val s = rsRows.sparkSession
      import s.implicits._

      val rg = flowRuleGroup.ruleGroup.map(defaultCombinedFolder(stepIds))
      register_rule_suite_group_variable(rg.toDS(), flowRuleGroup.ruleGroupName)
    }

    override def combinedRuleSuiteRows(flowRuleGroup: FlowRuleGroup[CombinedRuleSuiteRows]): Seq[CombinedRuleSuiteRows] =
      flowRuleGroup.ruleGroup
  }

}


/**
 * How to encode an [[Operation.ruleSuite]], similar to [[com.sparkutils.quality.RuleSuiteParam]] but no Column
 * instance exists.
 */
sealed trait OperationProcessing {
  type ResType
  type StorageType
}

sealed trait StorageIsCombinedRows extends OperationProcessing {
  override type StorageType = CombinedRuleSuiteRows
}

/**
 * Loads RuleSuites from [[FlowDataSets.combined]], and directly from the [[FullStep.ruleSuite]] when calling [[fromFullFlow]]
 */
object RuleSuiteFromDataset extends StorageIsCombinedRows {
  override type ResType = RuleSuite
}

/**
 * Where possible attempt to load RuleSuites from the [[FlowRow.flowRuleGroup]], falling back to [[FlowDataSets.combined]]
 */
object RuleSuiteFromFlows extends StorageIsCombinedRows {
  override type ResType = RuleSuite
}

/**
 * Attempts to create references to the [[FlowRow.flowRuleGroup]], falling back to [[FlowDataSets.combined]].
 */
object IdFromFlows extends OperationProcessing {
  override type ResType = GroupRuleId
  override type StorageType = Id
}

/**
 * Creates references to the [[FlowDataSets.combined]] or from [[FullFlow.flowRow.flowRuleGroup]] when calling [[fromFullFlow]]
 */
object IdFromDatasets extends OperationProcessing {
  override type ResType = GroupRuleId
  override type StorageType = Id
}

sealed trait FlowGroupAndOperationType[F, O <: OperationProcessing] {
  def fromDataset(flowId: VersionedId, rsName: String, rsRows: Dataset[CombinedRuleSuiteRows],
            rid: Id, flowRow: FlowRow[F], step: StepRow): O#ResType

  def writableRuleSuite(rs: O#ResType): O#StorageType

  def fromFullRow(rules: Seq[CombinedRuleSuiteRows], variableName: String, flowRow: FlowRow[F], fullStep: FullStep[O#StorageType], rid: Id,
                  s: O#StorageType): O#ResType

  def toCombinedRows(rs: O#StorageType): Option[CombinedRuleSuiteRows] = None

  def toRuleSuite(flow: FlowT[F, O#ResType], rs: O#ResType): Option[ RuleSuite ]

}

object FlowGroupAndOperationType {
  trait WritableRuleSuite[F] {
    def writableRuleSuite(rs: RuleSuite): CombinedRuleSuiteRows = combined_rows(rs).head()

    //def toRuleSuite(rs: RuleSuite): RuleSuite = rs
    def toRuleSuite(flow: FlowT[F, RuleSuite], rs: RuleSuite): Option[RuleSuite] = Some(rs)
  }

  trait WritableId {
    def writableRuleSuite(rs: GroupRuleId): Id = rs.id
  }

  trait HasCombinedRows[F, O <: StorageIsCombinedRows] extends FlowGroupAndOperationType[F, O] {
    override def toCombinedRows(rs: CombinedRuleSuiteRows): Option[CombinedRuleSuiteRows] = Some(rs)
  }

  implicit val idsAndRuleSuite: FlowGroupAndOperationType[Id, RuleSuiteFromDataset.type] with WritableRuleSuite[Id]  =
    new HasCombinedRows[Id, RuleSuiteFromDataset.type] with WritableRuleSuite[Id] {

      override def fromDataset(flowId: VersionedId, rsName: String, rsRows: Dataset[CombinedRuleSuiteRows],
                         rid: Id, flowRow: FlowRow[Id], step: StepRow): RuleSuite =
        rule_suite(rsRows, rid).getOrElse(
          throw FlowException(s"Flow $flowId could not be loaded as a Step's RuleSuite $rid is missing")
        )

      override def fromFullRow(rules: Seq[CombinedRuleSuiteRows], variableName: String, flowRow: FlowRow[Id], fullStep: FullStep[CombinedRuleSuiteRows], rid: Id,
                               s: CombinedRuleSuiteRows): RuleSuite =
        defaultFolder(fullStep.step, rule_suite_group(Seq(s)).ruleSuites.values.head)

    }

  implicit val idsAndId: FlowGroupAndOperationType[Id, IdFromDatasets.type] =
    new FlowGroupAndOperationType[Id, IdFromDatasets.type] with WritableId {

      override def fromDataset(flowId: VersionedId, rsName: String, rsRows: Dataset[CombinedRuleSuiteRows],
                         rid: Id, flowRow: FlowRow[Id], step: StepRow): GroupRuleId =
        GroupRuleId(rsName, rid)

      override def fromFullRow(rules: Seq[CombinedRuleSuiteRows], variableName: String,
                               flowRow: FlowRow[Id], fullStep: FullStep[Id], rid: Id, s: Id): GroupRuleId =
        GroupRuleId(variableName, s)

      override def toRuleSuite(flow: FlowT[Id, GroupRuleId], rs: GroupRuleId): Option[RuleSuite] = None
    }

  implicit val rowsAndFullSuites: FlowGroupAndOperationType[CombinedRuleSuiteRows, RuleSuiteFromDataset.type] with
    WritableRuleSuite[CombinedRuleSuiteRows] =
    new HasCombinedRows[CombinedRuleSuiteRows, RuleSuiteFromDataset.type] with WritableRuleSuite[CombinedRuleSuiteRows] {

      override def fromDataset(flowId: VersionedId, rsName: String, rsRows: Dataset[CombinedRuleSuiteRows],
                         rid: Id, flowRow: FlowRow[CombinedRuleSuiteRows], step: StepRow): RuleSuite =
        rule_suite(rsRows, rid).getOrElse(
          flowRow.flowRuleGroup.flatMap(g =>
            g.ruleGroup.find(r => Id(r.ruleSuiteId, r.ruleSuiteVersion) == rid).map(rule_suite)).getOrElse(
            throw FlowException(s"Flow $flowId could not be loaded as a Step's RuleSuite $rid is missing")
          )
        )

      override def fromFullRow(rules: Seq[CombinedRuleSuiteRows], variableName: String,
                               flowRow: FlowRow[CombinedRuleSuiteRows], fullStep: FullStep[CombinedRuleSuiteRows],
                               rid: Id, s: CombinedRuleSuiteRows): RuleSuite =
        defaultFolder(fullStep.step, rule_suite_group(Seq(s)).ruleSuites.values.head)

    }

  implicit val rowsAndFullSuitesFromFlows: FlowGroupAndOperationType[CombinedRuleSuiteRows, RuleSuiteFromFlows.type] with
    WritableRuleSuite[CombinedRuleSuiteRows]  =
    new HasCombinedRows[CombinedRuleSuiteRows, RuleSuiteFromFlows.type] with WritableRuleSuite[CombinedRuleSuiteRows] {

      override def fromDataset(flowId: VersionedId, rsName: String, rsRows: Dataset[CombinedRuleSuiteRows],
                         rid: Id, flowRow: FlowRow[CombinedRuleSuiteRows], step: StepRow): RuleSuite = {
        val s = rsRows.sparkSession
        import s.implicits._
        rule_suite(flowRow.flowRuleGroup.map(_.ruleGroup.toDS()).getOrElse(rsRows), rid).getOrElse(
          throw FlowException(s"Flow $flowId could not be loaded as a Step's RuleSuite $rid is missing")
        )
      }

      override def fromFullRow(rules: Seq[CombinedRuleSuiteRows], variableName: String,
                               flowRow: FlowRow[CombinedRuleSuiteRows], fullStep: FullStep[CombinedRuleSuiteRows],
                               rid: Id, s: CombinedRuleSuiteRows): RuleSuite = rule_suite(s)

    }

  trait RuleSuiteFromGroup {

    def toRuleSuite(flow: FlowT[CombinedRuleSuiteRows, GroupRuleId], rs: GroupRuleId): Option[RuleSuite] =
      flow.flowRuleGroup.flatMap(g =>
        g.ruleGroup.find(r => Id(r.ruleSuiteId, r.ruleSuiteVersion) == rs.id).map(rule_suite)
      )
  }


  implicit val rowsAndIdsFromDatasets: FlowGroupAndOperationType[CombinedRuleSuiteRows, IdFromDatasets.type] =
    new FlowGroupAndOperationType[CombinedRuleSuiteRows, IdFromDatasets.type] with WritableId with RuleSuiteFromGroup {

      override def fromDataset(flowId: VersionedId, rsName: String, rsRows: Dataset[CombinedRuleSuiteRows],
                         rid: Id, flowRow: FlowRow[CombinedRuleSuiteRows], step: StepRow): GroupRuleId =
        GroupRuleId(rsName, rid)

      def fromFullRow(rules: Seq[CombinedRuleSuiteRows], variableName: String, flowRow: FlowRow[CombinedRuleSuiteRows],
                      fullStep: FullStep[Id], rid: Id, s: Id): GroupRuleId =
        GroupRuleId(variableName, s)

    }

  implicit val rowsAndIdsFromFlows: FlowGroupAndOperationType[CombinedRuleSuiteRows, IdFromFlows.type] =
    new FlowGroupAndOperationType[CombinedRuleSuiteRows, IdFromFlows.type] with WritableId with RuleSuiteFromGroup  {

      override def fromDataset(flowId: VersionedId, rsName: String, rsRows: Dataset[CombinedRuleSuiteRows],
                         rid: Id, flowRow: FlowRow[CombinedRuleSuiteRows], step: StepRow): GroupRuleId = {

        val useFlow = step.options.boolean(useFlowRuleGroupLevelRows, true)

        val name =
          step.options.getOrElse(useRuleGroupName,
            if (useFlow)
              flowRow.flowRuleGroup.map(_.ruleGroupName).getOrElse(rsName)
            else
              rsName
          )

        GroupRuleId(name, rid)
      }

      def fromFullRow(rules: Seq[CombinedRuleSuiteRows], variableName: String, flowRow: FlowRow[CombinedRuleSuiteRows],
                      fullStep: FullStep[Id], rid: Id, s: Id): GroupRuleId =
        flowRow.flowRuleGroup.collect{
          case g  if g.ruleGroup.exists(c => Id(c.ruleSuiteId, c.ruleSuiteVersion) == s) =>
              GroupRuleId(g.ruleGroupName, s)
        }.getOrElse{
          GroupRuleId(variableName, s)
        }
    }
}
