package com.sparkutils

import com.sparkutils.quality.{CombinedRuleSuiteRows, RuleSuite}

package object flow extends Serialisation with Constants {

  type Flow = FlowT[CombinedRuleSuiteRows, RuleSuite]
}
