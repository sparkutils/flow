package com.sparkutils

import com.sparkutils.quality.CombinedRuleSuiteRows

package object flow extends Serialisation with Constants {

  type Flow = FlowT[CombinedRuleSuiteRows]
}
