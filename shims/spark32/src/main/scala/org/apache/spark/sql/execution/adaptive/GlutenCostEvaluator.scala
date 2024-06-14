/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.ShuffledJoin

/**
 * This [[CostEvaluator]] is to force use the new physical plan when AQE's new physical plan
 * contains [[ShuffledJoin]] and cost is the same.
 */
case class GlutenCostEvaluator() extends CostEvaluator {
  private var isReOptimize = false
  override def evaluateCost(plan: SparkPlan): Cost = {
    val simpleCost =
      SimpleCostEvaluator.evaluateCost(plan).asInstanceOf[SimpleCost]
    var cost = simpleCost.value * 2
    if (isReOptimize && existsShuffledJoin(plan)) {
      cost -= 1
    }
    isReOptimize = !isReOptimize
    SimpleCost(cost)
  }

  private def existsShuffledJoin(plan: SparkPlan): Boolean = {
    if (plan.isInstanceOf[ShuffledJoin]) {
      true
    } else {
      plan.children.exists(existsShuffledJoin)
    }
  }
}
