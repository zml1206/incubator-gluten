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

import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan

case class GlutenCost(value: Long, planId: Int) extends Cost {
  override def compare(that: Cost): Int = that match {
    case GlutenCost(thatValue, thatId) =>
      if (value < thatValue || (value == thatValue && planId > thatId)) -1
      else if (value > thatValue) 1
      else 0
    case _ =>
      throw QueryExecutionErrors.cannotCompareCostWithTargetCostError(that.toString)
  }
}

/** This [[CostEvaluator]] is to force use the new physical plan when cost is equal. */
case class GlutenCostEvaluator() extends CostEvaluator {
  override def evaluateCost(plan: SparkPlan): Cost = {
    val simpleCost = SimpleCostEvaluator.evaluateCost(plan).asInstanceOf[SimpleCost]
    GlutenCost(simpleCost.value, plan.id)
  }
}
