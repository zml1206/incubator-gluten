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
package org.apache.gluten.execution

import org.apache.spark.sql.execution.{ProjectExec, SortExec, SparkPlan}

object SortUtils {
  def dropPartialSort(plan: SparkPlan): SparkPlan = plan match {
    case PartialSortChild(p) => p
    case ProjectChild(PartialSortChild(ProjectChild(p))) => p
    case _ => plan
  }

  private object PartialSortChild {
    def unapply(plan: SparkPlan): Option[SparkPlan] = plan match {
      case sort: SortExecTransformer if !sort.global =>
        Some(sort.child)
      case sort: SortExec if !sort.global =>
        Some(sort.child)
      case _ => None
    }
  }

  private object ProjectChild {
    def unapply(plan: SparkPlan): Option[SparkPlan] = plan match {
      case p: ProjectExec => Some(p.child)
      case p: ProjectExecTransformer => Some(p.child)
      case _ => None
    }
  }
}
