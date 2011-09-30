/*
 * Copyright 2011 LarKC project consortium
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package eu.larkc.RDFPig.optimizers.strategies;

import java.util.ArrayList;
import java.util.Set;

import eu.larkc.RDFPig.costCalculators.CostCalculator;
import eu.larkc.RDFPig.optimizers.TupleCostPair;
import eu.larkc.RDFPig.pig.Executor;

/**
 * Simple optimization strategy. Does not prune search space and does not do sampling. Do not use for large or complicated queries.
 * @author spyros
 *
 */
public class NoOptimizationStrategy implements OptimizationStrategy {
	@Override
	public void prune(ArrayList<Set<TupleCostPair>> searchSpace,
			Executor executor, CostCalculator calculator) {
		return;
	}

}
