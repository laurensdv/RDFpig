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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;
import org.openrdf.query.algebra.TupleExpr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.larkc.RDFPig.Configuration;
import eu.larkc.RDFPig.costCalculators.CostCalculator;
import eu.larkc.RDFPig.optimizers.TupleCostPair;
import eu.larkc.RDFPig.pig.Executor;

/**
 * Dynamic programming-like optimization strategy. For each expression, 
 * it will sample the most promising joins and keep the ones with the lowest cardinality.
 * @author spyros
 *
 */
public class SamplePruneOptimizationStrategy implements
		OptimizationStrategy {

	protected static Logger logger = LoggerFactory.getLogger(SamplePruneOptimizationStrategy.class);
	private StandardDeviation stdDev=new StandardDeviation();
	
	@Override
	public void prune(ArrayList<Set<TupleCostPair>> searchSpace,
			Executor executor, CostCalculator calculator)  {
		
		// Only sample up to MAXSAMPLINGCYCLES steps
		int maxSamplingCycles=Configuration.getInstance().getPropertyInt(Configuration.MAXSAMPLINGCYCLES);
		if (searchSpace.size() > maxSamplingCycles) {
			return;
		}
		
		logger.info("Calling SamplePrune for cycle: " + searchSpace.size());
		
		// Sort last calculated expressions by cost
		Set<TupleCostPair> currentLevel = searchSpace.get(searchSpace.size()-1);
		
		if (currentLevel.isEmpty()) {
			logger.warn("Empty current level, searchSpace sizes: ");
			for (Set<TupleCostPair> s: searchSpace) {
				logger.warn(Integer.toString(s.size()));
			}
			return;
		}
		
		List<TupleCostPair> sortedByCost = new ArrayList<TupleCostPair>(currentLevel);
		Collections.sort(sortedByCost, 
	    new Comparator<TupleCostPair>(){
			@Override
			public int compare(TupleCostPair o1, TupleCostPair o2) {
				if (o1.cost==o2.cost)
					return 0;
				else 
					return o1.cost>o2.cost?1:-1;
			}			
		});
		
		// TODO adapt pruning to stddev?
		
		// Prune the bottom part
		double staticPruneRatio = Configuration.getInstance().getPropertyDouble(Configuration.DYNAMICPROGRAMMINGSTATICPRUNEBEFORE);
		int staticPruneIndex = (int)Math.ceil(sortedByCost.size()/staticPruneRatio);
		if (staticPruneIndex==sortedByCost.size())
			staticPruneIndex--;
		sortedByCost=sortedByCost.subList(0, staticPruneIndex);
		
		// Sample the remaining ones
		Set<TupleExpr> candidates=new HashSet<TupleExpr>();
		for (TupleCostPair t:sortedByCost) {
			candidates.add(t.tuple);
		}
		try {
			executor.execute(candidates, Configuration.getInstance().getPropertyDouble(Configuration.SAMPLE_RATE));
		} catch (Exception e) {
			throw new RuntimeException("Could not sample",e);
		}
		
		// Update costs and remove the unsampled ones from the search space
		Iterator<TupleCostPair> it=currentLevel.iterator();
		while (it.hasNext()) {
			TupleCostPair c=it.next();
			if (candidates.contains(c.tuple))
				c.cost=calculator.getCost(c.tuple);
			else
				it.remove();
		}
		
		logger.info("Sample-Prune done for level " +(searchSpace.size()-1) + " current level size = " +currentLevel.size());
		
	}

}
