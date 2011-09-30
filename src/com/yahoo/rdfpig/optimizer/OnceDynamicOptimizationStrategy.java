/*
 * Copyright 2011 Yahoo! Inc.
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
package com.yahoo.rdfpig.optimizer;


import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;
import org.openrdf.query.algebra.TupleExpr;

import com.yahoo.rdfpig.costcalculators.SamplingCardinalityCalculator;
import com.yahoo.rdfpig.optimizer.EfficientJoinOptimizer.SearchPath;

/**
 * Simple dynamic optimization strategy that just evaluates the NUMBEROFPLANSPERBATCH cheapest sub-plans.
 * @author spyros
 *
 */
public class OnceDynamicOptimizationStrategy implements DynamicOptimizationStrategy {

	boolean firstTime=true;
	private StandardDeviation stdDev=new StandardDeviation();
	
	/* (non-Javadoc)
	 * @see eu.larkc.RDFPig.optimizers.DynamicOptimizationStrategy#checkDynamicOptimizationDone(org.openrdf.query.algebra.TupleExpr)
	 */
	@Override
	public boolean checkDynamicOptimizationDone(TreeSet<SearchPath> candidates){
		if (firstTime) {
			firstTime=false;
			return false;
		}
		else
			return true;
		
	}

	/* (non-Javadoc)
	 * @see eu.larkc.RDFPig.optimizers.DynamicOptimizationStrategy#selectPlans(java.util.TreeSet, eu.larkc.RDFPig.costCalculators.AbstractCostCalculator)
	 */
	@Override
	public Set<TupleExpr> selectPlans(TreeSet<SearchPath> candidates, SamplingCardinalityCalculator calculator) {
		HashSet<TupleExpr> ret=new HashSet<TupleExpr>();
		
		double[] costs=new double[candidates.size()];
		int i=0;
		for (SearchPath p:candidates)
			costs[i++]=p.cost;
		
		double dev=stdDev.evaluate(costs);
		
		int plansPerSubgraph=(int) dev+1;
		
		i=0;
		for (SearchPath p:candidates) {
			if (!calculator.isCalculated(p.current) && !calculator.isSampled(p.current)) {
				ret.add(p.current);
				
				if (i++>plansPerSubgraph)
					break;
			}
		}
		return ret;
	}
}
