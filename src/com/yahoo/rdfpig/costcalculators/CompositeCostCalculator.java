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
package com.yahoo.rdfpig.costcalculators;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;

public class CompositeCostCalculator implements CostCalculator {

	private Map<CostCalculator, Double> calculators;
	
	public CompositeCostCalculator(Map<CostCalculator, Double> calculators) {
		this.calculators=calculators;
	}
	
	@Override
	public void setInputSize(long inputSize) {
		for (CostCalculator c: calculators.keySet())
			c.setInputSize(inputSize);
	}

	@Override
	public void prepareEstimation(List<? extends StatementPattern> stPatterns, long sizeInput)
			throws IOException {
		for (CostCalculator c: calculators.keySet())
			c.prepareEstimation(stPatterns, sizeInput);
	}

	@Override
	public double getCost(TupleExpr sp) {
		double cost=0;
		for (Map.Entry<CostCalculator, Double> e : calculators.entrySet()) {
			cost+=e.getKey().getCost(sp)*e.getValue();
		}
		return cost;
	}
}
