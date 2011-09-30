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
package eu.larkc.RDFPig.costCalculators;

import java.util.List;

import org.openrdf.query.algebra.StatementPattern;

import eu.larkc.RDFPig.pig.Executor;

/**
 * Returns fake cardinalities based on hashcodes. Only use for testing.
 * 
 * @author spyros
 * 
 */
public class FakingCardinalityCalculator extends SamplingCardinalityCalculator {

	public FakingCardinalityCalculator(Executor executor) {
		super(0,0,1,2,1,250000, executor);
	}

	@Override
	protected double getCardinality(StatementPattern sp) {
		return Math.abs(sp.toString().hashCode());
	}

	@Override
	public void prepareEstimation(List<? extends StatementPattern> stPatterns, long sizeInput) {
	}

}
