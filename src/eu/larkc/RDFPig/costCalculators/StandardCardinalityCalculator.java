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

import java.io.IOException;
import java.util.List;

import org.openrdf.query.algebra.StatementPattern;

import eu.larkc.RDFPig.pig.Executor;

public class StandardCardinalityCalculator extends
		AbstractCostCalculator {
	
	protected StandardCardinalityCalculator(long costPerDepth,
			long replicatedJoinThreshold, double costReadTuple,
			double costWriteTuple, double replicatedJoinBonus, long inputSize, Executor executor) {
		super(costPerDepth, replicatedJoinThreshold, costReadTuple, costWriteTuple,
				replicatedJoinBonus, inputSize);
	}

	//Coefficients for the variables
	private long sizeOfInput=1;
	
	public double WEIGHT_ZERO_VARIABLE = 0.000000001 * sizeOfInput;
	public double WEIGHT_ONE_VARIABLE = 0.1 * sizeOfInput;
	public double WEIGHT_TWO_VARIABLE = 0.6 * sizeOfInput;
	public double WEIGHT_THREE_VARIABLE = 1.0 * sizeOfInput;

	@Override
	public void prepareEstimation(List<? extends StatementPattern> expressions, long sizeOfInput) throws IOException {
		this.sizeOfInput=sizeOfInput;
	}

	@Override
	protected double getCardinality(StatementPattern sp) {
		int count = 0;
		
		if (sp.getSubjectVar().getValue() == null) {
			count++;
		}
		
		if (sp.getPredicateVar().getValue() == null) {
			count++;
		}
		
		if (sp.getObjectVar().getValue() == null) {
			count++;
		}
		
		if (count == 0)
			return WEIGHT_ZERO_VARIABLE;
		else if (count == 1)
			return WEIGHT_ONE_VARIABLE;
		else if (count == 2)
			return WEIGHT_TWO_VARIABLE;
		else
			return WEIGHT_THREE_VARIABLE;
	}


	/*@Override
	public double getCardinality(TupleExpr join) {
		// Recursively get the join arguments
		List<TupleExpr> joinArgs = getJoinArgs(join, new ArrayList<TupleExpr>());

		// Build maps of cardinalities and vars per tuple expression
		Map<TupleExpr, List<Var>> varsMap = new HashMap<TupleExpr, List<Var>>();

		for (TupleExpr tupleExpr : joinArgs) {
			varsMap.put(tupleExpr, Util.getStatementPatternVars(tupleExpr));
		}

		// Build map of var frequences
		Map<Var, Integer> varFreqMap = new HashMap<Var, Integer>();
		for (List<Var> varList : varsMap.values()) {
			getVarFreqMap(varList, varFreqMap);
		}

		return 0;
	}

	protected <L extends List<TupleExpr>> L getJoinArgs(TupleExpr tupleExpr,
			L joinArgs) {
		if (tupleExpr instanceof Join) {
			Join join = (Join) tupleExpr;
			getJoinArgs(join.getLeftArg(), joinArgs);
			getJoinArgs(join.getRightArg(), joinArgs);
		} else {
			joinArgs.add(tupleExpr);
		}

		return joinArgs;
	}

	protected int getForeignVarFreq(List<Var> ownUnboundVars,
			Map<Var, Integer> varFreqMap) {
		int result = 0;

		Map<Var, Integer> ownFreqMap = getVarFreqMap(ownUnboundVars,
				new HashMap<Var, Integer>());

		for (Map.Entry<Var, Integer> entry : ownFreqMap.entrySet()) {
			Var var = entry.getKey();
			int ownFreq = entry.getValue();
			result += varFreqMap.get(var) - ownFreq;
		}

		return result;
	}

	protected <M extends Map<Var, Integer>> M getVarFreqMap(List<Var> varList,
			M varFreqMap) {
		for (Var var : varList) {
			Integer freq = varFreqMap.get(var);
			freq = (freq == null) ? 1 : freq + 1;
			varFreqMap.put(var, freq);
		}
		return varFreqMap;
	}
	
	protected double getTupleExprCardinality(TupleExpr tupleExpr,
			Map<TupleExpr, Double> cardinalityMap,
			Map<TupleExpr, List<Var>> varsMap, Map<Var, Integer> varFreqMap,
			Set<String> boundVars) {
		double cardinality = cardinalityMap.get(tupleExpr);

		List<Var> vars = varsMap.get(tupleExpr);

		// Compensate for variables that are bound earlier in the evaluation
		List<Var> unboundVars = getUnboundVars(vars);
		List<Var> constantVars = getConstantVars(vars);
		int nonConstantVarCount = vars.size() - constantVars.size();
		if (nonConstantVarCount > 0) {
			double exp = (double) unboundVars.size() / nonConstantVarCount;
			cardinality = Math.pow(cardinality, exp);
		}

		if (unboundVars.isEmpty()) {
			// Prefer patterns with more bound vars
			if (nonConstantVarCount > 0) {
				cardinality /= nonConstantVarCount;
			}
		} else {
			// Prefer patterns that bind variables from other tuple expressions
			int foreignVarFreq = getForeignVarFreq(unboundVars, varFreqMap);
			if (foreignVarFreq > 0) {
				cardinality /= foreignVarFreq;
			}
		}

		// Prefer patterns that bind more variables
		// List<Var> distinctUnboundVars = getUnboundVars(new
		// HashSet<Var>(vars));
		// if (distinctUnboundVars.size() >= 2) {
		// cardinality /= distinctUnboundVars.size();
		// }

		return cardinality;
	}

	protected List<Var> getConstantVars(Iterable<Var> vars) {
		List<Var> constantVars = new ArrayList<Var>();

		for (Var var : vars) {
			if (var.hasValue()) {
				constantVars.add(var);
			}
		}

		return constantVars;
	}*/
}
