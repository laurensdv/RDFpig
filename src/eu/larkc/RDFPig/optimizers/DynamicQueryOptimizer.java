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
package eu.larkc.RDFPig.optimizers;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.larkc.RDFPig.Util;
import eu.larkc.RDFPig.costCalculators.CostCalculator;
import eu.larkc.RDFPig.costCalculators.SamplingCardinalityCalculator;
import eu.larkc.RDFPig.joinOperators.ExtendedQueryModelVisitor;
import eu.larkc.RDFPig.joinOperators.NWayJoin;
import eu.larkc.RDFPig.optimizers.strategies.NoOptimizationStrategy;
import eu.larkc.RDFPig.optimizers.strategies.OptimizationStrategy;
import eu.larkc.RDFPig.pig.Executor;

public class DynamicQueryOptimizer extends
		ExtendedQueryModelVisitor<RuntimeException> {

	protected static Logger logger = LoggerFactory
			.getLogger(DynamicQueryOptimizer.class);

	protected CostCalculator calculator;

	protected Executor executor;
	protected OptimizationStrategy strategy;

	public DynamicQueryOptimizer(CostCalculator calculator, Executor executor,
			OptimizationStrategy strategy) {
		this.calculator = calculator;
		this.executor = executor;
		this.strategy = strategy;
	}

	/**
	 * Construct a query plan for this query, performing dynamic optimization.
	 * Evaluates of the query to determine which is the best plan. Potentially
	 * very expensive operation. Run this optimizer last.
	 *
	 * @param expr
	 *            the query tree to be optimized
	 * @return optimized query
	 * @throws IOException
	 * @throws QueryEvaluationException
	 */
	public TupleExpr doDynamicOptimization(TupleExpr expr) throws IOException,
			QueryEvaluationException {
		expr.visit(this);
		return expr;

		// Do not execute it now but later
		// executor.execute(Arrays.asList(expr), 1.0);

		/*
		 * while (true) { candidatePlans=new TreeSet<SearchPath>();
		 * expr.visit(this); // Run optimizer
		 *
		 * // Check if we are ready to run to the end if
		 * (strategy.checkDynamicOptimizationDone(candidatePlans)) { // We're
		 * done, return what we have return candidatePlans.first().getCurrent();
		 * } else { // Run the candidate plans Set<TupleExpr> selectedPlans =
		 * strategy.selectPlans(candidatePlans,calculator);
		 * executor.execute(selectedPlans,1.0); } }
		 */
	}

	protected int numberOfCostEvaluations = 0;

	protected double getCost(TupleExpr expr) {
		numberOfCostEvaluations++;
		double cost = calculator.getCost(expr);
		return cost;
	}

	@Override
	public void meet(NWayJoin nWayJoin) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void meet(LeftJoin leftJoin) {
		leftJoin.getLeftArg().visit(this);
		leftJoin.getRightArg().visit(this);
	}

	@Override
	public void meet(Join node) {

		ArrayList<Set<TupleCostPair>> joinArgs = new ArrayList<Set<TupleCostPair>>(); 

		// Get the first join arguments
		HashSet<TupleExpr> js = getJoinArgs(node, new HashSet<TupleExpr>());

		// Ask the calculator for the costs of the arguments and put in list
		HashSet<TupleCostPair> firstLevel = new HashSet<TupleCostPair>();
		for (TupleExpr j : js)
			firstLevel.add(new TupleCostPair(j, calculator.getCost(j)));
		joinArgs.add(firstLevel);

		try {
			optimizeJoinChildren(joinArgs.get(0));
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}

		HashSet<TupleCostPair> done = new HashSet<TupleCostPair>();

		while (!joinArgs.get(joinArgs.size() - 1).isEmpty()) {
			Set<TupleCostPair> newLevel = new HashSet<TupleCostPair>();
			for (TupleCostPair outer : joinArgs.get(joinArgs.size() - 1)) {
				Set<Var> outerVars = getTupleExprVariables(outer.tuple);
				for (int i = 0; i < joinArgs.size(); i++) {
					for (TupleCostPair inner : joinArgs.get(i)) {
						if (overlap(outer.tuple, inner.tuple))
							continue; // TODO not sure where this is needed and if it is correct
						Set<Var> innerVars = getTupleExprVariables(inner.tuple);
						innerVars.retainAll(outerVars);
						if (innerVars.isEmpty())
							continue; // No join here
						else {
							if (outer.tuple instanceof NWayJoin) {
								NWayJoin nw = (NWayJoin) outer.tuple;
								Set<Var> joinVars = getJoinVariables(nw);
								for (Var v : joinVars) {
									if (innerVars.contains(v)) {
										NWayJoin newOne = (NWayJoin) outer.tuple
												.clone();
										newOne.addArg(inner.tuple);
										if (checkDone(newOne, joinArgs.get(0)))
											done.add(new TupleCostPair(newOne,
													calculator.getCost(newOne)));
										else
											newLevel.add(new TupleCostPair(
													newOne, calculator
															.getCost(newOne)));
										break;
									}
								}
							}
							NWayJoin newOne = new NWayJoin();
							newOne.addArg(inner.tuple.clone());
							newOne.addArg(outer.tuple.clone());
							if (checkDone(newOne, joinArgs.get(0)))
								done.add(new TupleCostPair(newOne, calculator
										.getCost(newOne)));
							else
								newLevel.add(new TupleCostPair(newOne,
										calculator.getCost(newOne)));
						}
					}
				}
			}
			joinArgs.add(newLevel);
			logger.debug("Level = " + joinArgs.size() + " size of level = " + newLevel.size());
			//logger.debug(newLevel.toString());

			// Prune the search space
			strategy.prune(joinArgs, executor, calculator);
		}

		double minCost = Double.MAX_VALUE;
		TupleExpr minExpr = null;
		for (TupleCostPair expr : done) {
			if (expr.cost < minCost) {
				minExpr = expr.tuple;
				minCost = expr.cost;
			}
		}
		
		if (minExpr==null) {
			logger.warn("Expression " + node + " contains cardinal product");
			return;
		}

		if (minExpr != null) {
			logger.debug("Minimum expression " + minExpr.toString());
		}
		node.replaceWith(minExpr);
	}

	protected boolean overlap(TupleExpr c1, TupleExpr c2) {
		for (StatementPattern s1 : StatementPatternCollector.process(c1))
			for (StatementPattern s2 : StatementPatternCollector.process(c2))
				if (s1.equals(s2))
					return true;
		return false;
	}

	protected boolean isContainedIn(TupleExpr c1, TupleExpr c2) {
		List<StatementPattern> casp1 = StatementPatternCollector.process(c1);
		List<StatementPattern> casp2 = StatementPatternCollector.process(c2);
		return casp1.containsAll(casp2) || casp2.containsAll(casp1);
	}

	protected boolean checkDone(NWayJoin expr, Set<TupleCostPair> set) {
		// Get all the statement patterns used in the just-built join
		List<StatementPattern> joinArgs = StatementPatternCollector
				.process(expr);

		// Get all the original statement patterns that should be considered
		Set<StatementPattern> originalJoinArgs = new HashSet<StatementPattern>();
		for (TupleCostPair tc : set) {
			originalJoinArgs
					.addAll(StatementPatternCollector.process(tc.tuple));
		}

		// If the expr consider all the statement patterns, we have built a
		// complete exec. tree
		for (StatementPattern sp : originalJoinArgs) {
			if (!joinArgs.contains(sp))
				return false;
		}
		return true;
	}

	/**
	 * Recursively optimize join terms. Optimization is done in parallel
	 *
	 * @param set
	 * @throws IOException
	 */
	private void optimizeJoinChildren(Set<TupleCostPair> set)
			throws IOException {

		for (TupleCostPair tc : set) {
			tc.tuple.visit(this);
			tc.cost = calculator.getCost(tc.tuple);
		}

		/*
		 * boolean done=true; do { TreeSet<TupleExpr> allCandidates=new
		 * TreeSet<TupleExpr>(); for (TupleCostPair t:set) { if (t.tuple
		 * instanceof Join || t.tuple instanceof LeftJoin) { candidatePlans=new
		 * TreeSet<SearchPath>(); t.tuple.visit(this); if
		 * (!strategy.checkDynamicOptimizationDone(candidatePlans)) {
		 * done=false; allCandidates.addAll(strategy.selectPlans(candidatePlans,
		 * calculator)); } } } //executor.execute(allCandidates, 1.0); } while
		 * (!done);
		 *
		 * this.candidatePlans=new TreeSet<SearchPath>();
		 */
	}

	/**
	 * Gets all expressions that are to be joined. This is implemented by
	 * recursing down the tree and picking up all join terms
	 *
	 * @param <L>
	 * @param tupleExpr
	 * @param joinArgs
	 * @return
	 */
	protected <L extends Set<TupleExpr>> L getJoinArgs(TupleExpr tupleExpr,
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

	protected Set<Var> getTupleExprVariables(TupleExpr expr) {
		return VariableCollector.process(expr);
	}

	protected Set<Var> getJoinVariables(NWayJoin expr) {
		if (expr.getArgs().isEmpty())
			return Collections.emptySet();
		HashSet<Var> allVars = new HashSet<Var>();
		allVars.addAll(VariableCollector.process(expr.getArgs().get(0)));
		for (TupleExpr x : expr.getArgs())
			allVars.retainAll(VariableCollector.process(x));
		return allVars;
	}

	/**
	 * For testing
	 *
	 * @param args
	 * @throws MalformedQueryException
	 * @throws IOException
	 * @throws QueryEvaluationException
	 */
	public static void main(String[] args) throws MalformedQueryException,
			IOException, QueryEvaluationException {
		SPARQLParser parser = new SPARQLParser();

		String inDir = "./data/bsbm250K.nt";
		String outDir = "./test";

		String query = null;
		try {
			query = Util.readFile("queries/BSBM1.sparql");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ParsedQuery q = parser.parseQuery(query, "http://blaaah");

		TupleExpr tupleExpr = q.getTupleExpr();

		if (!(tupleExpr instanceof Query)) {
			tupleExpr = new QueryRoot(tupleExpr);
		}

		System.out.println(tupleExpr);
		Executor ex = new Executor(inDir, outDir);
		SamplingCardinalityCalculator calculator2 = new SamplingCardinalityCalculator(
				1000000, 0, 1, 1, 1, 250000, ex);

		calculator2.prepareEstimation(
				StatementPatternCollector.process(tupleExpr), 10000000);
		DynamicQueryOptimizer opt = new DynamicQueryOptimizer(calculator2, ex,
				new NoOptimizationStrategy());
		opt.doDynamicOptimization(tupleExpr);
		SanityCheckVisitor san = new SanityCheckVisitor();
		tupleExpr.visit(san);

		System.out.println(tupleExpr);
	}

}
