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


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.yahoo.rdfpig.costcalculators.FakingCardinalityCalculator;
import com.yahoo.rdfpig.costcalculators.SamplingCardinalityCalculator;

import eu.larkc.RDFPig.Util;
import eu.larkc.RDFPig.joinOperators.ExtendedQueryModelVisitor;
import eu.larkc.RDFPig.joinOperators.NWayJoin;
import eu.larkc.RDFPig.pig.Executor;
import eu.larkc.RDFPig.pig.TupleSetMetadata;

public class EfficientJoinOptimizer extends ExtendedQueryModelVisitor<RuntimeException> {


	private static final Integer STATICPLANNINGTHRESHOLD = 5;
	private static final int NUMBEROFPLANSPERJOINGROUP =4;
//	protected static Logger logger = LoggerFactory.getLogger(EfficientJoinOptimizerVisitor.class);

	protected SamplingCardinalityCalculator calculator;

	protected TreeSet<SearchPath> candidatePlans;
	protected Executor executor;
	protected DynamicOptimizationStrategy strategy;

	public EfficientJoinOptimizer(SamplingCardinalityCalculator calculator) {
		this.calculator=calculator;
	}

	/**
	 * Returns the path for the results. Performs dynamic optimization.
	 * @param expr
	 * @return
	 * @throws IOException
	 * @throws QueryEvaluationException
	 */
	public String doDynamicOptimization(TupleExpr expr) throws IOException, QueryEvaluationException {
		while (true) {
			candidatePlans=new TreeSet<SearchPath>();
			expr.visit(this); // Run optimizer

			// Check if we are ready to run to the end
			if (strategy.checkDynamicOptimizationDone(candidatePlans)) {
				// We're done, run what we have
				List<TupleExpr> t = Arrays.asList((TupleExpr)candidatePlans.first().current);
				TupleSetMetadata m= executor.execute(t, 1.0).get(t);
				return m.name; // TODO We actually want the filename for this one, not the variable name
			}
			else {
				// Run the candidate plans
				executor.execute(strategy.selectPlans(candidatePlans,calculator),1.0);
			}
		}
	}

	protected int numberOfCostEvaluations=0;

	protected double getCost(TupleExpr expr) {
		numberOfCostEvaluations++;
		double cost= calculator.getCost(expr);
	//	System.out.println("Cost for " +expr + " = " + cost);
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
		/* This optimizer assumes that adding nodes to a tree can only increase cost, it is guaranteed to find the best result */

		// Recursively get the join arguments
		Queue<TupleExpr> joinArgs = getJoinArgs(node,
				new LinkedList<TupleExpr>());

		try {
			optimizeJoinChildren(joinArgs);
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		PriorityQueue<SearchPath> search=new PriorityQueue<SearchPath>();

		// Build the set of all possible join variable combinations to reduce search space
		Map<Set<Var>, Integer> varCombinationFrequencies=new HashMap<Set<Var>,Integer>();
		for (TupleExpr tOuter: joinArgs) {
			Set<Var> tOuterVars=getUnboundVars(Util.getExpressionVars(tOuter));
			List<Set<Var>> allcombOuter = new ArrayList<Set<Var>>();
			getAllCombinationsRecursively(tOuterVars, new ArrayList<Var>(), allcombOuter);
			for (Set<Var> c: allcombOuter) {
				for (TupleExpr tInner: joinArgs) {
					if (tInner!=tOuter) {
						Set<Var> tInnerVars=getUnboundVars(Util.getExpressionVars(tInner));
						if (tInnerVars.containsAll(c)) {
							Integer count=varCombinationFrequencies.get(c);
							if (count==null)
								varCombinationFrequencies.put(c, 1);
							else
								varCombinationFrequencies.put(c, count+1);
						}
					}
				}
			}
		}

		// Do static joins
		Map<Set<Var>, NWayJoin> staticPaths=new HashMap<Set<Var>, NWayJoin>();
		Iterator<TupleExpr> it=joinArgs.iterator();
		while(it.hasNext()) {
			boolean skipTuple=false;
			TupleExpr nextOne=it.next();
			Set<Var> exprVars = getUnboundVars(Util.getExpressionVars(nextOne));
			for (Map.Entry<Set<Var>,Integer> c:varCombinationFrequencies.entrySet()) {
				if (exprVars.containsAll(c.getKey()) && c.getValue()>STATICPLANNINGTHRESHOLD) {
						skipTuple=true;
						NWayJoin sp=staticPaths.get(c.getKey());
						if (sp==null) {
							sp=new NWayJoin();
							sp.addArg(nextOne);
							staticPaths.put(c.getKey(),sp);
						}
						else {
							sp.addArg(nextOne);
						}
					}
			}
			if(skipTuple)
				it.remove();
		}

		for (TupleExpr e:staticPaths.values()) {
			joinArgs.add(e);
		}


		// Initialize search
		SearchPath best=new SearchPath();
		best.current=new NWayJoin();
		best.remaining=joinArgs;
		best.cost=new Double(0);
		best.joinVars=new HashSet<Var>();
		best.allVars=new HashSet<Var>();

		// prime the search
		for (TupleExpr nextOne: joinArgs) {
			Set<Var> exprVars = getUnboundVars(Util.getExpressionVars(nextOne));
			for (Map.Entry<Set<Var>,Integer> c:varCombinationFrequencies.entrySet()) {
				if (exprVars.containsAll(c.getKey())) {
					SearchPath newOne=(SearchPath) best.clone();
					newOne.remaining.remove(nextOne);
					newOne.current.addArg(nextOne);
					newOne.joinVars=c.getKey();
					newOne.allVars=new HashSet<Var>(exprVars);
					newOne.cost=getCost(newOne.current);
					search.add(newOne);
				}
			}
		}




		int searchInNWay=0;
		int searchInOther=0;
		int plansFoundSoFar=0;
		SearchPath trueBest=null;

		// TODO account for costs that depend on the order of the operations
		while (!search.isEmpty()) {
			best=search.poll();


//			if (iterations++%100==0) { // FIXME In current calculator, NWay joins can become cheaper if we add terms
//				System.out.println(best);
//				System.out.println("#search=" +search.size() + " remaining=" + best.remaining.size() + " cost=" + best.cost + " #nway=" + searchInNWay + " #other=" + searchInOther);
//
//				//for (SearchPath p:search)
//				//	System.out.print(p.cost+" ");
//			}

			if (best.remaining.isEmpty()) { // found a candidate
				if (trueBest==null)
					trueBest=best;
				plansFoundSoFar++;
				candidatePlans.add((SearchPath) best.clone());
				if (plansFoundSoFar<NUMBEROFPLANSPERJOINGROUP) // found all the candidates we want
					break;
			}

			TupleExpr nextOne= best.remaining.poll();
			Set<Var> exprVars = getUnboundVars(Util.getExpressionVars(nextOne));
			if (best.current.getArgs().size()>1) { // Only if we already have a join
				for (TupleExpr t: best.current.getArgs()) {	// Switch join tuple
						SearchPath newOne=(SearchPath) best.clone();
						Set<Var> nExprVars=new HashSet<Var>(exprVars);
						nExprVars.retainAll(Util.getExpressionVars(t));
						if (nExprVars.isEmpty())
							continue; // no join possible with this tuple
						NWayJoin newRoot=new NWayJoin();
						newRoot.addArg(newOne.current);
						newRoot.addArg(nextOne);
						newOne.current=newRoot;
						newOne.remaining.remove(nextOne);
						newOne.allVars.addAll(exprVars);

						for (Set<Var> c:varCombinationFrequencies.keySet()) { // Make joins on all possible combinations
							if (nExprVars.containsAll(c)) {
								SearchPath n=(SearchPath) newOne.clone();
								n.joinVars=c;
								n.cost=getCost(n.current);
								searchInOther++;
								search.add(n);
								if (n.cost<best.cost)
									throw new IllegalStateException("Cost model is problematic, cost is decreasing, while plan was expanded \nnew plan=\n" + n + "\n old plan=\n"+best);
							}
					}
				}
			}
			if (best.isJoinCompatible(exprVars)) { // Add to current join, if we can do the join, TODO we do not need all the join orders
				SearchPath newOne=(SearchPath) best.clone();
				newOne.remaining.remove(nextOne);
				newOne.current.addArg(nextOne);
				newOne.cost=getCost(newOne.current);
				newOne.allVars.addAll(exprVars);
				searchInNWay++;
				search.add(newOne);
				if (newOne.cost<best.cost)
					throw new IllegalStateException("Cost model is problematic, cost is decreasing, while plan was expanded \nnew plan=\n" + newOne + "\n old plan=\n"+best);
			}

			best=null; // To cause an exception if we do not find a solution
		}
		node.replaceWith(best.current);
	}

	/**
	 * Recursively optimize join terms. Optimization is done in parallel
	 * @param joinArgs
	 * @throws IOException
	 * @throws QueryEvaluationException
	 */
	private void optimizeJoinChildren(Queue<TupleExpr> joinArgs) throws IOException, QueryEvaluationException {

		boolean done=true;
		do {
			TreeSet<TupleExpr> allCandidates=new TreeSet<TupleExpr>();
			for (TupleExpr t:joinArgs) {
				candidatePlans=new TreeSet<SearchPath>();
				t.visit(this);
				if (!strategy.checkDynamicOptimizationDone(candidatePlans)) {
					done=false;
					allCandidates.addAll(strategy.selectPlans(candidatePlans, calculator));
				}
			}
			executor.execute(allCandidates, 1.0);
		}
		while (!done);

		this.candidatePlans=new TreeSet<SearchPath>();
	}

	/**
	 * Recalculates the costs for the current SearchPaths.
	 * @param q
	 * @return
	 */
	protected PriorityQueue<SearchPath> recalculateCosts(PriorityQueue<SearchPath> q) {
		PriorityQueue<SearchPath> r=new PriorityQueue<SearchPath>(q.size());
		for (SearchPath s:q) {
			s.cost=getCost(s.current);
			r.add(s);
		}
		return r;
	}



	protected boolean isJoinFeasible(TupleExpr expr, Set<Var> joinTerms) {
		Set<Var> exprVars = getUnboundVars(Util.getExpressionVars(expr));
		return exprVars.containsAll(joinTerms);
	}

	protected List<Var> getCommonVariables(List<Var>... t) {
		ArrayList<Var> ret=new ArrayList<Var>();
		if (t.length==0)
			return ret;
		else
			ret.addAll(t[0]);
		for (List<Var> tuple:t) {
			ret.retainAll(tuple);
		}
		return ret;
	}

	protected Set<Var> getUnboundVars(Iterable<Var> vars) {
		Set<Var> unboundVars = new HashSet<Var>();

		for (Var var : vars) {
			if (!var.hasValue()) {
				unboundVars.add(var);
			}
		}

		return unboundVars;
	}

	/**
	 * Gets all expressions that are to be joined. This is implemented by recursing down the tree and picking up all join terms
	 * @param <L>
	 * @param tupleExpr
	 * @param joinArgs
	 * @return
	 */
	protected <L extends Queue<TupleExpr>> L getJoinArgs(
			TupleExpr tupleExpr, L joinArgs) {
		if (tupleExpr instanceof Join) {
			Join join = (Join) tupleExpr;
			getJoinArgs(join.getLeftArg(), joinArgs);
			getJoinArgs(join.getRightArg(), joinArgs);
		} else {
			joinArgs.add(tupleExpr);
		}

		return joinArgs;
	}

	protected void getAllCombinationsRecursively(Set<Var> exprVars, List<Var> soFar, List<Set<Var>> allcomb) {
		Iterator<Var> it=exprVars.iterator();
		while (it.hasNext()) {
			Var t=it.next();
			Set<Var> newOne=new HashSet<Var>();
			newOne.addAll(soFar);
			newOne.add(t);
			allcomb.add(newOne);

			Set<Var> newRemaining=new HashSet<Var>(exprVars);
			newRemaining.remove(t);
			it.remove(); // we want combinations, not permutations
			List<Var> newSoFar=new ArrayList<Var>(soFar);
			newSoFar.add(t);
			getAllCombinationsRecursively(newRemaining, newSoFar, allcomb);
		}


	}

	protected class SearchPath implements Comparable<SearchPath> {

		NWayJoin current;
		Queue<TupleExpr> remaining;
		Double cost;
		Set<Var> joinVars;
		Set<Var> allVars;

		@Override
		public int compareTo(SearchPath o) {
			if (o.cost==this.cost)
				return 0;
			return this.cost < o.cost? -1 : 1;
		}
		/* (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			return cost.hashCode();
		}
		/* (non-Javadoc)
		 * @see java.lang.Object#clone()
		 */
		@Override
		protected Object clone() {
			SearchPath newOne=new SearchPath();
			newOne.current=current.clone();
			newOne.remaining=new LinkedList<TupleExpr>(remaining);
			newOne.cost=cost;
			newOne.joinVars=new HashSet<Var>(joinVars);
			newOne.allVars=new HashSet<Var>(allVars);
			return newOne;
		}

		@Override
		public String toString() {
			return current.toString() + "\n cost=" + cost + " jointerms=" + joinVars + " remaining: " + remaining;
		}

		public boolean isJoinCompatible(Set<Var> vars) {
			if (!vars.containsAll(joinVars)) // make sure that all join variables are in the candidate expr
				return false;
		//	for (Var v:vars) {
		//		if (allVars.contains(v) && !joinVars.contains(v))  // make sure that no joins are missed because of joining on too few elements
		//			return false;
		//	}
			return true;
		}

	}

	/**
	 * For testing
	 * @param args
	 * @throws MalformedQueryException
	 * @throws IOException
	 * @throws QueryEvaluationException
	 */
	public static void main(String[] args) throws MalformedQueryException, IOException, QueryEvaluationException {
		SPARQLParser parser = new SPARQLParser();

		String query=null;
		try {
			query = Util.readFile("queries/nastyquery.sparql");
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
		FakingCardinalityCalculator calculator2 = new FakingCardinalityCalculator(null);
		calculator2.prepareEstimation(null, 10000000);
		EfficientJoinOptimizer opt=new EfficientJoinOptimizer(calculator2);
		opt.doDynamicOptimization(tupleExpr);

		System.out.println(tupleExpr);
	}


}
