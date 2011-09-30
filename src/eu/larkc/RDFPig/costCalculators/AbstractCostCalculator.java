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

import java.util.Arrays;
import java.util.List;

import org.openrdf.query.algebra.BinaryTupleOperator;
import org.openrdf.query.algebra.EmptySet;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.SingletonSet;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.evaluation.impl.ExternalSet;

import eu.larkc.RDFPig.joinOperators.ExtendedQueryModelVisitor;
import eu.larkc.RDFPig.joinOperators.NWayJoin;

public abstract class AbstractCostCalculator extends
	ExtendedQueryModelVisitor<RuntimeException> implements CostCalculator{

	/*
	 * Replicated joins have to be ordered in decreasing cardinality
	 * Normal joins have to be ordered in increasing cardinality
	 */
	
	protected long inputSize = 0;
	protected double cardinality = 0;

	
	protected double cost = 0;
	protected int depth = 0; // number of jobs
	protected final long costPerDepth;
	private final double replicatedJoinThreshold; // max tuples for a replicated join
	protected final double costReadTuple;
	protected final double costWriteTuple;
	protected final double replicatedJoinBonus;
	
	
	protected AbstractCostCalculator(long costPerDepth, long replicatedJoinThreshold, double costReadTuple, double costWriteTuple, double replicatedJoinBonus, long inputSize) {
		this.costPerDepth=costPerDepth;
		this.inputSize=inputSize;
		this.replicatedJoinBonus=replicatedJoinBonus;
		this.replicatedJoinThreshold=replicatedJoinThreshold;
		this.costReadTuple=costReadTuple;
		this.costWriteTuple=costWriteTuple;
	}

	abstract protected double getCardinality(StatementPattern sp);

	protected void meetExternalSet(ExternalSet node) {
		cardinality = node.cardinality();
	}

	@Override
	public void setInputSize(long sizeOfInput) {
		this.inputSize = sizeOfInput;
	}

	@Override
	public void meet(LeftJoin node) {
		node.getLeftArg().visit(this);
		double leftArgCardinality = this.cardinality;
		double leftArgCost = this.cost;
		int leftArgDepth = this.depth;

		node.getRightArg().visit(this);
		double rightArgCost= this.cost;
		double bonus=this.cardinality<replicatedJoinThreshold?replicatedJoinBonus:1;
		double rightArgCardinality= this.cardinality;
		cardinality *= leftArgCardinality*calculateCorrelation(Arrays.asList(node.getLeftArg(), node.getRightArg()));

		cost =costWriteTuple*cardinality*bonus + leftArgCost + rightArgCost + (leftArgCardinality + rightArgCardinality)*costReadTuple;
		depth = Math.max(leftArgDepth, this.depth)+1;
	}

	@Override
	protected void meetBinaryTupleOperator(BinaryTupleOperator node) {
		node.getLeftArg().visit(this);
		double leftArgCost = this.cost;
		double leftArgCard = this.cardinality;
		double leftArgDepth = this.depth;
		
		node.getRightArg().visit(this);
		cardinality += leftArgCard;
		cost+=leftArgCost;
		depth+=leftArgDepth;
	}

	@Override
	protected void meetUnaryTupleOperator(UnaryTupleOperator node) {
		node.getArg().visit(this);
	}

	
	
	@Override
	protected void meetNode(QueryModelNode node) {
		if (node instanceof ExternalSet) {
			meetExternalSet((ExternalSet) node);
		} else {
			throw new IllegalArgumentException("Unhandled node type: "
					+ node.getClass());
		}
	}

	@Override
	public void meet(Join node) {
		if (node instanceof NWayJoin) {
			meet((NWayJoin) node);
		} else {
			node.getLeftArg().visit(this);
			double leftArgCardinality = this.cardinality;
			int ldepth = this.depth;
			double leftArgCost = this.cost;

			node.getRightArg().visit(this);
			double rightArgCardinality = this.cardinality;
			double rightArgCost = this.cost;

			depth = Math.max(ldepth, this.depth)+1;
			double bonus=this.cardinality<replicatedJoinThreshold?replicatedJoinBonus:1;
			cardinality += rightArgCardinality
					* leftArgCardinality
					* calculateCorrelation(Arrays.asList(node.getLeftArg(),
							node.getRightArg()));
			cost+=(rightArgCardinality+leftArgCardinality)*costReadTuple+leftArgCost+rightArgCost + this.cardinality*costWriteTuple*bonus;
		}
	}


	@Override
	public void meet(NWayJoin nWayJoin) {
		if (nWayJoin.getArgs().size()==0)
			return;
		
		double[] cards = new double[nWayJoin.getArgs().size()];
		int[] depths = new int[nWayJoin.getArgs().size()];
		double tCost =0;
		int i = 0;
		boolean bonus=true;
		for (TupleExpr e : nWayJoin.getArgs()) {
			e.visit(this);
			cards[i] = this.cardinality;
			if (this.cardinality>=replicatedJoinThreshold && i>0)
				bonus=false;
			depths[i] = this.depth;
			tCost+=this.cost;
			tCost+=this.cardinality*costReadTuple;
			i++;
		}
		// Now we have covered the cost of reading and the cost of the children
		
		Arrays.sort(depths);
		int maxDepth = depths[depths.length - 1];
		this.depth = maxDepth + 1;

		this.cardinality = 1;
		for (double d : cards)
			this.cardinality *= d;
		this.cardinality *= calculateCorrelation(nWayJoin.getArgs());
		this.cost=tCost+ this.cardinality*costWriteTuple*(bonus?replicatedJoinBonus:1);
	}

	/**
	 * We assume that we have 50% correlation between expressions. This is a very crude estimate based on nothing.
	 * @param exprs
	 * @return
	 */
	protected double calculateCorrelation(List<TupleExpr> exprs) {
		// Make all pairs
		return Math.pow(0.5, exprs.size()-1);
	}


	public double getCost(TupleExpr expr) {
		cardinality = 0;
		depth=0;
		cost =0;
		expr.visit(this);
		return this.cost + depth*costPerDepth;
	}


	@Override
	public void meet(EmptySet node) {
		cardinality = 0;
	}

	@Override
	public void meet(SingletonSet node) {
		cardinality = 1;
	}

	@Override
	public void meet(StatementPattern sp) {
		cardinality = getCardinality(sp);
		cost+= cardinality*costReadTuple;
	}

	public double getInputSize() {
		return inputSize;
	}
}