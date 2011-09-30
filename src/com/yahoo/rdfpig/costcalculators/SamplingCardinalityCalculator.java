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
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.PigServer;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.larkc.RDFPig.joinOperators.NWayJoin;
import eu.larkc.RDFPig.pig.Executor;
import eu.larkc.RDFPig.pig.TupleSetMetadata;

public class SamplingCardinalityCalculator extends
		StandardCardinalityCalculator {

	private Executor executor;

	public SamplingCardinalityCalculator(long costPerDepth,
			long replicatedJoinThreshold, double costReadTuple,
			double costWriteTuple, double replicatedJoinBonus, long inputSize,
			Executor executor) {
		super(costPerDepth, replicatedJoinThreshold, costReadTuple,
				costWriteTuple, replicatedJoinBonus, inputSize, executor);
		this.executor = executor;
	}

	PigServer pigServer = null;

	protected static Logger logger = LoggerFactory
			.getLogger(SamplingCardinalityCalculator.class);

	@Override
	public void prepareEstimation(List<? extends StatementPattern> expressions,
			long sizeOfInput) throws IOException {
		try {
			// executor.execute(expressions,
			// Configuration.getInstance().getPropertyDouble(Configuration.SAMPLE_RATE));
			executor.execute(expressions, 1.0);
		} catch (QueryEvaluationException e) {
			throw new IOException(e);
		}

		logger.info("Statement Pattern sampling done");
		for (StatementPattern s : expressions)
			logger.info("Estimated cardinality of:" + s + " = "
					+ executor.cache.getFromCache(s).getSize());
	}

	@Override
	protected double getCardinality(StatementPattern st) {
		TupleSetMetadata meta = executor.cache.getFromCache(st);
		if (meta != null)
			return meta.getSize();
		else
			return super.getCardinality(st);
	}

	@Override
	public double getCost(TupleExpr pattern) {
		// Check if calculated already
		TupleSetMetadata meta = executor.cache.getFromCache(pattern);
		if (meta != null)
			return meta.getSize() * super.costReadTuple;
		else
			return super.getCost(pattern);
	}

	/**
	 * THIS IS WORK IN PROGRESS TODO
	 * @param exprs
	 * @param debt
	 * @param current
	 * @return
	 */
	protected double calculateCorrelationRecursive(List<TupleExpr> exprs, List<TupleExpr> debt, TupleExpr current) {
		if (exprs.size()<2)
			return 1;
		if (current!=null) {
			for (int i=0; i<exprs.size(); i++) {
				NWayJoin nJ=new NWayJoin();
				exprs=new ArrayList<TupleExpr>(exprs);
				nJ.addArg(exprs.remove(i));
				nJ.addArg(current);
				return getCost(nJ)*calculateCorrelationRecursive(exprs, debt, null);
			}
		}
		else if (current==null) {
			for (int i=0; i<exprs.size(); i++) {
				exprs=new ArrayList<TupleExpr>(exprs);
				return calculateCorrelationRecursive(exprs, debt, exprs.remove(i));
			}
		}
		return 0;
	}

	public boolean isCalculated(NWayJoin current) {
		// TODO to implement
		return false;
	}

	public boolean isSampled(NWayJoin current) {
		// TODO to implement
		return false;
	}

}
