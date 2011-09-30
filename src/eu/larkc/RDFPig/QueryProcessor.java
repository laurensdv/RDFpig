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
package eu.larkc.RDFPig;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.impl.BindingAssigner;
import org.openrdf.query.algebra.evaluation.impl.CompareOptimizer;
import org.openrdf.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.openrdf.query.algebra.evaluation.impl.DisjunctiveConstraintOptimizer;
import org.openrdf.query.algebra.evaluation.impl.FilterOptimizer;
import org.openrdf.query.algebra.evaluation.impl.IterativeEvaluationOptimizer;
import org.openrdf.query.algebra.evaluation.impl.QueryModelNormalizer;
import org.openrdf.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.larkc.RDFPig.costCalculators.CompositeCostCalculator;
import eu.larkc.RDFPig.costCalculators.CostCalculator;
import eu.larkc.RDFPig.optimizers.DynamicQueryOptimizer;
import eu.larkc.RDFPig.optimizers.strategies.OptimizationStrategy;
import eu.larkc.RDFPig.pig.Executor;
import eu.larkc.RDFPig.pig.TupleSetMetadata;

public class QueryProcessor {

	protected static Logger logger = LoggerFactory.getLogger(QueryProcessor.class);

	private SPARQLParser parser;
	private Executor queryExecutor;

	private DynamicQueryOptimizer dynQueryOptimizer;
	private CostCalculator costCalculator;

	public QueryProcessor(String inputDir, String outputDir) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException  {
		parser = new SPARQLParser();
		queryExecutor= new Executor(inputDir, outputDir);
		
		costCalculator = createCostCalculator(Configuration
				.getInstance().getProperty(
						Configuration.COST_CALCULATOR_CLASSES_WEIGHTS), queryExecutor);
		OptimizationStrategy s=createOptimizationStrategy(Configuration
				.getInstance().getProperty(Configuration.OPTIMIZATIONSTRATEGY));
		this.dynQueryOptimizer=new DynamicQueryOptimizer(costCalculator, queryExecutor, s);
	}

	/**
	 * @param query
	 * @param nsBase
	 * @throws MalformedQueryException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws QueryEvaluationException
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	protected void processQuery(String query, String nsBase)
			throws MalformedQueryException, IOException, QueryEvaluationException {
		ParsedQuery q = parser.parseQuery(query, nsBase);

		
		TupleExpr tupleExpr = q.getTupleExpr();

		if (!(tupleExpr instanceof Query)) {
			tupleExpr = new QueryRoot(tupleExpr);
		}
		
		logger.debug("Original tuple expr: \n" + tupleExpr.toString());

		Dataset dataset = null;
		BindingSet bindings = new MapBindingSet();

		// Boring optimizations
		new QueryModelNormalizer().optimize(tupleExpr, dataset, bindings);
		new BindingAssigner().optimize(tupleExpr, dataset, bindings);
		new CompareOptimizer().optimize(tupleExpr, dataset, bindings);
		new ConjunctiveConstraintSplitter().optimize(tupleExpr, dataset,
				bindings);
		new DisjunctiveConstraintOptimizer().optimize(tupleExpr, dataset,
				bindings);
		new SameTermFilterOptimizer().optimize(tupleExpr, dataset, bindings);		
		new IterativeEvaluationOptimizer().optimize(tupleExpr, dataset,
				bindings);
		new FilterOptimizer().optimize(tupleExpr, dataset, bindings);
		
		logger.debug("Tuple expr after boring optimizations: \n" + tupleExpr.toString());

		// Interesting optimizations
		
		// Get all statement patterns
		StatementPatternCollector allSp=new StatementPatternCollector(){
			@Override
			public void meet(Filter node)
			{
				super.meetUnaryTupleOperator(node); // Also visit filter conditions
			}
			
		};
		tupleExpr.visit(allSp);
		
		costCalculator.prepareEstimation(allSp.getStatementPatterns(), 10000000);
		
		
		tupleExpr=dynQueryOptimizer.doDynamicOptimization(tupleExpr);

		logger.info("Final tuple expr: " + tupleExpr);
		
		List<TupleExpr> list = new ArrayList<TupleExpr>();
		list.add(tupleExpr);
		Map<TupleExpr, TupleSetMetadata> results = queryExecutor.execute(list, 1.0);
		logger.info("Size of the results: " + results.size());

	}
	
	private CostCalculator createCostCalculator(String costCalculatorsSpec, Executor executor) {
		Map<CostCalculator, Double> calcs=parseCostCalculatorConfigString(costCalculatorsSpec, executor);
		if (calcs.isEmpty())
			throw new IllegalArgumentException("No cost calculators specified");
		else if (calcs.size()==1)
			return calcs.keySet().iterator().next();
		else 
			return new CompositeCostCalculator(calcs);
	}
	
	private OptimizationStrategy createOptimizationStrategy(String optimizationStrategy) throws InstantiationException, IllegalAccessException, ClassNotFoundException  {
		return (OptimizationStrategy) Class.forName(optimizationStrategy).newInstance();
	}

	private Map<CostCalculator, Double> parseCostCalculatorConfigString(String costCalcs, Executor executor) {
		Map<CostCalculator, Double> ret = new HashMap<CostCalculator, Double>();
		try {
			String[] fields = costCalcs.split(";");
			for (int i = 0; i < fields.length; i += 2) {
				String classSpec=fields[0];
				int delim=classSpec.indexOf("(");
				String[] args=classSpec.substring(delim+1,classSpec.indexOf(")")).split(",");
				Object[] nargs=new Object[args.length+1];
				for (int j=0; j<args.length; j++) {
					try {
						nargs[j]=Integer.parseInt(args[j].trim());
					}
					catch (NumberFormatException e) {
						try {
							nargs[j]=Double.parseDouble(args[j].trim());
						}
						catch (NullPointerException e2) {
							nargs[i]=args[i].trim();
						}
					}
				}
				nargs[nargs.length-1]=executor;
				CostCalculator costCalculator=null;
				String classname = classSpec.substring(0,delim);
				for (Constructor<?> c:Class.forName(classname).getConstructors()) {
					try {
						costCalculator=(CostCalculator) c.newInstance(nargs);
						logger.info("Matched constructor for " + classname);
						break;
					}
					catch (InstantiationException e) {
						logger.warn("Did not match constructor, trying next: ", e);
					}
				}
				if (costCalculator==null)
					logger.warn("Could not instantiate cost calculator for "+classname);
				else
					ret.put(costCalculator, Double.parseDouble(fields[i + 1]));
			}
		} catch (Exception e) {
			throw new IllegalArgumentException(
					"Illegal calculator specification string: " + costCalcs, e);
		}
		return ret;
	}

}
