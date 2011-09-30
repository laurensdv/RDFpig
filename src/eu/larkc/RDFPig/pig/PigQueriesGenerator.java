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
package eu.larkc.RDFPig.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList; 
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Distinct;
import org.openrdf.query.algebra.Exists;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Group;
import org.openrdf.query.algebra.GroupElem;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.MultiProjection;
import org.openrdf.query.algebra.Not;
import org.openrdf.query.algebra.Order;
import org.openrdf.query.algebra.OrderElem;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.Slice;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.larkc.RDFPig.Configuration;
import eu.larkc.RDFPig.Util;
import eu.larkc.RDFPig.costCalculators.JoinTypeSelector;
import eu.larkc.RDFPig.costCalculators.JoinTypeSelector.JoinType;
import eu.larkc.RDFPig.io.NTriplesReader;
import eu.larkc.RDFPig.joinOperators.NWayJoin;

public class PigQueriesGenerator {

	protected static Logger logger = LoggerFactory
			.getLogger(PigQueriesGenerator.class);

	private JoinTypeSelector joinTypeSelector;
	private static String INPUT_VAR = "IN";
	private static final String DATATYPE = "chararray";
	ValueExprGenerator valueExprGen = new ValueExprGenerator();
	static int storageCount = 0;

	// This class is a container of all parameters that might be needed during
	// the evaluation
	private class Context {
		// public double sampleRate = 1;
		public List<String> pigQueries = null;
		public Executor.Cache cache = null;
		public double desiredSampling = 1.0;
	}

	public PigQueriesGenerator(Executor executor,
			JoinTypeSelector joinTypeSelector) {
		this.joinTypeSelector = joinTypeSelector;
	}

	public void setup(List<String> pigScripts, String inputDir) {
		pigScripts.clear();
		pigScripts.add(String.format(
				"%s = LOAD '%s' USING %s() AS (S:%s, P:%s, O:%s);", INPUT_VAR,
				inputDir, NTriplesReader.class.getName(), DATATYPE, DATATYPE,
				DATATYPE));
	}

	public TupleSetMetadata evaluate(TupleExpr expr, Executor.Cache cache,
			Path outputLocation, List<String> list, double sample)
			throws QueryEvaluationException, IOException {

		// Start evaluate the expression
		Context context = new Context();
		context.pigQueries = list;
		context.cache = cache;
		context.desiredSampling = sample;
		TupleSetMetadata meta = evaluate(expr, context);

		if (meta.location == null) {
			Path outputLoc = new Path(outputLocation,
					Integer.toString(storageCount++));
			String storageType;
			if (expr instanceof QueryRoot) {
				storageType = "PigStorage('\\t')";
				outputLoc.suffix("-text");
				list.add(String.format("STORE %s INTO '%s' USING %s;\n",
						meta.name, outputLoc.toString() + "-output",
						storageType));
			} else {
				if (sample == 1.0) {
					storageType = "org.apache.hadoop.zebra.pig.TableStorer('')";
					list.add(String.format("STORE %s INTO '%s' USING %s;\n",
							meta.name, outputLoc.toString(), storageType));
				} else {
					list.add(String.format("STORE %s INTO '%s';", meta.name,
							outputLoc.toString()));
				}
			}
			meta.location = outputLoc;
		}
		return meta;
	}

	private TupleSetMetadata evaluate(TupleExpr expr, Context context)
			throws QueryEvaluationException, IOException {

		/* Look in the cache */
		TupleSetMetadata exprValue = context.cache.getFromCache(expr,
				context.desiredSampling);
		if (exprValue != null) {
			context.pigQueries
					.add(String
							.format("%s = LOAD '%s' USING org.apache.hadoop.zebra.pig.TableLoader('');",
									exprValue.name, exprValue.location));
			return exprValue;
		}

		if (expr instanceof QueryRoot) {
			exprValue = physEvaluate((QueryRoot) expr, context);
		} else if (expr instanceof StatementPattern) {
			exprValue = physEvaluate((StatementPattern) expr, context);
		} else if (expr instanceof Projection) {
			exprValue = physEvaluate((Projection) expr, context);
		} else if (expr instanceof NWayJoin) {
			exprValue = physEvaluate((NWayJoin) expr, context);
		} else if (expr instanceof Join) {
			exprValue = physEvaluate((Join) expr, context);
		} else if (expr instanceof MultiProjection) {
			exprValue = physEvaluate((MultiProjection) expr, context);
		} else if (expr instanceof Filter) {
			exprValue = physEvaluate((Filter) expr, context);
		} else if (expr instanceof Extension) {
			exprValue = physEvaluate((Extension) expr, context);
		} else if (expr instanceof Distinct) {
			exprValue = physEvaluate((Distinct) expr, context);
		} else if (expr instanceof LeftJoin) {
			exprValue = physEvaluate((LeftJoin) expr, context);
		} else if (expr instanceof Union) {
			exprValue = physEvaluate((Union) expr, context);
		} else if (expr instanceof Group) {
			exprValue = physEvaluate((Group) expr, context);
		} else if (expr instanceof Slice) {
			exprValue = physEvaluate((Slice) expr, context);
		} else if (expr instanceof Order) {
			exprValue = physEvaluate((Order) expr, context);
		} else {
			throw new QueryEvaluationException("Unsupported expr: " + expr);
		}

		// Future usage of this expression might benefit from knowing the
		// estimated size of this expression.
		if (context.cache.getFromCache(expr) != null) {
			exprValue.size = context.cache.getFromCache(expr).getSize();
		}

		return exprValue;
	}

	/**
	 *
	 * @param meta
	 * @param joinArgs
	 * @param isLeft
	 * @param context
	 * @param doProjection
	 *            whether to project out the unnecessary columns
	 * @param joinVars
	 *            the variables that the arguments share will be put here
	 * @return the metadata of the arguments of the join
	 * @throws QueryEvaluationException
	 * @throws IOException
	 */
	private List<TupleSetMetadata> join(TupleExpr thisJoin,
			TupleSetMetadata meta, List<TupleExpr> joinArgs, boolean isLeft,
			Context context, Set<String> joinVars, boolean doProjection)
			throws QueryEvaluationException, IOException {

		List<TupleSetMetadata> childrenMeta = new ArrayList<TupleSetMetadata>(
				joinArgs.size());

		// Calculate the children expressions and find the common variables.
		double desiredSampling = context.desiredSampling;
		context.desiredSampling = 1.0; // I need to have the children completely
										// calculated if I want to estimate the
										// join cardinality (for bifocal
										// sampling)

		Map<String, List<String>> commonVars = new HashMap<String, List<String>>();
		for (TupleExpr e : joinArgs) {
			TupleSetMetadata childMeta = evaluate(e, context);
			childrenMeta.add(childMeta);

			Set<String> unboundVars = Util.getUnboundExpressionVars(e);
			for (String var : unboundVars) {
				List<String> group = commonVars.get(var);
				if (group == null) {
					group = new ArrayList<String>();
					commonVars.put(var, group);
				}
				group.add(childMeta.name);
			}
		}

		// Restore the desired sampling
		context.desiredSampling = desiredSampling;

		if (commonVars.isEmpty())
			throw new QueryEvaluationException(
					"Could not find common variables for join between: "
							+ joinArgs);

		// Sort to exploit join optimizations
		// In Pig biggest joins go first. Do not rearrange left joins.
		if (!isLeft)
			sortBySizeDesc(childrenMeta);

		/*
		 * Perform the JOIN only on the variables shared by all children. The
		 * other implements with a filter. Also, copy all variables in the
		 * metadata of the results, removing the duplicates.
		 */
		Set<String> filterVars = new HashSet<String>();
		for (Map.Entry<String, List<String>> entry : commonVars.entrySet()) {
			if (entry.getValue().size() == joinArgs.size()) {
				joinVars.add(entry.getKey());
			} else if (entry.getValue().size() > 1) {
				filterVars.add(entry.getKey());
			}
			meta.qualifyName(entry.getKey(), entry.getValue().get(0));
		}

		/********** CROSS (this is nasty but allowed) ************/
		if (joinVars.isEmpty()) {
			doCross(meta, context, childrenMeta);
			return childrenMeta;
		}

		if (context.desiredSampling < 1.0) {
			// Write code to perform bifocal sampling
			meta.sampleRate = context.desiredSampling;

			if (childrenMeta.size() > 2) {
				throw new QueryEvaluationException(
						"Bifocal sampling works only with two children!");
			}

			// First calculate a subset of tuples that are dense on both sides
			String child1 = childrenMeta.get(0).name;
			String child2 = childrenMeta.get(1).name;
			String joinColumns = TupleSetMetadata
					.constructQualifiedColumnString(joinVars,
							childrenMeta.get(0), "");
			List<String> list = context.pigQueries;

			// Sample the two children
			list.add(String.format("BS1%s = SAMPLE %s %f;", meta.name, child1,
					context.desiredSampling));
			list.add(String.format("BS2%s = SAMPLE %s %f;", meta.name, child2,
					context.desiredSampling));

			// Group them
			list.add(String.format("BSG1%s = GROUP BS1%s BY %s;", meta.name,
					meta.name, joinColumns));
			list.add(String.format("BSG2%s = GROUP BS2%s BY %s;", meta.name,
					meta.name, joinColumns));

			// Count them
			list.add(String
					.format("BSC1%s = FOREACH BSG1%s GENERATE flatten(group), COUNT(BS1%s) as c;",
							meta.name, meta.name, meta.name));
			list.add(String
					.format("BSC2%s = FOREACH BSG2%s GENERATE flatten(group), COUNT(BS2%s) as c;",
							meta.name, meta.name, meta.name));

			// Remove the "group" prefix
			String replacement = "";
			if (joinVars.size() == 1) {
				replacement = "group as " + joinColumns + ",";
			} else {
				for (String var : joinVars) {
					replacement += "group::" + var + " as " + var + ",";
				}
			}

			list.add(String.format(
					"BSC1%s = FOREACH BSC1%s GENERATE %s c as c;", meta.name,
					meta.name, replacement));
			list.add(String.format(
					"BSC2%s = FOREACH BSC2%s GENERATE %s c as c;", meta.name,
					meta.name, replacement));

			// Split between popular and not popular
			long threshold = Configuration.getInstance().getPropertyInt(
					Configuration.DENSE_SAMPLING_THRESHOLD);
			list.add(String
					.format("SPLIT BSC1%s INTO BSC1P%s IF (long)c>=(long)%d, BSC1NP%s IF (long)c<(long)%d;",
							meta.name, meta.name, threshold, meta.name,
							threshold));
			list.add(String
					.format("SPLIT BSC2%s INTO BSC2P%s IF (long)c>=(long)%d, BSC2NP%s IF (long)c<(long)%d;",
							meta.name, meta.name, threshold, meta.name,
							threshold));

			// Join dense
			list.add(String.format(
					"BSDJ%s = JOIN BSC1P%s BY %s, BSC2P%s BY %s using 'replicated';", meta.name,
					meta.name, joinColumns, meta.name, joinColumns));
			list.add(String
					.format("BSDJ%s = FOREACH BSDJ%s GENERATE BSC1P%s::c as C1, BSC2P%s::c as C2;",
							meta.name, meta.name, meta.name, meta.name));

			// Sparse_any
			list.add(String.format(
					"BSNDJ1%s = JOIN BSC2%s BY %s, BSC1NP%s BY %s;", meta.name,
					meta.name, joinColumns, meta.name, joinColumns));
			list.add(String
					.format("BSNDJ1%s = FOREACH BSNDJ1%s GENERATE BSC1NP%s::c as C1, BSC2%s::c as C2;",
							meta.name, meta.name, meta.name, meta.name));

			// Any_sparse
			list.add(String.format(
					"BSNDJ2%s = JOIN BSC1%s BY %s, BSC2NP%s BY %s;", meta.name,
					meta.name, joinColumns, meta.name, joinColumns));
			list.add(String
					.format("BSNDJ2%s = FOREACH BSNDJ2%s GENERATE BSC1%s::c as C1, BSC2NP%s::c as C2;",
							meta.name, meta.name, meta.name, meta.name));

			// Union
			list.add(String.format(
					"BSU%s = UNION ONSCHEMA BSDJ%s, BSNDJ1%s, BSNDJ2%s;",
					meta.name, meta.name, meta.name, meta.name));

			// Multiplication
			list.add(String.format(
					"BS%s = FOREACH BSU%s GENERATE C1*C2 as C;\n", meta.name,
					meta.name));

			meta.name = "BS" + meta.name;

		} else { // Use the metainfo on the children to determine what is the
					// best type of join to perform
			meta.sampleRate = 1.0;
			// Do I have a sample of this join that I can use for the join
			// selection?
			TupleSetMetadata sampledJoin = context.cache
					.getFromCacheWithSamplingLessThan(thisJoin, 1.0);

			JoinType joinType = joinTypeSelector.selectTypeOfJoin(sampledJoin,
					childrenMeta);
			if (joinType.equals(JoinType.LOADBALANCED)) {

				/********** LOAD BALANCED JOIN **********/

				String firstAlias = childrenMeta.get(0).name;
				String joinColumns = TupleSetMetadata
						.constructQualifiedColumnString(joinVars,
								childrenMeta.get(0), "");
				if (commonVars.size() == 1)
					joinColumns = joinColumns.substring(1,
							joinColumns.length() - 1);

				String otherAliases = "";
				boolean first = true;
				for (TupleSetMetadata s : childrenMeta) {
					if (first) {
						first = false;
						continue;
					}
					otherAliases += " " + s.name + " BY (";
					for (String v : joinVars) {
						otherAliases += s.getQualifiedVariableNames().get(v)
								+ ",";
					}
					otherAliases = Util.clearTrailingCommas(otherAliases)
							+ "),";
				}
				otherAliases = Util.clearTrailingCommas(otherAliases);

				Set<String> allVars1 = new HashSet<String>(meta
						.getQualifiedVariableNames().keySet());
				String restColumns = TupleSetMetadata
						.makeQualifiedVariableString(allVars1, childrenMeta,
								true);

				List<String> list = context.pigQueries;
				list.add(String.format(
						"sample%s = SAMPLE %s %s;\n",
						meta.name,
						firstAlias,
						Configuration.getInstance().getPropertyInt(
								Configuration.SAMPLE_RATE)));
				list.add(String.format("G%s = GROUP sample%s BY %s;\n",
						meta.name, meta.name, joinColumns));
				list.add(String
						.format("C%s = FOREACH G%s GENERATE COUNT_STAR(sample%s), group AS %s;\n",
								meta.name, meta.name, meta.name, joinColumns));
				list.add(String.format("O%s = ORDER C%s BY $0 DESC;\n",
						meta.name, meta.name));
				list.add(String.format(
						"popular%s = LIMIT O%s %s;\n",
						meta.name,
						meta.name,
						Configuration.getInstance().get(
								Configuration.INMEMORY_THRESHOLD)));
				list.add(String.format(
						"popular%s = FOREACH popular%s GENERATE %s AS %s;\n",
						meta.name, meta.name, joinColumns, joinColumns));
				list.add(String
						.format("splitjoin%s = JOIN %s BY %s LEFT, popular%s BY %s using 'replicated';\n",
								meta.name, firstAlias, joinColumns, meta.name,
								joinColumns));
				String qualifiedJoinColumns = TupleSetMetadata
						.constructQualifiedColumnString(joinVars,
								childrenMeta.get(0), "popular" + meta.name);
				list.add(String
						.format("SPLIT splitjoin%s INTO replicatedInput%s IF %s is not null, nonreplicatedInput%s IF %s is null;\n",
								meta.name, meta.name, qualifiedJoinColumns,
								meta.name, qualifiedJoinColumns));
				list.add(String.format(
						"restJOIN%s = JOIN %s, popular%s BY %s;\n", meta.name,
						otherAliases, meta.name, joinColumns));
				list.add(String
						.format("replicatedJoin%s = JOIN replicatedInput%s BY %s::%s %s, restJOIN%s BY popular%s::%s using 'replicated';\n",
								meta.name, meta.name, firstAlias, joinColumns,
								isLeft ? "LEFT" : "", meta.name, meta.name,
								joinColumns));
				list.add(String
						.format("replicatedJoinP%s = FOREACH replicatedJoin%s GENERATE %s ;\n",
								meta.name, meta.name, restColumns));

				list.add(String
						.format("nonreplicatedJoin%s = JOIN nonreplicatedInput%s BY %s::%s %s, %s;\n",
								meta.name, meta.name, firstAlias, joinColumns,
								isLeft ? "LEFT" : "", otherAliases));
				list.add(String
						.format("nonreplicatedJoinP%s = FOREACH nonreplicatedJoin%s GENERATE %s ;\n",
								meta.name, meta.name, restColumns));

				list.add(String
						.format("%s = UNION ONSCHEMA replicatedJoinP%s, nonreplicatedJoinP%s;\n",
								meta.name, meta.name, meta.name));

				meta.resetQualifications();

			} else {

				/********** NORMAL OR REPLICATED JOIN **********/

				String pigQuery = String.format("%s = JOIN", meta.name);
				boolean isFirstArg = true;
				for (TupleSetMetadata s : childrenMeta) {
					pigQuery += " " + s.name + " BY (";
					for (String v : joinVars) {
						pigQuery += s.getQualifiedVariableNames().get(v) + ",";
					}
					pigQuery = Util.clearTrailingCommas(pigQuery) + "),";
					if (isFirstArg) {
						isFirstArg = false;
						if (isLeft)
							pigQuery = Util.clearTrailingCommas(pigQuery)
									+ " LEFT,";
					}
				}
				pigQuery = Util.clearTrailingCommas(pigQuery)
						+ (joinType.equals(JoinType.REPLICATED) ? " using 'replicated'"
								: "") + ";";

				List<String> list = context.pigQueries;
				list.add(pigQuery);

				String filterJoin = "";
				for (String filterVar : filterVars) {
					List<String> group = commonVars.get(filterVar);
					String first = group.get(0);
					for (int i = 1; i < group.size(); ++i) {
						String current = group.get(i);
						filterJoin += first + " == " + current + " AND ";
					}
				}
				if (filterJoin.length() > 0) {
					filterJoin = filterJoin.substring(0,
							filterJoin.length() - 5);
					list.add(String.format("%sAF = FILTER %s BY %s", meta.name,
							meta.name, filterJoin));
					meta.name += "AF";
				}

				if (doProjection) {
					// Project the results of the join to remove duplicated
					// columns
					String projections = "";
					for (String s : meta.getQualifiedVariableNames().keySet()) {
						projections += meta.getQualifiedVariable(s) + " AS "
								+ s + ",";
						meta.addUnqualifiedName(s);
					}
					list.add(String.format("%sP = FOREACH %s GENERATE %s;",
							meta.name, meta.name,
							Util.clearTrailingCommas(projections)));
					meta.name += "P";
				}
			}
		}

		/*
		 * // Update sampling rate for (TupleSetMetadata ts : childrenMeta) {
		 * meta.sampleRate *= ts.sampleRate; }
		 */

		return childrenMeta;
	}

	/**
	 * Do cross-product (used when we have a join with no variables in common.
	 * It should be avoided.
	 *
	 * @param meta
	 * @param context
	 * @param childrenMeta
	 */
	private void doCross(TupleSetMetadata meta, Context context,
			List<TupleSetMetadata> childrenMeta) {
		String crossAliases = "";
		for (TupleSetMetadata m : childrenMeta) {
			crossAliases += m.name + ",";
		}
		context.pigQueries.add(String.format("%s = CROSS %s;\n", meta.name,
				Util.clearTrailingCommas(crossAliases)));

		// Remove qualifications
		meta.qualifiedVariableNames = new HashMap<String, String>();

		// Project the results of the join to remove duplicated columns
		String projections = "";
		for (TupleSetMetadata t : childrenMeta)
			for (String s : t.getQualifiedVariableNames().keySet()) {
				projections += t.getQualifiedVariable(s) + " AS " + s + ",";
				meta.addUnqualifiedName(s);
			}
		context.pigQueries.add(String.format("%sP = FOREACH %s GENERATE %s;\n",
				meta.name, meta.name, Util.clearTrailingCommas(projections)));
		meta.name += "P";
	}

	protected void sortBySizeAsc(List<TupleSetMetadata> metas) {
		Collections.sort(metas, new Comparator<TupleSetMetadata>() {
			@Override
			public int compare(TupleSetMetadata o1, TupleSetMetadata o2) {
				if (o1.size == o2.size)
					return 0;
				else
					return o1.size > o2.size ? 1 : -1;
			}
		});
	}

	protected void sortBySizeDesc(List<TupleSetMetadata> metas) {
		Collections.sort(metas, new Comparator<TupleSetMetadata>() {
			@Override
			public int compare(TupleSetMetadata o1, TupleSetMetadata o2) {
				if (o1.size == o2.size)
					return 0;
				else
					return o1.size > o2.size ? -1 : 1;
			}
		});
	}

	private TupleSetMetadata physEvaluate(Join expr, Context context)
			throws QueryEvaluationException, IOException {
		TupleSetMetadata meta = new TupleSetMetadata("JOIN");
		TupleExpr leftArg = expr.getLeftArg();
		TupleExpr rightArg = expr.getRightArg();
		List<TupleExpr> joinArgs = Arrays.asList(leftArg, rightArg);
		join(expr, meta, joinArgs, false, context, new HashSet<String>(), true);
		return meta;
	}

	private TupleSetMetadata physEvaluate(NWayJoin expr, Context context)
			throws QueryEvaluationException, IOException {
		TupleSetMetadata meta = new TupleSetMetadata("NJOIN");
		join(expr, meta, expr.getArgs(), false, context, new HashSet<String>(),
				true);
		return meta;
	}

	private TupleSetMetadata physEvaluate(LeftJoin expr, Context context)
			throws QueryEvaluationException, IOException {
		TupleSetMetadata meta = new TupleSetMetadata("LJOIN");
		List<TupleExpr> joinArgs = Arrays.asList(expr.getLeftArg(),
				expr.getRightArg());
		join(expr, meta, joinArgs, true, context, new HashSet<String>(), true);
		return meta;
	}

	private TupleSetMetadata physEvaluate(Distinct distinct, Context context)
			throws QueryEvaluationException, IOException {
		if (context.desiredSampling < 1.0) {
			throw new IOException(
					"This operator is not supported when sampling is less than 1.0");
		}
		TupleSetMetadata childMeta = evaluate(distinct.getArg(), context);
		TupleSetMetadata meta = new TupleSetMetadata("DIST");
		meta.qualifiedVariableNames.putAll(childMeta.qualifiedVariableNames);
		meta.sampleRate = childMeta.sampleRate;
		context.pigQueries.add(String.format("%sD = DISTINCT %s;", meta.name,
				childMeta.name));
		meta.name += "D";
		return meta;
	}

	private TupleSetMetadata physEvaluate(StatementPattern expr, Context context)
			throws QueryEvaluationException, IOException {

		TupleSetMetadata meta = new TupleSetMetadata("SP");
		List<String> query = new LinkedList<String>();

		// First set a filter on the bounded variables
		String boundVariables = "";
		if (expr.getSubjectVar().getValue() != null) {
			boundVariables += "S eq \'"
					+ Util.formatValue(expr.getSubjectVar().getValue())
					+ "\' and ";
		}

		if (expr.getPredicateVar().getValue() != null) {
			boundVariables += "P eq \'"
					+ Util.formatValue(expr.getPredicateVar().getValue())
					+ "\' and ";
		}

		if (expr.getObjectVar().getValue() != null) {
			boundVariables += "O eq \'"
					+ Util.formatValue(expr.getObjectVar().getValue()) + "\'";
		}

		if (boundVariables.endsWith(" and ")) {
			boundVariables = boundVariables.substring(0,
					boundVariables.length() - 5);
		}

		String input = INPUT_VAR;
		if (boundVariables.length() > 0) {
			query.add(String.format("%sP = FILTER %s BY %s;", meta.name,
					INPUT_VAR, Util.clearTrailingCommas(boundVariables)));
			input = meta.name + "P";
		}

		String projections = "";
		String filter = "";

		// Subject
		if (expr.getSubjectVar().getValue() == null) {
			projections += "S AS " + expr.getSubjectVar().getName() + ", ";
		}

		// Predicate
		if (expr.getPredicateVar().getValue() == null) {
			if (!expr.getPredicateVar().getName()
					.equals(expr.getSubjectVar().getName())) {
				projections += "P AS " + expr.getPredicateVar().getName()
						+ ", ";
			} else { // Add filter expression to filter subj = pred
				filter = "S == P and ";
			}
		}

		// Object
		if (expr.getObjectVar().getValue() == null) {
			if (expr.getObjectVar().getName()
					.equals(expr.getSubjectVar().getName())) {
				filter += "S == O";
			} else if (expr.getObjectVar().getName()
					.equals(expr.getPredicateVar().getName())) {
				filter += "P == O";
			} else {
				projections += "O AS " + expr.getObjectVar().getName();
			}
		}

		// Clean filters
		if (filter.endsWith("and "))
			filter = filter.substring(0, filter.length() - 4);
		projections = Util.clearTrailingCommas(projections);

		if (filter.length() > 0) {
			query.add(String.format("%sFE = FILTER %s BY %s;", meta.name,
					input, filter));
			input = meta.name + "FE";
		}

		if (projections.length() > 0) {
			query.add(String.format("%sP = FOREACH %s GENERATE %s;", meta.name,
					input, projections)); // project
			input = meta.name + "P";
		}

		for (Var v : expr.getVarList()) {
			if (v.getValue() == null)
				meta.addUnqualifiedName(v.getName());
		}

		String distinctPreprocessing = (String) Configuration.getInstance()
				.get(Configuration.DISTINCT_PREPROCESSING);
		if (distinctPreprocessing != null
				&& distinctPreprocessing.equalsIgnoreCase("true")) {
			query.add(String.format("%sD = DISTINCT %s;", meta.name, input));
			meta.name += "D";
		} else {
			meta.name += "P";
		}

		context.pigQueries.addAll(query);

		/* Sample */
		if (context.desiredSampling < 1.0) {
			context.pigQueries.add(String.format("%sS = SAMPLE %s %f;",
					meta.name, meta.name, context.desiredSampling));
			context.pigQueries.add(String.format("%sSC = GROUP %sS ALL;",
					meta.name, meta.name));
			context.pigQueries.add(String.format(
					"%sSCF = FOREACH %sSC GENERATE COUNT(%sS);", meta.name,
					meta.name, meta.name));
			meta.name += "SCF";

			meta.sampleRate = context.desiredSampling;
		}

		return meta;
	}

	private TupleSetMetadata physEvaluate(Group expr, Context context)
			throws QueryEvaluationException, IOException {

		if (context.desiredSampling < 1.0) {
			throw new IOException(
					"This operator is not supported when sampling is less than 1.0");
		}

		TupleSetMetadata child = evaluate(expr.getArg(), context);
		TupleSetMetadata meta = new TupleSetMetadata("GROUP");
		meta.sampleRate = child.sampleRate;

		String c = "";
		int i = 0;
		Map<ValueExpr, String> calculatedExpressions = new HashMap<ValueExpr, String>();

		// Calculate complex expressions like casts
		for (GroupElem ge : expr.getGroupElements()) {
			if (!valueExprGen.isSimpleExpression(ge.getOperator())) {
				ValueExpr ve=valueExprGen.getAggregateOperatorArgument(ge.getOperator());
				String complExpr = valueExprGen.getValue(ve, child);
				String projectedColumn = "projected" + (i++);
				String projectedColumnWithAggregate = valueExprGen.getAggregateOperatorAsString(ge.getOperator()) + "("+ child.name +"C." + projectedColumn + ")";
				c+=" " + complExpr + " AS " + projectedColumn + ",";
				calculatedExpressions.put(ge.getOperator(), projectedColumnWithAggregate);
			}
		}
		if (!c.isEmpty()) {
			String existingColumns = "";
			for (String v : child.getQualifiedVariableNames().values()) {
				existingColumns += " " + v + ",";
			}
			context.pigQueries.add(String.format(
					"%sC = FOREACH %s GENERATE %s, %s;\n", child.name,
					child.name, Util.clearTrailingCommas(existingColumns),
					Util.clearTrailingCommas(c)));
			child.name += "C";
		}

		// Generate grouping statement //
		String group = "";
		for (String ge : expr.getGroupBindingNames()) {
			group += ge + ", ";
			meta.addUnqualifiedName(ge);
		}
		if (group.isEmpty())
			context.pigQueries.add(String.format("%sG = GROUP %s ALL;\n", meta.name,
					child.name));
		else {
			group = "(" + group.substring(0, group.length() - 2) + ")";
			context.pigQueries.add(String.format("%sG = GROUP %s BY %s;\n", meta.name,
					child.name, group));
		}

		TupleSetMetadata groupMeta = new TupleSetMetadata();
		for (String v : child.getQualifiedVariableNames().keySet()) {
			groupMeta.qualifiedVariableNames.put(v, child.name + "." + v);
		}

		// Generate aggregate statement //
		String allAggr = "";
		for (GroupElem ge : expr.getGroupElements()) {
			String v = calculatedExpressions.get(ge.getOperator());
			if (v == null)
				v = valueExprGen.getValue(ge.getOperator(), groupMeta, true);
			// String aggr = qualifyAggregateExpr(child, v);

			String projectedName = Util.ensurePigFriendlyName(ge.getName());
			allAggr += v + " AS " + projectedName + " ,";
			meta.addUnqualifiedName(projectedName);
		}

		String nq;
		if (group.isEmpty())
			nq = String.format(
					"%s = FOREACH %sG GENERATE %s;\n",
					meta.name, meta.name, Util.clearTrailingCommas(allAggr));
		else
			nq = String.format(
				"%s = FOREACH %sG GENERATE %s FLATTEN(group) AS %s;\n",
				meta.name, meta.name, allAggr, group);

		context.pigQueries.add(nq);

		return meta;
	}

	private TupleSetMetadata physEvaluate(Slice expr, Context context)
			throws QueryEvaluationException, IOException {

		if (context.desiredSampling < 1.0) {
			throw new IOException(
					"This operator is not supported when sampling is less than 1.0");
		}

		TupleSetMetadata child = evaluate(expr.getArg(), context);

		if (expr.getOffset() > 0)
			throw new QueryEvaluationException("Offset not supported");
		TupleSetMetadata meta = new TupleSetMetadata("SLICE");
		meta.sampleRate = child.sampleRate;
		meta.setQualifiedVariableNames(child.qualifiedVariableNames);

		context.pigQueries.add(String.format("%s = LIMIT %s %s;\n", meta.name,
				child.name, expr.getLimit()));

		return meta;
	}

	private TupleSetMetadata physEvaluate(Order expr, Context context)
			throws QueryEvaluationException, IOException {

		if (context.desiredSampling < 1.0) {
			throw new IOException(
					"This operator is not supported when sampling is less than 1.0");
		}

		TupleSetMetadata child = evaluate(expr.getArg(), context);
		TupleSetMetadata meta = new TupleSetMetadata("ORDER");
		meta.sampleRate = child.sampleRate;
		String readAlias = child.name;

		// Check for expressions
		boolean hasExpressions = false;
		for (OrderElem e : expr.getElements())
			if (!(e.getExpr() instanceof Var)) {
				hasExpressions = true;
				break;
			}

		int newAliasCounter = 0;
		Map<ValueExpr, String> doneAliases = new HashMap<ValueExpr, String>();
		if (hasExpressions) {
			String projections = "";
			for (String s : child.getQualifiedVariableNames().keySet()) {
				projections += child.getQualifiedVariable(s) + " AS " + s + ",";
				meta.addUnqualifiedName(s);
			}
			for (OrderElem e : expr.getElements()) {
				if (!(e.getExpr() instanceof Var)) {
					String newAlias = meta.name + "P" + newAliasCounter++;
					doneAliases.put(e.getExpr(), newAlias);
					projections += valueExprGen.getValue(e.getExpr(), child)
							+ " AS " + newAlias + ",";
				}
			}
			context.pigQueries.add(String.format(
					"%sP = FOREACH %s GENERATE %s;\n", meta.name, child.name,
					Util.clearTrailingCommas(projections)));
			readAlias = meta.name + "P";
		}
		else
			meta.setQualifiedVariableNames(child.getQualifiedVariableNames());

		String order = "";
		for (OrderElem e : expr.getElements()) {
			String alias = doneAliases.get(e.getExpr());
			if (alias == null)
				alias = valueExprGen.getValue(e.getExpr(), meta);
			order += alias + " " + (e.isAscending() ? "ASC" : "DESC") + ", ";
		}
		order = order.substring(0, order.length() - 2);

		context.pigQueries.add(String.format("%s = ORDER %s BY %s;\n",
				meta.name, readAlias, order));
		meta.qualifiedVariableNames = new HashMap<String, String>(
				child.qualifiedVariableNames);
		return meta;
	}

	private TupleSetMetadata physEvaluate(Projection expr, Context context)
			throws QueryEvaluationException, IOException {
		TupleSetMetadata meta = new TupleSetMetadata("PROJECTION");
		TupleSetMetadata childMeta = evaluate(expr.getArg(), context);
		meta.sampleRate = childMeta.sampleRate; // Projection does not change
												// sampleRate

		if (checkDoProjection(expr, childMeta)) {
			String projection = "";
			for (ProjectionElem e : expr.getProjectionElemList().getElements()) {
				projection += childMeta.getQualifiedVariable(TupleSetMetadata
						.cleanVariableName(e.getSourceName()))
						+ " AS "
						+ TupleSetMetadata.cleanVariableName(e.getTargetName())
						+ ",";
				meta.addUnqualifiedName(TupleSetMetadata.cleanVariableName(e
						.getTargetName()));
			}
			context.pigQueries.add(String.format(
					"%s%s = FOREACH %s GENERATE %s;\n", meta.name, "",
					childMeta.name, Util.clearTrailingCommas(projection)));
		} else
			meta = childMeta;
		return meta;
	}

	/**
	 * Check whether projection actually influences the schema
	 *
	 * @param expr
	 * @param childMeta
	 * @return
	 */
	private boolean checkDoProjection(Projection expr,
			TupleSetMetadata childMeta) {
		if (expr.getProjectionElemList().getElements().size() != childMeta.qualifiedVariableNames
				.entrySet().size())
			return true;

		for (ProjectionElem e : expr.getProjectionElemList().getElements()) {
			String qualifiedVariable = childMeta
					.getQualifiedVariable(TupleSetMetadata.cleanVariableName(e
							.getSourceName()));
			if (qualifiedVariable != null
					&& !qualifiedVariable.equals(TupleSetMetadata
							.cleanVariableName(e.getTargetName())))
				return true;
		}
		return false;
	}

	private String buildProjectionQuery(TupleSetMetadata childMeta,
			TupleSetMetadata meta, ProjectionElemList projectionElemList,
			String namePostfix) {

		String projection = "";

		for (ProjectionElem e : projectionElemList.getElements()) {
			projection += childMeta.getQualifiedVariable(TupleSetMetadata
					.cleanVariableName(e.getSourceName()))
					+ " AS "
					+ TupleSetMetadata.cleanVariableName(e.getTargetName())
					+ ",";
			meta.addUnqualifiedName(TupleSetMetadata.cleanVariableName(e
					.getSourceName()));
		}
		return String.format("%s%s = FOREACH %s GENERATE %s;\n", meta.name,
				namePostfix, childMeta.name,
				Util.clearTrailingCommas(projection));
	}

	private TupleSetMetadata physEvaluate(MultiProjection multiProjection,
			Context context) throws QueryEvaluationException, IOException {
		if (!(multiProjection.getParentNode() instanceof QueryRoot)
				&& !(multiProjection.getParentNode().getParentNode() instanceof QueryRoot))
			throw new QueryEvaluationException(
					"Expected multi-projection to be at the top of the query tree");

		TupleSetMetadata childMeta = evaluate(multiProjection.getArg(), context);
		TupleSetMetadata meta = new TupleSetMetadata(multiProjection);
		meta.sampleRate = childMeta.sampleRate;

		String forUnion = "";
		int i = 0;
		for (ProjectionElemList pel : multiProjection.getProjections()) {
			String q = buildProjectionQuery(childMeta, meta, pel, "_" + i);
			forUnion += meta.name + "_" + i + ",";
			i++;
			context.pigQueries.add(q);
		}

		String unionQuery = String.format("%s=UNION ONSCHEMA %s;\n", meta.name,
				Util.clearTrailingCommas(forUnion));
		context.pigQueries.add(unionQuery);

		return meta;
	}

	private void unions(TupleExpr expr, List<TupleSetMetadata> children,
			Context context) throws QueryEvaluationException, IOException {
		if (expr instanceof Union) {
			Union u = (Union) expr;
			unions(u.getLeftArg(), children, context);
			unions(u.getRightArg(), children, context);
		} else
			children.add(evaluate(expr, context));
	}

	private TupleSetMetadata physEvaluate(Union expr, Context context)
			throws QueryEvaluationException, IOException {

		TupleSetMetadata meta = new TupleSetMetadata("UNION");
		ArrayList<TupleSetMetadata> childrenUnions = new ArrayList<TupleSetMetadata>();

		// Calculate all the expressions of the children
		unions(expr, childrenUnions, context);

		String childrenS = "";
		for (TupleSetMetadata t : childrenUnions)
			childrenS += t.name + ",";
		childrenS = Util.clearTrailingCommas(childrenS);

		String pigQuery = "";
		pigQuery += meta.name + " = UNION ONSCHEMA " + childrenS;

		meta.sampleRate = context.desiredSampling;
		for (TupleSetMetadata m : childrenUnions) {
			for (String v : m.getQualifiedVariableNames().keySet())
				meta.addUnqualifiedName(v);
		}
		context.pigQueries.add(Util.clearTrailingCommas(pigQuery) + ";\n");

		return meta;
	}

	private TupleSetMetadata physEvaluate(Extension extension, Context context)
			throws QueryEvaluationException, IOException {
		TupleSetMetadata child = evaluate(extension.getArg(), context);

		// If the child is a GROUP, do nothing
		if (extensionHasGroupDescendent(extension.getArg()))
			return child;
		else { // Else, do the extension
			TupleSetMetadata meta = new TupleSetMetadata(extension);
			meta.sampleRate = child.sampleRate;

			String extElems = "";
			for (ExtensionElem e : extension.getElements()) {
				extElems += valueExprGen.getValue(e.getExpr(), child) + " AS "
						+ TupleSetMetadata.cleanVariableName(e.getName())
						+ " ,";
				meta.addUnqualifiedName(TupleSetMetadata.cleanVariableName(e
						.getName()));
			}

			for (Entry<String, String> v : child.getQualifiedVariableNames()
					.entrySet()) {
				extElems += v.getValue() + " AS " + v.getKey() + ",";
				meta.addUnqualifiedName(v.getKey());
			}

			context.pigQueries.add(String.format(
					"%s = FOREACH %s GENERATE  %s;\n", meta.name, child.name,
					Util.clearTrailingCommas(extElems)));
			return meta;
		}
	}

	private boolean extensionHasGroupDescendent(TupleExpr expr) {
		if (expr instanceof Filter)
			return extensionHasGroupDescendent(((Filter) expr).getArg());
		else if (expr instanceof Group)
			return true;
		else if (expr instanceof Order)
			return extensionHasGroupDescendent(((Order) expr).getArg());
		else if (expr instanceof Extension)
			return extensionHasGroupDescendent(((Extension) expr).getArg());
		else
			return false;
	}

	private TupleSetMetadata physEvaluate(QueryRoot expr, Context context)
			throws QueryEvaluationException, IOException {
		return evaluate(expr.getArg(), context);
	}

	private TupleSetMetadata physEvaluate(Filter filter, Context context)
			throws QueryEvaluationException, IOException {

		if (context.desiredSampling < 1.0) {
			throw new IOException(
					"This operator is not supported when sampling is less than 1.0");
		}

		// Handle EXISTS (similar to inner join, but project out the results)
		if (filter.getCondition() instanceof Exists)
			throw new QueryEvaluationException("Exists not supported, use an inner join instead");

		// Handle NOT EXISTS (do a left join, and then filter the results)
		if (filter.getCondition() instanceof Not
				&& ((Not) filter.getCondition()).getArg() instanceof Exists) {
			// Do left join
			TupleExpr cond = ((Exists) ((Not) filter.getCondition()).getArg())
					.getSubQuery();
			TupleSetMetadata meta = new TupleSetMetadata("NOTEXISTS");
			List<TupleExpr> joinArgs = Arrays.asList(filter.getArg(), cond);
			Set<String> joinVars = new HashSet<String>();
			// FIXME: Jacopo: I am not sure filter represents the join we want
			// to make
			List<TupleSetMetadata> lm = join(filter, meta, joinArgs, true,
					context, joinVars, false);
			TupleSetMetadata childMeta = lm.get(0);
			TupleSetMetadata optionalMeta = lm.get(1);

			String variableForOptional = joinVars.iterator().next();

			// Filter
			String filterSt = String.format(
					"%sF = FILTER %s BY %s::%s is null;", meta.name, meta.name,
					optionalMeta.name,
					optionalMeta.getQualifiedVariable(variableForOptional));
			context.pigQueries.add(filterSt);
			meta.name += "F";

			// Project only on the variables of the argument of the join
			String projections = "";
			for (String s : childMeta.getQualifiedVariableNames().keySet()) {
				projections += meta.getQualifiedVariable(s) + " AS " + s + ",";
				meta.addUnqualifiedName(s);
			}
			context.pigQueries.add(String.format(
					"%sP = FOREACH %s GENERATE %s;", meta.name, meta.name,
					Util.clearTrailingCommas(projections)));
			meta.name += "P";

			return meta;
		} else {
			TupleSetMetadata childMeta = evaluate(filter.getArg(), context);
			TupleSetMetadata meta = new TupleSetMetadata(filter, childMeta);
			meta.sampleRate = childMeta.sampleRate;
			String childName = childMeta.name;
			String value = valueExprGen.getValue(filter.getCondition(),
					childMeta);

			context.pigQueries.add(String.format("%s = FILTER %s BY %s;",
					meta.name, childName, value));
			return meta;
		}

	}

}
