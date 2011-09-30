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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.tools.pigstats.OutputStats;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.larkc.RDFPig.Configuration;
import eu.larkc.RDFPig.costCalculators.JoinTypeSelector;

/**
 * Notes: The PigServer can actually directly read a file using
 * PigServer.openIterator
 *
 *
 */
public class Executor {

	protected static Logger logger = LoggerFactory.getLogger(Executor.class);

	private StatisticsCollector stats;

	Path outputDir = null;
	Path inputDir = null;
	JoinTypeSelector joinTypeSelector = new JoinTypeSelector();

	/* Abstract the class with a new object */
	public Cache cache = new Cache();

	public class Cache {

		private Map<TupleExpr, List<TupleSetMetadata>> storedExpressions = new TreeMap<TupleExpr, List<TupleSetMetadata>>(
				new TupleExprComparator());

		public TupleSetMetadata getFromCache(TupleExpr expr) {
			List<TupleSetMetadata> values = storedExpressions.get(expr);
			if (values == null || values.isEmpty())
				return null;
			TupleSetMetadata max = values.get(0);
			for (TupleSetMetadata t : values)
				if (t.sampleRate > max.sampleRate)
					max = t;
			return max;
		}

		public TupleSetMetadata getFromCache(TupleExpr expr, double sample) {
			List<TupleSetMetadata> values = storedExpressions.get(expr);
			if (values != null) {
				for (TupleSetMetadata value : values) {
					if (value.sampleRate >= sample) {
						return value;
					}
				}
			}
			return null;
		}

		public TupleSetMetadata getFromCacheWithSamplingLessThan(
				TupleExpr expr, double sample) {
			List<TupleSetMetadata> values = storedExpressions.get(expr);
			if (values != null) {
				for (TupleSetMetadata value : values) {
					if (value.sampleRate < sample) {
						return value;
					}
				}
			}
			return null;
		}

		public void putInCache(TupleExpr expr, TupleSetMetadata meta) {
			List<TupleSetMetadata> list = storedExpressions.get(expr);
			if (list == null) {
				list = new ArrayList<TupleSetMetadata>();
				storedExpressions.put(expr, list);
			}
			list.add(meta);
		}
	}

	public Executor(String inDir, String outDir) throws IOException {
		inputDir = new Path(inDir);
		outputDir = new Path(outDir);

		// Clean the output directory
		FileSystem fs = FileSystem
				.get(new org.apache.hadoop.conf.Configuration());
		fs.delete(outputDir, true);
		this.stats = new StatisticsCollector();
	}

	/********* PUBLIC METHOD ***********/
	public Map<TupleExpr, TupleSetMetadata> execute(
			Collection<? extends TupleExpr> input, double sampleRate)
			throws IOException, QueryEvaluationException {
		Map<TupleExpr, TupleSetMetadata> output = new HashMap<TupleExpr, TupleSetMetadata>();

		PigQueriesGenerator gen = new PigQueriesGenerator(this,
				joinTypeSelector);
		List<String> pigScripts = new LinkedList<String>();
		gen.setup(pigScripts, inputDir.toString());

		Map<TupleExpr, TupleSetMetadata> toExecuteExpressions = new HashMap<TupleExpr, TupleSetMetadata>();
		for (TupleExpr expr : input) {
			TupleSetMetadata info = cache.getFromCache(expr, sampleRate);
			if (info != null) {
				logger.info(expr + " found in cache");
				output.put(expr, info);
			} else {
				info = gen.evaluate(expr, cache, outputDir, pigScripts,
						sampleRate);
				toExecuteExpressions.put(expr, info);
			}
		}

		// If pigScripts is greater than 0 than need to execute the scripts
		if (pigScripts.size() > 0) {
			logger.info("Running using " + pigScripts.size()
					+ " pig statements");
			printScripts(pigScripts);

			List<ExecJob> jobs = executePigScripts(pigScripts);

			long startTime = System.currentTimeMillis();
			// Update the size of the expressions
			for (Map.Entry<TupleExpr, TupleSetMetadata> entry : toExecuteExpressions
					.entrySet()) {
				if (sampleRate == 1.0) {
					// Get the size of the expression
					for (ExecJob job : jobs) {
						for (OutputStats stats : job.getStatistics()
								.getOutputStats()) {
							if (stats.getAlias().equals(entry.getValue().name)) {
								entry.getValue().size = stats
										.getNumberRecords();
							}
						}
					}
					output.put(entry.getKey(), entry.getValue());
				} else {
					long count = 0;

					FileSystem fs = FileSystem
							.get(new org.apache.hadoop.conf.Configuration());
					FileStatus[] files = fs
							.listStatus(entry.getValue().location);
					int n = 0;
					long max = 0;
					for (FileStatus file : files) {
						if (!file.getPath().getName().startsWith("_")) {
							FSDataInputStream in = fs.open(file.getPath());
							BufferedReader reader = new BufferedReader(
									new InputStreamReader(in));
							String line;
							while ((line = reader.readLine()) != null) {
								long value = Long.valueOf(line);
								n++;
								count += value;
								if (value > max) {
									max = value;
								}
							}
							reader.close();
						}
					}

					// Skewness index is the difference between the max and the
					// average
					if (n > 0) {
						entry.getValue().skewness = max - (count / n);
					}

					entry.getValue().size = count;
				}

				if (!entry.getKey().getClass().equals(QueryRoot.class)) {
					cache.putInCache(entry.getKey(), entry.getValue());
				}
			}

			stats.processStatistics(jobs, toExecuteExpressions,
					System.currentTimeMillis() - startTime, startTime);
		}
		return output;
	}

	private void printScripts(List<String> scripts) {
		for (String line : scripts) {
			logger.info(line);
		}
	}

	PigServer pigServer;

	private List<ExecJob> executePigScripts(List<String> queries)
			throws ExecException, IOException {
		String pigMode = Configuration.getInstance().getProperty(
				Configuration.PIG_MODE);

		pigServer = new PigServer(
				pigMode.equalsIgnoreCase("local") ? ExecType.LOCAL
						: ExecType.MAPREDUCE);

		if (!Configuration.getInstance().getPropertyBoolean(
				Configuration.USE_LZO)) {
			pigServer.getPigContext().getProperties()
					.setProperty("pig.tmpfilecompression", "false");
		} else {
			pigServer.getPigContext().getProperties()
					.setProperty("pig.tmpfilecompression", "true");
			pigServer.getPigContext().getProperties()
					.setProperty("pig.tmpfilecompression.codec", "lzo");
		}
		pigServer.setBatchOn();
		pigServer.registerJar("RDFPig.jar");

		// Eliminate duplicates, pig does not like them.
		LinkedHashSet<String> queriesNoDuplicates = new LinkedHashSet<String>(
				queries);

		for (String line : queriesNoDuplicates) {
			pigServer.registerQuery(line);
		}
		return pigServer.executeBatch();
	}

	/** Method to test the bindings manager **/
	private void test(String[] args) throws Exception {
		FileReader fr = new FileReader(args[0]);
		BufferedReader br = new BufferedReader(fr);
		String line;
		String query = "";
		while ((line = br.readLine()) != null)
			query += line + "\n";
		br.close();
		fr.close();
		SPARQLParser parser = new SPARQLParser();
		ParsedQuery q = parser.parseQuery(query, "http://www.vu.nl");
		TupleExpr tupleExpr = q.getTupleExpr();
		QueryRoot root = new QueryRoot();
		root.setArg(tupleExpr);
		logger.info(tupleExpr.toString());
		List<TupleExpr> set = new ArrayList<TupleExpr>();
		set.add(root);
		execute(set, 1.0);
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Args: [query file] [input dir] [output dir]");
		}

		new Executor(args[1], args[2]).test(args);
	}
}