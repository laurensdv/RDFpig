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

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configuration extends Properties {

	private static Logger logger = LoggerFactory.getLogger(Configuration.class);

	public final static String PIG_MODE = "pig.mode";
	public final static String COST_CALCULATOR_CLASSES_WEIGHTS = "cost.calculator";
//	public final static String INPUT_LOCATION = "input.location";
//	public final static String OUTPUT_LOCATION = "output.location";
	public final static String INMEMORY_THRESHOLD ="in.memory.threshold";
	public final static String SAMPLE_RATE = "sample.rate";
	public final static String DISTINCT_PREPROCESSING="distinct.preprocessing";

	private static final long serialVersionUID = 1L;

	public static final String OPTIMIZATIONSTRATEGY = "optimization.strategy";
	public static final String DYNAMICPROGRAMMINGSTATICPRUNEBEFORE = "dynamic.programming.static.prune";

	public static final String STATISTICS_STREAM = "statistics.stream";

	public static final String RESETEXECUTORFOREACHQUERY = "reset.executor.for.each.query";

	public static final String MAXSAMPLINGCYCLES = "max.sampling.cycles";

	public static final String DENSE_SAMPLING_THRESHOLD = "sampling.threshold";

	public static final String USE_LZO = "compression.lzo";

	public static final String SKEWNESS_THRESHOLD = "join.skewness";

	private Configuration() throws FileNotFoundException, IOException {
		// Set default values for all parameters
		setProperty(DISTINCT_PREPROCESSING, "false");
		setProperty(INMEMORY_THRESHOLD, "100000");
		setProperty(SAMPLE_RATE, "0.05");
		setProperty(PIG_MODE, "local");
		setProperty(COST_CALCULATOR_CLASSES_WEIGHTS,
				"eu.larkc.RDFPig.costCalculators.SamplingCardinalityCalculator(100000000, 10000, 1, 3, 0.1, 1000000);1"); //semi-colon separated classnames/args, interleaved with the weights
		setProperty(DYNAMICPROGRAMMINGSTATICPRUNEBEFORE, "3.0");
		setProperty(OPTIMIZATIONSTRATEGY, "eu.larkc.RDFPig.optimizers.strategies.SamplePruneOptimizationStrategy");
		setProperty(STATISTICS_STREAM, "statistics.csv");
		setProperty(RESETEXECUTORFOREACHQUERY, "true");
		setProperty(MAXSAMPLINGCYCLES, "2");
		setProperty(USE_LZO, "false");
		setProperty(DENSE_SAMPLING_THRESHOLD, "1000");
		setProperty(SKEWNESS_THRESHOLD, "10000000");

		// Load the file configuration.ini file if in the classpath
		URL urlFile = Class.class.getResource("/configuration.ini");
		try {
			logger.info("Loading configuration from " + urlFile.getFile());
			this.load(new FileReader(urlFile.getFile()));
		}
		catch(Exception e) {
			logger.warn("Could not load config file. Using defaults.",e);
		}
		// Override all the properties with the ones passed on the command line
		for (Map.Entry<Object, Object> entry : System.getProperties()
				.entrySet()) {
			this.put(entry.getKey(), entry.getValue());
		}
	}

	private static Configuration instance = null;

	public boolean getPropertyBoolean(String property) {
		return super.getProperty(property)!=null && (super.getProperty(property).equalsIgnoreCase("true") || super.getProperty(property).equalsIgnoreCase("yes"));
	}

	public double getPropertyDouble(String property) {
		return Double.parseDouble(super.getProperty(property));
	}

	public int getPropertyInt(String property) {
		return Integer.parseInt(super.getProperty(property));
	}

	public long getPropertyLong(String property) {
		return Long.parseLong(super.getProperty(property));
	}

	public static Configuration getInstance() {
		if (instance == null) {
			try {
				instance = new Configuration();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return instance;
	}
}
