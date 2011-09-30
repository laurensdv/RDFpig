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

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @author spyros
 *

 */
public class Main {

	protected static Logger logger = LoggerFactory.getLogger(Main.class);

	private static final String NSBASE = "http://blah";

	/**
	 * @param args
	 * @throws MalformedQueryException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws QueryEvaluationException 
	 */
	public static void main(String[] args) throws MalformedQueryException, IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException, QueryEvaluationException {

		if (args.length!=3) {
			System.err.println("Usage: Main [query file] [input path] [output path]");
			System.exit(2);
		}
		
		QueryProcessor v=new QueryProcessor(args[1],args[2]);
		String allQueries = Util.readFile(args[0]);
		int i=0;
		for (String q: allQueries.split("#Q")) {
			q=q.trim();
			if (q.isEmpty() || q.startsWith("#") || q.startsWith("//")) //Skip empty lines and comments
				continue;
			
			if (Configuration.getInstance().getPropertyBoolean(Configuration.RESETEXECUTORFOREACHQUERY))
				v=new QueryProcessor(args[1],args[2]+i++);
			v.processQuery(q, NSBASE);
		}	
	}
}
