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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.tools.pigstats.InputStats;
import org.openrdf.query.algebra.TupleExpr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.larkc.RDFPig.Configuration;

/**
 * Writes out statistics spreadsheet-like
 * @author spyros
 *
 */
public class StatisticsCollector {
	protected static Logger logger = LoggerFactory.getLogger(StatisticsCollector.class);
	protected OutputStreamWriter writer;
	protected String outS;
	
	public StatisticsCollector() {
		outS=Configuration.getInstance().getProperty(Configuration.STATISTICS_STREAM)+"-"+System.currentTimeMillis();
	}
	
	public void processStatistics(List<ExecJob> execJobs, Map<TupleExpr, TupleSetMetadata> tuples, long wallTime, long wallStartTime) throws IOException{
		// Open and close file each time to make sure we do not lose anything to buffers
		
		
		OutputStream out;
		if (outS.equalsIgnoreCase("System.out")) {
			out=System.out;
		}
		else {
			out=new FileOutputStream(outS,true);
		}
		writer=new OutputStreamWriter(out);
		
		for (Map.Entry<TupleExpr, TupleSetMetadata> t : tuples.entrySet()) {
			for (ExecJob j: execJobs) {
				if (j.getAlias().equals(t.getValue().name)) {

					long bytesIn=0;
					long recordsIn=0;
					for (InputStats s:j.getStatistics().getInputStats()) {
						bytesIn+=s.getBytes();
						recordsIn+=s.getNumberRecords();
					}
					long bytesOut=j.getStatistics().getBytesWritten();
					long tuplesOut=j.getStatistics().getRecordWritten();
					int noJobs=j.getStatistics().getNumberJobs();
					long duration=j.getStatistics().getDuration();
					long spillTuples=j.getStatistics().getProactiveSpillCountRecords();
					double samplingRate=t.getValue().sampleRate;

					this.processStatsTuple(j.getAlias(), t.getKey().toString(), noJobs, bytesIn, bytesOut, spillTuples, recordsIn, tuplesOut, duration, wallTime, wallStartTime, samplingRate);
					break;
				}
			}
		}
		writer.close();
		out.close();
	}
	
	protected void processStatsTuple(String name, String tupleExpr, int noJobs, long bytesIn, long bytesOut, long spillTuples, long recordsIn, long recordsOut, long duration, long wallTime, long wallStartTime, double samplingRate) throws IOException {
		String outString = String.format("%s, %s, %d, %d, %d, %d, %d, %d, %d, %d, %d, %s\n", name, tupleExpr, noJobs, bytesIn, bytesOut, spillTuples, recordsIn, recordsOut, duration, wallTime, wallStartTime, samplingRate);
		writer.write(outString);
		}
	
}
