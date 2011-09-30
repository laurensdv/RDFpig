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

import java.util.List;

import eu.larkc.RDFPig.Configuration;
import eu.larkc.RDFPig.pig.TupleSetMetadata;

public class JoinTypeSelector {
	public enum JoinType {
		REPLICATED, LOADBALANCED, NORMAL
	};

	/**
	 * Select type of join.
	 *
	 * @pre join arguments are sorted according to size (asc), we have>1
	 *      joinArgs
	 * @param joinArgs
	 * @return
	 */
	public JoinType selectTypeOfJoin(TupleSetMetadata sampledJoin, List<TupleSetMetadata> joinArgs) {

		// Children meta are sorted from largest to shortest. If all but the
		// first are under the memory threshold then choose the replicated join
		// (best)
		boolean replicated = false;
		long memThreshold = Configuration.getInstance().getPropertyLong(
				Configuration.INMEMORY_THRESHOLD);

		for(int i = 1; i < joinArgs.size(); ++i) {
			if (joinArgs.get(i).getSize() > memThreshold || joinArgs.get(i).getSize() == -1) { //Second condition happens if I don't anything about it
				replicated = false;
			}
		}

		if (replicated)
			return JoinType.REPLICATED;

		//Now the choice is between the standard join or the skewed one.
		if (joinArgs.size() == 2 && sampledJoin != null) {
			//Check whether the skew of the data requires to apply the join
			double expectedDifference = sampledJoin.skewness / sampledJoin.sampleRate;
			long threshold = Configuration.getInstance().getPropertyLong(Configuration.SKEWNESS_THRESHOLD);
			if (expectedDifference > threshold)
				return JoinType.LOADBALANCED;
		}

		return JoinType.NORMAL;
	}
}
