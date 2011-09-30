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
package eu.larkc.RDFPig.optimizers;

import org.openrdf.query.algebra.TupleExpr;

public class TupleCostPair {
	public TupleCostPair(TupleExpr tuple, double cost) {
		this.tuple=tuple;
		this.cost=cost;
	}
	public TupleExpr tuple;
	public double cost;
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TupleCostPair)
			return ((TupleCostPair)obj).tuple.equals(this.tuple);
		else
			return false;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return this.tuple.hashCode();
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return tuple.toString() + " \nCost=" + cost;
	}
}
