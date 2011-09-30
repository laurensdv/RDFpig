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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;

import eu.larkc.RDFPig.Util;

public class TupleSetMetadata{

	public Path location; //If the expression is calculated
	protected long size = -1; //Number elements
	public String name;
	public double sampleRate = 1.0;
	Map<String, String> qualifiedVariableNames = new HashMap<String, String>(); //Maps variable names to pig-qualified variable names, qualified variable names look like Join1::Join4:X
	public long skewness = 0;

	static int counter = 0; //To assign unique names


	public TupleSetMetadata(String prefix) {
		this.name= prefix + Integer.toString(counter++);
		//this.size=Long.MAX_VALUE;
	}

	public TupleSetMetadata() {
		this("VAR");
	}

	/**
	 * Get the size of this set, corrected for sampling
	 * @return the size
	 */
	public long getSize() {
		return (long) (size/sampleRate);
	}

	/**
	 * Set the size of this set, this is *not* corrected for sampling, i.e. it is the actual number of tuple written
	 * @param size the size to set
	 */
	public void setSize(long size) {
		this.size = size;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return name + " size=" + this.getSize() + " sampling=" + this.sampleRate + " location=" + location;
	}

	/**
	 * Adds an unqualified name
	 * @param v
	 */
	public void addUnqualifiedName(String var) {
		qualifiedVariableNames.put(var, var);

	}

	public TupleSetMetadata(TupleExpr expr) {
		this(expr.getClass().getSimpleName());
	}

	public TupleSetMetadata(TupleExpr expr, TupleSetMetadata copyFrom) {
		this(expr);
		this.qualifiedVariableNames.putAll(copyFrom.qualifiedVariableNames);
	}

	public String getQualifiedVariable(String var) {
		return qualifiedVariableNames.get(var);
	}

	/**
	 * @return the qualifiedVariableNames
	 */
	public Map<String, String> getQualifiedVariableNames() {
		return qualifiedVariableNames;
	}

	/**
	 * @param qualifiedVariableNames the qualifiedVariableNames to set
	 */
	public void setQualifiedVariableNames(Map<String, String> qualifiedVariableNames) {
		this.qualifiedVariableNames.clear();
		this.qualifiedVariableNames.putAll(qualifiedVariableNames);
	}


	public void qualifyName(String var, String name) {
		String oldName=qualifiedVariableNames.get(var);
		if (oldName==null)
			oldName=var;
		String newName=name+"::"+oldName;
		qualifiedVariableNames.put(var, newName);
	}

	public void resetQualifications() {
		for (String s:new HashSet<String>(qualifiedVariableNames.keySet()))
				addUnqualifiedName(s);
	}

	public static String constructQualifiedColumnString(Set<String> vars,
			TupleSetMetadata meta, String extraQualifier) {
		String ret = "";
		int count = 0;
		for (String s : vars) {
			++ count;
			ret += (extraQualifier.isEmpty() ? "" : (extraQualifier + "::"))
					+ meta.getQualifiedVariable(s) + ",";
		}
		ret = Util.clearTrailingCommas(ret) + "";
		if (count > 1) {
			ret = "(" + ret + ")";
		}

		return ret;
	}

	public static String makeQualifiedVariableString(Set<String> vars,
			Collection<TupleSetMetadata> meta, boolean increaseQualification)
			throws QueryEvaluationException {
		String ret = "";

		for (String v : vars) {
			boolean foundone = false;
			for (TupleSetMetadata m : meta) {
				String cc = m.getQualifiedVariable(v);
				if (cc != null) {
					ret += (increaseQualification ? (m.name + "::") : "") + cc
							+ " AS " + v + ", ";
					foundone = true;
					break;
				}
			}
			if (!foundone)
				throw new QueryEvaluationException("No qualifier for variable "
						+ v);
		}
		return Util.clearTrailingCommas(ret);
	}

	public static String cleanVariableName(String varName) {
		return varName.replace("-", "");
	}
}