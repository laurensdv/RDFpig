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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import eu.larkc.RDFPig.optimizers.VariableCollector;

public class Util {
	
	public static Set<Var> getExpressionVars(TupleExpr tupleExpr) {
		return VariableCollector.process(tupleExpr);
	}

	public static Set<String> getUnboundExpressionVars(TupleExpr tupleExpr) {
		Set<String> ret = new HashSet<String>(10);
		Set<Var> allVars = getExpressionVars(tupleExpr);
		for (Var v : allVars) {
			if (v.getValue() == null)
				ret.add(v.getName());
		}
		return ret;
	}

	public static String clearTrailingCommas(String s) {
		while (s.endsWith(",") || s.endsWith(", ")) {
			s = s.substring(0, s.length() - 1);
		}
		return s;
	}

	public static String asCommaSeparated(Collection<?> c) {
		StringBuffer b = new StringBuffer();
		for (Object o : c) {
			b.append(o.toString());
			b.append(",");
		}
		b.setLength(b.length() - 1);
		return b.toString();
	}

	public static List<StatementPattern> extractStatementPatterns(
			TupleExpr tupleExpr) {
		final Set<StatementPattern> patterns = new HashSet<StatementPattern>();

		QueryModelVisitor<RuntimeException> visitor = new QueryModelVisitorBase<RuntimeException>() {
			@Override
			public void meet(StatementPattern node) throws RuntimeException {
				patterns.add(node);
				super.meet(node);
			}
		};

		tupleExpr.visit(visitor);

		return new ArrayList<StatementPattern>(patterns);
	}

	public static String serializeIntArray(int[] a) {
		if (a.length == 0)
			return "";
		StringBuffer buf = new StringBuffer(a.length * 13);
		for (int aa : a)
			buf.append(aa + ":");
		return buf.substring(0, buf.length() - 1);
	}

	public static int[] deserializeIntArray(String s) {
		if (s.isEmpty())
			return new int[0];
		String[] f = s.split(":");
		int[] r = new int[f.length];
		int i = 0;
		for (String ff : f) {
			r[i++] = Integer.parseInt(ff);
		}
		return r;
	}

	public static String makeCommaSeparated(Collection<?> c) {
		StringBuffer ret = new StringBuffer(1000);
		for (Object o : c) {
			ret.append(o.toString());
			ret.append(',');
		}
		ret.setLength(ret.length() - 1);
		return ret.toString();
	}

	/**
	 * Constructs a String signature from the statement pattern
	 * 
	 * @param sp
	 * @return
	 */
	public static String getSignature(TupleExpr expr) {

		if (expr instanceof StatementPattern) {
			StatementPattern sp = (StatementPattern) expr;
			String ret = "";
			if (!sp.getSubjectVar().hasValue())
				ret += "?,";
			else
				ret += sp.getSubjectVar().getValue();
			if (!sp.getPredicateVar().hasValue())
				ret += "?,";
			else
				ret += sp.getPredicateVar().getValue();
			if (!sp.getObjectVar().hasValue())
				ret += "?";
			else
				ret += sp.getObjectVar().getValue();
			return ret;
		}

		// For now we return the serialization of the expression
		return expr.toString();
	}

	public static String formatValue(Value v) {
		if (v instanceof Literal)
			return "\"" + v.stringValue() + "\"";
		else if (v instanceof Resource)
			return "<" + v.stringValue() + ">";
		else
			throw new IllegalArgumentException("Unknown Value type");
	}

	public static String readFile(String filename)
			throws FileNotFoundException, IOException {
		FileReader fr = new FileReader(filename);
		BufferedReader br = new BufferedReader(fr);
		String line;
		String query = "";
		while ((line = br.readLine()) != null)
			query += line + "\n";
		br.close();
		fr.close();
		return query;
	}
	
	public static String ensurePigFriendlyName(String s) {
		return s.replace("-", ""); //TODO replace this with something that can not appear in SPARQL
	}
}
