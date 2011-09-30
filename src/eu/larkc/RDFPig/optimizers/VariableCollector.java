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

import java.util.HashSet;
import java.util.Set;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;

import eu.larkc.RDFPig.joinOperators.ExtendedQueryModelVisitor;
import eu.larkc.RDFPig.joinOperators.NWayJoin;

public class VariableCollector extends ExtendedQueryModelVisitor<RuntimeException> {

	public static Set<Var> process(QueryModelNode node) {
		VariableCollector collector = new VariableCollector();
		node.visit(collector);
		return collector.getVariables();
	}
	
	private Set<Var> vars = new HashSet<Var>();

	public Set<Var> getVariables() {
		return vars;
	}

	@Override
	public void meet(Filter node)
	{
		// Skip boolean constraints
		node.getArg().visit(this);
	}

	@Override
	public void meet(StatementPattern node)
	{
		node.getVars(vars);
	}
	
	@Override
	public void meet(Projection projection) {
		// Do not visit children for projections
		for (String v: projection.getProjectionElemList().getTargetNames())
			vars.add(new Var(v));
	}
	
	@Override
	public void meet(NWayJoin nWayJoin) {
		for (TupleExpr x: nWayJoin.getArgs())
			x.visit(this);
	}
}
