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
package eu.larkc.RDFPig.joinOperators;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.openrdf.query.algebra.EmptySet;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.TupleExpr;

public class NWayJoin extends Join implements TupleExpr  {
	protected List<TupleExpr> args;
	
	private static TupleExpr FAKE=new EmptySet(); // This is ugly
	
	public NWayJoin() {
		super(FAKE, FAKE);
		args=new ArrayList<TupleExpr>(5);
	}
	
	public void addArg(TupleExpr arg) {
		arg.setParentNode(this);
		args.add(arg);
	}
	
	public List<TupleExpr> getArgs() {
		return args;
	}
	
	public <X extends Exception> void visit(ExtendedQueryModelVisitor<X> visitor) throws X {
		visitor.meet(this);
	}

	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor) throws X {
		visitor.meet(this);		
	}

	@Override
	public Set<String> getAssuredBindingNames() {
		Set<String> bindingNames = new LinkedHashSet<String>(16);
		for (TupleExpr n:args)
			bindingNames.addAll(n.getAssuredBindingNames());
		return bindingNames;
	}

	@Override
	public Set<String> getBindingNames() {
		Set<String> bindingNames = new LinkedHashSet<String>(16);
		for (TupleExpr n:args)
			bindingNames.addAll(n.getBindingNames());
		return bindingNames;
	}

	@Override
	public void replaceChildNode(QueryModelNode arg0, QueryModelNode arg1) {
		args.remove((TupleExpr)arg0);
		args.add((TupleExpr)arg1);
	}

	@Override
	public <X extends Exception> void visitChildren(QueryModelVisitor<X> arg0)
			throws X {
		for (TupleExpr n:args)
			n.visit(arg0);
	}

	@Override
	public NWayJoin clone() {
		NWayJoin clone = new NWayJoin();
		clone.setParentNode(getParentNode());
		for (TupleExpr t:args) {
			if (t==this)
				throw new IllegalStateException("Detected cycle");
			clone.addArg(t.clone());
		}
		return clone;
	}

	/* (non-Javadoc)
	 * @see org.openrdf.query.algebra.Join#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object other) {
		if (other instanceof NWayJoin) {
			NWayJoin o = (NWayJoin) other;
			if (this.getArgs().size()==o.getArgs().size() && this.hashCode()==o.hashCode()) {
				for (TupleExpr expr1: this.getArgs()) {
					boolean found=false;
					for (TupleExpr expr2: o.getArgs()) 
						if (expr1.equals(expr2)) {
							found=true;
							break;
						}
					if (!found)
						return false;
				}
				return true;
			}
		}
		return false;
	}

	/* (non-Javadoc)
	 * @see org.openrdf.query.algebra.Join#hashCode()
	 */
	@Override
	public int hashCode() {
		long h=1;
		for (TupleExpr e: this.getArgs())
			h+=e.hashCode();
		return (int)h;
	}

	/* (non-Javadoc)
	 * @see org.openrdf.query.algebra.QueryModelNodeBase#toString()
	 */
	@Override
	public String toString() {
		return super.toString();
	}
}
