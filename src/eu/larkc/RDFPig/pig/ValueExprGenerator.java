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

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.AggregateOperator;
import org.openrdf.query.algebra.And;
import org.openrdf.query.algebra.Avg;
import org.openrdf.query.algebra.BNodeGenerator;
import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.Count;
import org.openrdf.query.algebra.FunctionCall;
import org.openrdf.query.algebra.MathExpr;
import org.openrdf.query.algebra.Max;
import org.openrdf.query.algebra.Min;
import org.openrdf.query.algebra.Or;
import org.openrdf.query.algebra.Regex;
import org.openrdf.query.algebra.Sum;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.larkc.RDFPig.Util;

public class ValueExprGenerator {
	protected static Logger logger = LoggerFactory.getLogger(ValueExprGenerator.class);	
	
	/**
	 * 
	 * Checks whether an expression is simple (i.e. it can be embedded in an existing pig statement. E.g. casts can not be embedded in bag operations
	 * @param expr
	 * @return true is the expression is simple
	 */
	public boolean isSimpleExpression(ValueExpr expr) {
		if (expr instanceof Var || expr instanceof Count || expr instanceof ValueConstant)	// TODO this is pessimistic, we can include more expressions in existing statements
			return true;
		else
			return false;
	}
	
	public String getValue(ValueExpr expr, TupleSetMetadata meta) throws QueryEvaluationException {
		return getValue(expr, meta, false);
	}
	
	public String getValue(ValueExpr expr, TupleSetMetadata meta, boolean bagValues)
			throws QueryEvaluationException {
		if (expr instanceof Var) {
			return getValue((Var) expr, meta);
		} else if (expr instanceof ValueConstant) {
			return getValue((ValueConstant) expr);
		} else if (expr instanceof BNodeGenerator) {
			return getValue((BNodeGenerator) expr, meta);
		} else if (expr instanceof Regex) {
			return getValue((Regex) expr, meta);
		} else if (expr instanceof MathExpr) {
			return getValue((MathExpr) expr, meta);
		} else if (expr instanceof Count) {
			return getValue((Count) expr, meta);
		} else if (expr instanceof Avg) {
			return getValue((Avg) expr, meta, bagValues);
		} else if (expr instanceof Max) {
			return getValue((Max) expr, meta, bagValues);
		} else if (expr instanceof Min) {
			return getValue((Min) expr, meta, bagValues);
		} else if (expr instanceof Sum) {
			return getValue((Sum) expr, meta, bagValues);
		} else if (expr instanceof Compare) {
			return getValue((Compare) expr, meta);
		} else if (expr instanceof FunctionCall) {
			return getValue((FunctionCall) expr, meta, bagValues);
		} else if (expr instanceof And) {
			return getValue((And) expr, meta);
		} else if (expr instanceof Or) {
			return getValue((Or) expr, meta);
		} else
			throw new UnsupportedOperationException(
					"Unsupported value expression: " + expr);

		/*
		 * else if (expr instanceof Bound) { return getValue((Bound) expr,
		 * bindings); } else if (expr instanceof Str) { return getValue((Str)
		 * expr, bindings); } else if (expr instanceof Label) { return
		 * getValue((Label) expr, bindings); } else if (expr instanceof Lang) {
		 * return getValue((Lang) expr, bindings); } else if (expr instanceof
		 * LangMatches) { return getValue((LangMatches) expr, bindings); } else
		 * if (expr instanceof Datatype) { return getValue((Datatype) expr,
		 * bindings); } else if (expr instanceof Namespace) { return
		 * getValue((Namespace) expr, bindings); } else if (expr instanceof
		 * LocalName) { return getValue((LocalName) expr, bindings); } else if
		 * (expr instanceof IsResource) { return getValue((IsResource) expr,
		 * bindings); } else if (expr instanceof IsURI) { return
		 * getValue((IsURI) expr, bindings); } else if (expr instanceof IsBNode)
		 * { return getValue((IsBNode) expr, bindings); } else if (expr
		 * instanceof IsLiteral) { return getValue((IsLiteral) expr, bindings);
		 * } } else if (expr instanceof Like) { return getValue((Like) expr,
		 * bindings);
		 */
		/*
		 * } else if (expr instanceof FunctionCall) { return
		 * evaluate((FunctionCall) expr, bindings); } else if (expr instanceof
		 * And) { return evaluate((And) expr, bindings); } else if (expr
		 * instanceof Or) { return evaluate((Or) expr, bindings); } else if
		 * (expr instanceof Not) { return evaluate((Not) expr, bindings); } else
		 * if (expr instanceof SameTerm) { return evaluate((SameTerm) expr,
		 * bindings); } else if (expr instanceof Compare) { return
		 * evaluate((Compare) expr, bindings); } else if (expr instanceof In) {
		 * return evaluate((In) expr, bindings); } else if (expr instanceof
		 * CompareAny) { return evaluate((CompareAny) expr, bindings); } else if
		 * (expr instanceof CompareAll) { return evaluate((CompareAll) expr,
		 * bindings); } else if (expr instanceof Exists) { return
		 * evaluate((Exists) expr, bindings); } else if (expr == null) { throw
		 * new IllegalArgumentException("expr must not be null");
		 */
	}
	
	public ValueExpr getAggregateOperatorArgument(AggregateOperator expr) throws QueryEvaluationException {
		if (expr instanceof Count) {
			return ((Count) expr).getArg();
		} else if (expr instanceof Avg) {
			return ((Avg) expr).getArg();
		} else if (expr instanceof Max) {
			return ((Max) expr).getArg();
		} else if (expr instanceof Min) {
			return ((Min) expr).getArg();
		} else if (expr instanceof Sum) {
			return ((Sum) expr).getArg();
		}
		else
			throw new QueryEvaluationException("Unknown aggregate: "+ expr);
	}
	
	public String getAggregateOperatorAsString(AggregateOperator expr) throws QueryEvaluationException {
		if (expr instanceof Count) {
			return "COUNT";
		} else if (expr instanceof Avg) {
			return "AVG";
		} else if (expr instanceof Max) {
			return "MIN";
		} else if (expr instanceof Min) {
			return "MAX";
		} else if (expr instanceof Sum) {
			return "SUM";
		}
		else
			throw new QueryEvaluationException("Unknown aggregate: "+ expr);
	}

	private String getValue(MathExpr expr, TupleSetMetadata meta) throws QueryEvaluationException {
		String mathString;
		switch (expr.getOperator()) {
		case PLUS:
			mathString="+";
			break;
		case MULTIPLY:
			mathString="*";
			break;
		case DIVIDE:
			mathString="/";
			break;
		case MINUS:
			mathString="-";
			break;
		default:
			throw new QueryEvaluationException("Unknown comparison operator: " + expr.getOperator());
		}
		
		return "("+getValue(expr.getLeftArg(), meta) + " " + mathString + " " + getValue(expr.getRightArg(), meta)+")";
	}
	
	private String getValue(Compare expr, TupleSetMetadata meta) throws QueryEvaluationException {
		String compareOpString;
		switch (expr.getOperator()) {
		case EQ:
			compareOpString="==";
			break;
		case NE:
			compareOpString="!=";
			break;
		case GE:
			compareOpString=">=";
			break;
		case LE:
			compareOpString="<=";
			break;
		case GT:
			compareOpString=">";
			break;
		case LT:
			compareOpString="<";
			break;
		default:
			throw new QueryEvaluationException("Unknown comparison operator: " + expr.getOperator());
		}
		return "("+getValue(expr.getLeftArg(), meta) + " " +  compareOpString + " " + getValue(expr.getRightArg(), meta)+")";
	}

	private String getValue(Var var, TupleSetMetadata meta) {
		return meta.getQualifiedVariable(Util.ensurePigFriendlyName(var.getName()));
	}
	private String getValue(Avg var, TupleSetMetadata meta, boolean bag) throws QueryEvaluationException {
		return "AVG(" +getValue(var.getArg(), meta, bag) + ")";
	}
	private String getValue(Max var, TupleSetMetadata meta, boolean bag) throws QueryEvaluationException {
		return "MAX(" +getValue(var.getArg(), meta, bag) + ")";
	}
	private String getValue(Min var, TupleSetMetadata meta, boolean bag) throws QueryEvaluationException {
		return "MIN(" +getValue(var.getArg(), meta,bag) + ")";
	}
	private String getValue(Count var, TupleSetMetadata meta) throws QueryEvaluationException {
		return "COUNT(" +getValue(var.getArg(), meta) + ")"; 
	}
	private String getValue(Sum var, TupleSetMetadata meta, boolean bag) throws QueryEvaluationException {
		return "SUM(" +getValue(var.getArg(), meta, bag) + ")";
	}
	
	/**
	 * Function calls are ignored
	 * @param func
	 * @return
	 * @throws QueryEvaluationException
	 */
	private String getValue(FunctionCall func, TupleSetMetadata meta, boolean bag) throws QueryEvaluationException {		
		if (func.getArgs().isEmpty()) {
			logger.warn("Function:" + func.getURI() + " ignored");
			return "";
		}
		else if (func.getArgs().size()==1) {
			String cast="";
			if (func.getURI().equals("http://www.w3.org/2001/XMLSchema#float")) {
				if (bag) {
					cast="(bag{tuple(float)})";
				} else {
					cast="(float)";
				}
			} else if (func.getURI().equals("http://www.w3.org/2001/XMLSchema#double")) { 
				if (bag) {
					cast="(bag{tuple(double)})";
				} else {
					cast="(double)";
				}				
			} else if (func.getURI().equals("http://www.w3.org/2001/XMLSchema#string")) { 
				return "REPLACE(" + getValue(func.getArgs().get(0),meta) + ",'\"','')";				
			}
			else {
				logger.warn("Function:" + func.getURI() + " ignored");
				return getValue(func.getArgs().get(0),meta);
			}
			return cast+getValue(func.getArgs().get(0),meta);
		}
		else
			throw new QueryEvaluationException("Do not know how to evaluate function " + func);
	}
	
	private String getValue(ValueConstant valueConstant)
			throws ValueExprEvaluationException, QueryEvaluationException {
		if (valueConstant.getValue() instanceof URI)
			return "'<"+valueConstant.getValue().stringValue() +">'";
		else if (valueConstant.getValue() instanceof Literal) {
			Literal lValue=(Literal)valueConstant.getValue();
			if (lValue.getDatatype()!=null && 
					(lValue.getDatatype().equals(XMLSchema.INT) || 
							lValue.getDatatype().equals(XMLSchema.INTEGER) || 
									lValue.getDatatype().equals(XMLSchema.FLOAT) || 
									lValue.getDatatype().equals(XMLSchema.DECIMAL) || 
									lValue.getDatatype().equals(XMLSchema.DOUBLE))  )
				return valueConstant.getValue().stringValue();
			else
				return "'\""+valueConstant.getValue().stringValue() +"\"'";
		}
		else 
			throw new QueryEvaluationException("Do not know how to handle as ValueConstant:" + valueConstant);
	}

	public String getValue(Regex node, TupleSetMetadata meta)
		throws ValueExprEvaluationException, QueryEvaluationException {
	String arg = getValue(node.getArg(), meta);
	String parg = getValue(node.getPatternArg(), meta);
	
	return arg + "' matches '" + parg;
	}
	
	public ValueExpr getArg(ValueExpr expr) {
		if (expr instanceof Var) {
			return null;
		} else if (expr instanceof ValueConstant) {
			return null;
		} else if (expr instanceof BNodeGenerator) {
			return null;
		} else if (expr instanceof Regex) {
			return ((Regex) expr).getArg();
		} else if (expr instanceof MathExpr) {
			return null;
		} else if (expr instanceof Count) {
			return ((Count) expr).getArg();
		} else if (expr instanceof Avg) {
			return ((Avg) expr).getArg();
		} else if (expr instanceof Max) {
			return ((Max) expr).getArg();
		} else if (expr instanceof Min) {
			return ((Min) expr).getArg();
		} else if (expr instanceof Compare) {
			return null;
		} else if (expr instanceof FunctionCall) {
			return null;
		} else
			throw new UnsupportedOperationException(
					"Unsupported value expression: " + expr);
	}
	
	private String getValue(And node, TupleSetMetadata meta) throws QueryEvaluationException {
		return getValue(node.getLeftArg(),meta) + " AND " + getValue(node.getRightArg(),meta);
	}
	
	private String getValue(Or node, TupleSetMetadata meta) throws QueryEvaluationException {
		return getValue(node.getLeftArg(),meta) + " OR " + getValue(node.getRightArg(),meta);
	}
	
	
	/*
	 * private String getValue(IsResource node, BindingSet bindings) throws
	 * ValueExprEvaluationException, QueryEvaluationException { Value argValue =
	 * getValue(node.getArg(), bindings); return
	 * BooleanLiteralImpl.valueOf(argValue instanceof Resource); }
	 * 
	 * private String getValue(IsURI node, BindingSet bindings) throws
	 * ValueExprEvaluationException, QueryEvaluationException { Value argValue =
	 * getValue(node.getArg(), bindings); return
	 * BooleanLiteralImpl.valueOf(argValue instanceof URI); }
	 * 
	 * private String getValue(IsBNode node, BindingSet bindings) throws
	 * ValueExprEvaluationException, QueryEvaluationException { Value argValue =
	 * getValue(node.getArg(), bindings); return
	 * BooleanLiteralImpl.valueOf(argValue instanceof BNode); }
	 * 
	 * private String getValue(IsLiteral node, BindingSet bindings) throws
	 * ValueExprEvaluationException, QueryEvaluationException { Value argValue =
	 * getValue(node.getArg(), bindings); return
	 * BooleanLiteralImpl.valueOf(argValue instanceof Literal); }
	 */



	/*
	 * private String getValue(Like node, BindingSet bindings) throws
	 * ValueExprEvaluationException, QueryEvaluationException { Value val =
	 * getValue(node.getArg(), bindings); String strVal = null;
	 * 
	 * if (val instanceof URI) { strVal = ((URI) val).toString(); } else if (val
	 * instanceof Literal) { strVal = ((Literal) val).getLabel(); }
	 * 
	 * if (strVal == null) { throw new ValueExprEvaluationException(); }
	 * 
	 * if (!node.isCaseSensitive()) { // Convert strVal to lower case, just like
	 * the pattern has been done strVal = strVal.toLowerCase(); }
	 * 
	 * int valIndex = 0; int prevPatternIndex = -1; int patternIndex =
	 * node.getOpPattern().indexOf('*');
	 * 
	 * if (patternIndex == -1) { // No wildcards return
	 * BooleanLiteralImpl.valueOf(node.getOpPattern() .equals(strVal)); }
	 * 
	 * String snippet;
	 * 
	 * if (patternIndex > 0) { // Pattern does not start with a wildcard, first
	 * part must match snippet = node.getOpPattern().substring(0, patternIndex);
	 * if (!strVal.startsWith(snippet)) { return BooleanLiteralImpl.FALSE; }
	 * 
	 * valIndex += snippet.length(); prevPatternIndex = patternIndex;
	 * patternIndex = node.getOpPattern().indexOf('*', patternIndex + 1); }
	 * 
	 * while (patternIndex != -1) { // Get snippet between previous wildcard and
	 * this wildcard snippet = node.getOpPattern().substring(prevPatternIndex +
	 * 1, patternIndex);
	 * 
	 * // Search for the snippet in the value valIndex = strVal.indexOf(snippet,
	 * valIndex); if (valIndex == -1) { return BooleanLiteralImpl.FALSE; }
	 * 
	 * valIndex += snippet.length(); prevPatternIndex = patternIndex;
	 * patternIndex = node.getOpPattern().indexOf('*', patternIndex + 1); }
	 * 
	 * // Part after last wildcard snippet =
	 * node.getOpPattern().substring(prevPatternIndex + 1);
	 * 
	 * if (snippet.length() > 0) { // Pattern does not end with a wildcard.
	 * 
	 * // Search last occurence of the snippet. valIndex =
	 * strVal.indexOf(snippet, valIndex); int i; while ((i =
	 * strVal.indexOf(snippet, valIndex + 1)) != -1) { // A later occurence was
	 * found. valIndex = i; }
	 * 
	 * if (valIndex == -1) { return BooleanLiteralImpl.FALSE; }
	 * 
	 * valIndex += snippet.length();
	 * 
	 * if (valIndex < strVal.length()) { // Some characters were not matched
	 * return BooleanLiteralImpl.FALSE; } }
	 * 
	 * return BooleanLiteralImpl.TRUE; }
	 * 
	 * }
	 */
}