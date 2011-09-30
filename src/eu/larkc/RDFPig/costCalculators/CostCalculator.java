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
package eu.larkc.RDFPig.costCalculators;

import java.io.IOException;
import java.util.List;

import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;

public interface CostCalculator {

	public abstract void setInputSize(long inputSize);

	public abstract void prepareEstimation(List<? extends StatementPattern> stPatterns, long sizeInput) throws IOException;

	public abstract double getCost(TupleExpr sp);

}