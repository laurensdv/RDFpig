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

import java.util.Arrays;
import java.util.HashSet;

public class RDFConstants {
	/***** Standard URIs *****/
	public static final String S_RDF_NIL = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>";
	public static final String S_RDF_LIST = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#List>";
	public static final String S_RDF_FIRST = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>";
	public static final String S_RDF_REST = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>";
	public static final String S_RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
	public static final String S_RDF_PROPERTY = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>";
	public static final String S_RDFS_RANGE = "<http://www.w3.org/2000/01/rdf-schema#range>";
	public static final String S_RDFS_DOMAIN = "<http://www.w3.org/2000/01/rdf-schema#domain>";
	public static final String S_RDFS_SUBPROPERTY = "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>";
	public static final String S_RDFS_SUBCLASS = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>";
	public static final String S_RDFS_MEMBER = "<http://www.w3.org/2000/01/rdf-schema#member>";
	public static final String S_RDFS_LITERAL = "<http://www.w3.org/2000/01/rdf-schema#Literal>";
	public static final String S_RDFS_CONTAINER_MEMBERSHIP_PROPERTY = "<http://www.w3.org/2000/01/rdf-schema#ContainerMembershipProperty>";
	public static final String S_RDFS_DATATYPE = "<http://www.w3.org/2000/01/rdf-schema#Datatype>";
	public static final String S_RDFS_CLASS = "<http://www.w3.org/2000/01/rdf-schema#Class>";
	public static final String S_RDFS_RESOURCE = "<http://www.w3.org/2000/01/rdf-schema#Resource>";
	public static final String S_OWL_CLASS = "<http://www.w3.org/2002/07/owl#Class>";
	public static final String S_OWL_FUNCTIONAL_PROPERTY = "<http://www.w3.org/2002/07/owl#FunctionalProperty>";
	public static final String S_OWL_INVERSE_FUNCTIONAL_PROPERTY = "<http://www.w3.org/2002/07/owl#InverseFunctionalProperty>";
	public static final String S_OWL_SYMMETRIC_PROPERTY = "<http://www.w3.org/2002/07/owl#SymmetricProperty>";
	public static final String S_OWL_TRANSITIVE_PROPERTY = "<http://www.w3.org/2002/07/owl#TransitiveProperty>";
	public static final String S_OWL_SAME_AS = "<http://www.w3.org/2002/07/owl#sameAs>";
	public static final String S_OWL_INVERSE_OF = "<http://www.w3.org/2002/07/owl#inverseOf>";
	public static final String S_OWL_EQUIVALENT_CLASS = "<http://www.w3.org/2002/07/owl#equivalentClass>";
	public static final String S_OWL_EQUIVALENT_PROPERTY = "<http://www.w3.org/2002/07/owl#equivalentProperty>";
	public static final String S_OWL_HAS_VALUE = "<http://www.w3.org/2002/07/owl#hasValue>";
	public static final String S_OWL_ON_PROPERTY = "<http://www.w3.org/2002/07/owl#onProperty>";
	public static final String S_OWL_SOME_VALUES_FROM = "<http://www.w3.org/2002/07/owl#someValuesFrom>";
	public static final String S_OWL_ALL_VALUES_FROM = "<http://www.w3.org/2002/07/owl#allValuesFrom>";
	public static final String S_OWL2_PROPERTY_CHAIN_AXIOM = "<http://www.w3.org/2002/07/owl#propertyChainAxiom>";
	public static final String S_OWL2_HAS_KEY = "<http://www.w3.org/2002/07/owl#hasKey>";
	
	public HashSet<String> schemaTerms=new HashSet<String>(Arrays.asList(S_RDF_FIRST, S_RDF_LIST, S_RDF_NIL, S_RDF_PROPERTY, S_RDF_REST, S_RDF_TYPE, S_RDFS_CLASS, S_RDFS_CONTAINER_MEMBERSHIP_PROPERTY, S_RDFS_DATATYPE, S_RDFS_DOMAIN, S_RDFS_LITERAL, S_RDFS_MEMBER, S_RDFS_RANGE, S_RDFS_RESOURCE, S_RDFS_SUBCLASS, S_RDFS_SUBPROPERTY, S_OWL2_HAS_KEY, S_OWL2_PROPERTY_CHAIN_AXIOM, S_OWL_ALL_VALUES_FROM, S_OWL_CLASS, S_OWL_EQUIVALENT_CLASS, S_OWL_EQUIVALENT_PROPERTY, S_OWL_FUNCTIONAL_PROPERTY, S_OWL_HAS_VALUE, S_OWL_INVERSE_FUNCTIONAL_PROPERTY, S_OWL_INVERSE_OF, S_OWL_ON_PROPERTY, S_OWL_SAME_AS, S_OWL_SOME_VALUES_FROM, S_OWL_SYMMETRIC_PROPERTY, S_OWL_TRANSITIVE_PROPERTY));
	public HashSet<String> highCardinalityPredicates=new HashSet<String>(Arrays.asList(S_RDF_TYPE,S_OWL_SAME_AS));
}
