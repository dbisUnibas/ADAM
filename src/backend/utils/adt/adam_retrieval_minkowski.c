/*
 * ADAM - Minkowski distance functions
 * name: adam_retrieval_minkowski
 * description: functions for calculating the Minkowski distances
 *
 * developed in the course of the MSc thesis at the University of Basel
 *
 * author: Ivan Giangreco
 * email: ivan.giangreco@unibas.ch
 *
 * src/backend/utils/adt/adam_retrieval_minkowski.c
 *
 *
 *
 *
 */
#include "postgres.h"

#include "utils/adam_retrieval_minkowski.h"
#include "utils/adam_data_feature.h"

#include "catalog/pg_type.h"
#include "parser/parse_node.h"
#include "utils/array.h"
#include "utils/builtins.h"

static Datum calculateMinkowskiL1(feature *f1, feature *f2);
static Datum calculateMinkowskiLn(feature *f1, feature *f2, Datum n);
static Datum calculateMinkowskiLmax(feature *f1, feature *f2);

static Datum calculateWeightedMinkowskiL1(feature *f1, feature *f2, ArrayType *weights);
static Datum calculateWeightedMinkowskiLn(feature *f1, feature *f2, ArrayType *weights, Datum n);
static Datum calculateWeightedMinkowskiLmax(feature *f1, feature *f2, ArrayType *weights);

#define EPSILON	0.001


/*
 * calculates the minkowski distance between two feature vectors
 * and the norm given
 */
Datum
calculateMinkowski(PG_FUNCTION_ARGS)
{
	feature *f1 = (feature *)  PG_GETARG_VARLENA_P(0);
	feature *f2 = (feature *)  PG_GETARG_VARLENA_P(1);
	float8 n = DatumGetFloat8(PG_GETARG_DATUM(2));
    
	Datum result;
    
	if(f1->typid != FLOAT8OID || f2->typid != FLOAT8OID){
		ereport(ERROR,(errmsg("the minkowski distance can only be used with numeric types")));
	}
    
	if(n - 1 < EPSILON && n > 0){
		result = calculateMinkowskiL1(f1, f2);
	} else if(n < EPSILON && n > 0){
		result = calculateMinkowskiLmax(f1, f2);
	} else {
		result = calculateMinkowskiLn(f1, f2, Float8GetDatum(n));
	}
    
	PG_RETURN_DATUM(result);
}

/*
 * calculates the L1 minkowksi distance (i.e. S| x - y |)
 */
static Datum
calculateMinkowskiL1(feature *f1, feature *f2)
{
	ArrayIterator f1_it;
	Datum		f1_val;
	bool		f1_isnull;
    
	ArrayIterator f2_it;
	Datum		f2_val;
	bool		f2_isnull;
    
	Datum		transResult;
	
	transResult = Float8GetDatum(0);
    
	f1_it = array_create_iterator(&f1->data, 0);
	f2_it = array_create_iterator(&f2->data, 0);
    
	while(array_iterate(f1_it, &f1_val, &f1_isnull) && array_iterate(f2_it, &f2_val, &f2_isnull)){
		if(!f1_isnull && !f2_isnull){
			transResult = DirectFunctionCall2(float8pl,
                                              transResult,
                                              DirectFunctionCall1(float8abs, DirectFunctionCall2(float8mi, f1_val, f2_val)));
		}
	}
    
	array_free_iterator(f1_it);
	array_free_iterator(f2_it);
    
	return transResult;
}

/*
 * calculates the Ln minkowksi distance (i.e. S(x - y)^n)
 */
static Datum
calculateMinkowskiLn(feature *f1, feature *f2, Datum n)
{
	ArrayIterator f1_it;
	Datum		f1_val;
	bool		f1_isnull;
    
	ArrayIterator f2_it;
	Datum		f2_val;
	bool		f2_isnull;
    
	Datum		transResult;
    
	transResult = Float8GetDatum(0);
    
	f1_it = array_create_iterator(&f1->data, 0);
	f2_it = array_create_iterator(&f2->data, 0);
    
	while(array_iterate(f1_it, &f1_val, &f1_isnull) && array_iterate(f2_it, &f2_val, &f2_isnull)){
		if(!f1_isnull && !f2_isnull){
			transResult = DirectFunctionCall2(float8pl,
                                              transResult,
                                              DirectFunctionCall2(dpow,
                                                                  DirectFunctionCall1(float8abs, DirectFunctionCall2(float8mi, f1_val, f2_val))
                                                                  , n));
		}
	}
    
	array_free_iterator(f1_it);
	array_free_iterator(f2_it);
    
	return transResult;
}

/*
 * calculates the Lmax minkowksi distance (i.e. max(x - y))
 */
static Datum
calculateMinkowskiLmax(feature *f1, feature *f2)
{
	ArrayIterator f1_it;
	Datum		f1_val;
	bool		f1_isnull;
    
	ArrayIterator f2_it;
	Datum		f2_val;
	bool		f2_isnull;
    
	Datum		transResult;
	Datum		maxResult;
    
	maxResult = Float8GetDatum(0);
	transResult = BoolGetDatum(false);
    
	f1_it = array_create_iterator(&f1->data, 0);
	f2_it = array_create_iterator(&f2->data, 0);
    
	while(array_iterate(f1_it, &f1_val, &f1_isnull) && array_iterate(f2_it, &f2_val, &f2_isnull)){
		if(!f1_isnull && !f2_isnull){
			
			transResult = DirectFunctionCall1(float8abs, DirectFunctionCall2(float8mi, f1_val, f2_val));
			
			if(DatumGetBool(DirectFunctionCall2(float8gt, transResult, maxResult))){
				maxResult = transResult;
			}
            
		}
	}
    
	array_free_iterator(f1_it);
	array_free_iterator(f2_it);
    
	return maxResult;
}


Datum
calculateWeightedMinkowski(PG_FUNCTION_ARGS)
{
	feature *f1 = (feature *)  PG_GETARG_VARLENA_P(0);
	feature *f2 = (feature *)  PG_GETARG_VARLENA_P(1);
	float8 n = PG_GETARG_FLOAT8(2);
	ArrayType *weights = PG_GETARG_ARRAYTYPE_P(3);
    
    Datum result;
    
	if (f1->typid != FLOAT8OID || f2->typid != FLOAT8OID){
		ereport(ERROR, (errmsg("the minkowski distance can only be used with numeric types")));
	}

	if (n - 1 < EPSILON && n > 0){
		result = calculateWeightedMinkowskiL1(f1, f2, weights);
	}
	else if (n < EPSILON && n > 0){
		result = calculateWeightedMinkowskiLmax(f1, f2, weights);
	}
	else {
		result = calculateWeightedMinkowskiLn(f1, f2, weights, Float8GetDatum(n));
	}

	PG_RETURN_DATUM(result);
}

/*
 * calculates the L1 minkowksi distance (i.e. S| x - y |) weighted
 */
static Datum
calculateWeightedMinkowskiL1(feature *f1, feature *f2, ArrayType *weights)
{
	ArrayIterator f1_it;
	Datum		f1_val;
	bool		f1_isnull;
    
	ArrayIterator f2_it;
	Datum		f2_val;
	bool		f2_isnull;
    
	ArrayIterator w_it;
	Datum		w_val;
	bool		w_isnull;
    
	float8		transResult = 0;
	float8      diff;
    
	f1_it = array_create_iterator(&f1->data, 0);
	f2_it = array_create_iterator(&f2->data, 0);
	w_it = array_create_iterator(weights, 0);
    
	while(array_iterate(f1_it, &f1_val, &f1_isnull) && array_iterate(f2_it, &f2_val, &f2_isnull) &&
          array_iterate(w_it, &w_val, &w_isnull)){
		
		if(!f1_isnull && !f2_isnull && !w_isnull){
			diff = DatumGetFloat8(f1_val) - DatumGetFloat8(f2_val);
			transResult = transResult + (DatumGetFloat8(w_val) * (diff<0 ? (-diff) : diff));
		}
	}
    
	array_free_iterator(f1_it);
	array_free_iterator(f2_it);
	array_free_iterator(w_it);
    
	PG_RETURN_FLOAT8(transResult);
}

/*
 * calculates the Ln minkowksi distance (i.e. S(x - y)^n) weighted
 */
static Datum
calculateWeightedMinkowskiLn(feature *f1, feature *f2, ArrayType *weights, Datum n)
{
	ArrayIterator f1_it;
	Datum		f1_val;
	bool		f1_isnull;
    
	ArrayIterator f2_it;
	Datum		f2_val;
	bool		f2_isnull;
    
	ArrayIterator w_it;
	Datum		w_val;
	bool		w_isnull;
    
	float8      diff;
	float8		transResult = 0;
	float8      pow;
        
	f1_it = array_create_iterator(&f1->data, 0);
	f2_it = array_create_iterator(&f2->data, 0);
	w_it = array_create_iterator(weights, 0);
    
	while(array_iterate(f1_it, &f1_val, &f1_isnull) && array_iterate(f2_it, &f2_val, &f2_isnull) &&
          array_iterate(w_it, &w_val, &w_isnull)){
		if (!f1_isnull && !f2_isnull && !w_isnull){
			diff = DatumGetFloat8(f1_val) - DatumGetFloat8(f2_val);
			pow = DatumGetFloat8(DirectFunctionCall2(dpow, Float8GetDatum(diff), n));
			transResult = transResult + (DatumGetFloat8(w_val) * pow);
		}
	}
    
	array_free_iterator(f1_it);
	array_free_iterator(f2_it);
	array_free_iterator(w_it);
    
	PG_RETURN_FLOAT8(transResult);
}

/*
 * calculates the Lmax minkowksi distance (i.e. max(x - y)) weighted
 */
static Datum
calculateWeightedMinkowskiLmax(feature *f1, feature *f2, ArrayType *weights)
{
	ArrayIterator f1_it;
	Datum		f1_val;
	bool		f1_isnull;
    
	ArrayIterator f2_it;
	Datum		f2_val;
	bool		f2_isnull;
    
	ArrayIterator w_it;
	Datum		w_val;
	bool		w_isnull;
    
	Datum		transResult;
	Datum		maxResult;
    
	maxResult = Float8GetDatum(DirectFunctionCall1(i4tod,Int32GetDatum(0)));
	transResult = BoolGetDatum(false);
    
	f1_it = array_create_iterator(&f1->data, 0);
	f2_it = array_create_iterator(&f2->data, 0);
	w_it = array_create_iterator(weights, 0);
    
	while(array_iterate(f1_it, &f1_val, &f1_isnull) && array_iterate(f2_it, &f2_val, &f2_isnull) &&
          array_iterate(w_it, &w_val, &w_isnull)){
		if(!f1_isnull && !f2_isnull && !w_isnull){
			
			transResult = DirectFunctionCall2(float8mul, w_val, DirectFunctionCall1(float8abs,
                                                                                    DirectFunctionCall2(float8mi, f1_val, f2_val)));
			
			if(DatumGetBool(DirectFunctionCall2(float8gt, transResult, maxResult))){
				maxResult = transResult;
			}
            
		}
	}
    
	array_free_iterator(f1_it);
	array_free_iterator(f2_it);
	array_free_iterator(w_it);
    
	return maxResult;
}



/*
 * given a val node from a AdamSelectStmt (i.e. a A_Const), this function returns
 * adjusts the node to a format that is used internally
 */
MinkowskiNorm 
getMinkowskiNormFromInput(Node *node)
{
	Node *value;
	MinkowskiNorm result = 0;
	
	//start off to get type of node and pre-process it
	if(IsA(node, A_Const)){
		A_Const * c = (A_Const *) node;
		value = (Node*) &c->val;
	} else {
		value = node;
	}
    
	//extract relevant information
	if(IsA(value, String) && (strcmp(strVal(value), "max") == 0 || strcmp(strVal(value), "MAX") == 0)){
		result = MINKOWSKI_MAX_NORM;
	} else if(IsA(value, String)) {
		char *in = strVal(value);
        
		if(strlen(in) <= 5){
			result = atof(in);
		}
	} else if(IsA(value, Integer)){
		result = intVal(value);
	} else if(IsA(value, Float)){
		result = floatVal(value);
	} else if(IsA(value, Const)){
		result = ((Const *) value)->constvalue;
	}
    
	//check the result
	if(result == MINKOWSKI_MAX_NORM || (result > 0 && result < 100)){
		//everything is fine
	} else {
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("only integer values [0, 100) and 'max' are allowed for Minkowski distances")));
	}
    
	return result;
}