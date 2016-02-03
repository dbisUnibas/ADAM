/* 
* ADAM - feature type
* name: adam_data_feature
* description: functions defining the feature datatype in ADAM
* 
* developed in the course of the MSc thesis at the University of Basel
*
* author: Ivan Giangreco
* email: ivan.giangreco@unibas.ch
*
* src/backend/utils/adt/adam_data_feature.c
*
* 
* 
*
*/
#include "postgres.h"

#include "utils/adam_data_feature.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#include <assert.h>

#define MIN(X, Y) ((X) < (Y) ? (X) : (Y))

static bool checkEqual(FunctionCallInfo fcinfo, feature *f1, feature *f2);
static Datum compare(feature *f1, feature *f2, FunctionCallInfo fcinfo, Datum (*fpointer)(FunctionCallInfo));


/*
*  feature in function for feature data, i.e. it reads in data
*/
Datum
	feature_in(PG_FUNCTION_ARGS)
{
	int32	    typid,
		typidarr;
	int16		typlen;		
	bool		typbyval;	
	char		typalign,	
		typdelim;	
	Oid			typioparam,
		typinfunc,	//pointer to in-function to be called
		typelem;	//pointer to input

	Datum		result;		//result of the in-function

	char	   *in;
	char	   *ptr;
	StringInfoData buf;
	char	   *in_result;

	feature	   *f;

	in = PG_GETARG_CSTRING(0);

	//scan the string
	ptr = in;

	//allow leading whitespace
	while (*ptr && isspace((unsigned char) *ptr))
		ptr++;
	if (*ptr != '<' && *ptr != '[')
		ereport(ERROR,
		(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
		errmsg("malformed record literal: \"%s\"", in),
		errdetail("Missing left parenthesis, expecting \"<\".")));

	initStringInfo(&buf);

	ptr++;

	while (*ptr != '>' && *ptr != ']'){
		char		ch = *ptr++;

		if (ch == '\0')
			ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			errmsg("malformed record literal: \"%s\"", in),
			errdetail("Unexpected end of input.")));
		if (ch == '\\') {
			if (*ptr == '\0')
				ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				errmsg("malformed record literal: \"%s\"", in),
				errdetail("Unexpected end of input.")));
			appendStringInfoChar(&buf, *ptr++);
		} else
			appendStringInfoChar(&buf, ch);
	}


	if (*ptr++ != '>' && *ptr != ']')
		ereport(ERROR,
		(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
		errmsg("malformed record literal: \"%s\"", in),
		errdetail("Too many columns.")));
	//closing
	while (*ptr && isspace((unsigned char) *ptr))
		ptr++;
	if (*ptr)
		ereport(ERROR,
		(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
		errmsg("malformed record literal: \"%s\"", in),
		errdetail("Junk after right parenthesis.")));


	in_result  = (char *) palloc(buf.len + 2 + 1);
	snprintf(in_result, buf.len + 2 + 1, "{%s}", buf.data);

	//a pointer to the input
	typelem =  PG_GETARG_OID(1);

	typid =  FLOAT8OID;
	typidarr = get_array_type(typid);


	get_type_io_data(typidarr, IOFunc_input, &typlen, &typbyval, &typalign, &typdelim, &typioparam, &typinfunc);


	//call in-function with collation already used and with parameters
	//note that the parameters have been adjusted: 
	// - we use PG_GETARG_DATUM(...) since function call asks for a datum (so no casting is necessary)
	// - we replace the second function (i.e. 1) parameter by the third (i.e. 2) since this is the one pointing to the true array type
	// - we use -1 for the third, i.e. the typemod, parameter
	result = OidFunctionCall3Coll(
		typinfunc, 
		fcinfo->fncollation, 
		CStringGetDatum(in_result), Int32GetDatum(typid), Int32GetDatum(-1));


	//create a feature struct, set correct size
	f = (feature *) palloc(VARHDRSZ + sizeof(int32) + VARSIZE_ANY(DatumGetPointer(result)));
	SET_VARSIZE(f, VARHDRSZ + sizeof(int32) + VARSIZE_ANY(DatumGetPointer(result)));

	//copy result to feature
	f->typid = typid;
	memcpy(&f->data, DatumGetPointer(result), VARSIZE_ANY(DatumGetPointer(result)));


	PG_RETURN_POINTER(f);
}

/*
*  feature out function for feature data, i.e. it writes out data
*/
Datum
	feature_out(PG_FUNCTION_ARGS)
{
	int32	    typid,
		typidarr;
	int16		typlen;		
	bool		typbyval;	
	char		typalign,	
		typdelim;	
	Oid			typioparam,
		typoutfunc;	//pointer to out-function to be called

	Datum		result;		//result of the out-function

	char	   *result_out;
	feature	   *f;

	f =  (feature *) PG_GETARG_POINTER(0);

	typid =  f->typid;
	typidarr = get_array_type(typid);

	get_type_io_data(typidarr, IOFunc_output, &typlen, &typbyval, &typalign, &typdelim, &typioparam, &typoutfunc);

	result = OidFunctionCall1Coll(
		typoutfunc, 
		fcinfo->fncollation, 
		PointerGetDatum(&f->data));

	result_out = DatumGetCString(result);
	result_out[0] = '<';
	result_out[strlen(result_out) -1] = '>';

	PG_RETURN_CSTRING(result_out);
}


/*
* casts a feature to an array
*/
Datum
	featureArrayCast(PG_FUNCTION_ARGS)
{
	feature	   *f;

	f =  (feature *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
	PG_RETURN_POINTER(&(f->data));
}


/*
* casts an array to a feature object
* this is needed for algorithm functions
*/
Datum
	arrayFeatureCast(PG_FUNCTION_ARGS)
{
	feature	   *f;
	ArrayType * arrayptr =  PG_GETARG_ARRAYTYPE_P(0);

	int typid = -1;

	//typ id is given via a typemod
	if(PG_NARGS() == 2){
		typid =  PG_GETARG_INT32(1);
	}

	f = (feature *) palloc(VARHDRSZ + sizeof(int32) + VARSIZE_ANY(arrayptr));
	SET_VARSIZE(f, VARHDRSZ + sizeof(int32) + VARSIZE_ANY(arrayptr));

	//copy result to feature
	if(typid > 0 && get_element_type(typid) > 0){
		f->typid = get_element_type(typid);
	} else {
		f->typid = arrayptr->elemtype;
	}

	memcpy(&f->data, arrayptr, VARSIZE_ANY(arrayptr));

	PG_RETURN_POINTER(f);
}

/*
* array datum to feature function
*/
Datum
	featureFromArray(FmgrInfo *fmgr, Datum arr, Oid typioparam, int32 typmod)
{
	Oid typinput;
	Oid typoutput;
	Oid typparam;

	Datum input_function_result;
	char* datum_to_char_result;
	feature *f;
	bool typvarlena;

	Oid typidarr = get_array_type(typmod);

	getTypeInputInfo(typidarr, &typinput, &typparam);
	getTypeOutputInfo(typidarr, &typoutput, &typvarlena);

	fmgr->fn_mcxt = CurrentMemoryContext;

	fmgr_info_cxt(typoutput, fmgr, fmgr->fn_mcxt);
	datum_to_char_result = OutputFunctionCall(fmgr, arr);

	fmgr_info_cxt(typinput, fmgr, fmgr->fn_mcxt);
	input_function_result = InputFunctionCall(fmgr, datum_to_char_result, typmod, -1);

	//create a feature struct, set correct size
	f = (feature *) palloc(VARHDRSZ + sizeof(int32) + VARSIZE_ANY(DatumGetPointer(input_function_result)));
	SET_VARSIZE(f, VARHDRSZ + sizeof(int32) + VARSIZE_ANY(DatumGetPointer(input_function_result)));

	//copy result to feature
	f->typid = typmod;
	memcpy(&f->data, DatumGetPointer(input_function_result), VARSIZE_ANY(DatumGetPointer(input_function_result)));


	PG_RETURN_POINTER(f);
}

/*
* formats the data name (used for format_type.c)
*/
char *
	getFeatureName(int32 typemod)
{
	//char	  *name;
	char	  *result;

	/*if(typemod != -1){
		name = typeTypeName(typeidType(typemod));

		result = (char *) palloc(100);
		snprintf(result, 100, "feature(%s)", name);
	} else {*/
		result = pstrdup("feature");
	//}

	return result;
}


/*
* checks whether the features are equal
*/
Datum
	feature_eq(PG_FUNCTION_ARGS)
{
	feature   *f1 = (feature *) PG_GETARG_VARLENA_P(0);
	feature   *f2 = (feature *) PG_GETARG_VARLENA_P(1);

	PG_RETURN_BOOL(checkEqual(fcinfo, f1, f2));
}

/*
* checks whether the features are not equal
*/
Datum
	feature_neq(PG_FUNCTION_ARGS)
{
	feature   *f1 = (feature *) PG_GETARG_VARLENA_P(0);
	feature   *f2 = (feature *) PG_GETARG_VARLENA_P(1);

	PG_RETURN_BOOL(!checkEqual(fcinfo, f1, f2));
}

/*
* checks whether two features are equal
*/
static bool 
	checkEqual(FunctionCallInfo fcinfo, feature *f1, feature *f2)
{
	bool		result;

	fcinfo->arg[0] = PointerGetDatum(&f1->data);
	fcinfo->arg[1] = PointerGetDatum(&f2->data);

	fcinfo->fncollation = InvalidOid;

	result = DatumGetBool(array_eq(fcinfo));

	return result;
}

/*
* checks whether a < b,
* where a is first feature argument, b second feature argument
*/
Datum
	feature_lt(PG_FUNCTION_ARGS)
{
	feature   *f1 = (feature *) PG_GETARG_VARLENA_P(0);
	feature   *f2 = (feature *) PG_GETARG_VARLENA_P(1);

	PG_RETURN_DATUM(compare(f1, f2, fcinfo, &array_lt));
}

/*
* checks whether a > b,
* where a is first feature argument, b second feature argument
*/
Datum
	feature_gt(PG_FUNCTION_ARGS)
{
	feature   *f1 = (feature *) PG_GETARG_VARLENA_P(0);
	feature   *f2 = (feature *) PG_GETARG_VARLENA_P(1);


	PG_RETURN_DATUM(compare(f1, f2, fcinfo, &array_gt));
}

/*
* checks whether a <= b,
* where a is first feature argument, b second feature argument
*/
Datum
	feature_le(PG_FUNCTION_ARGS)
{
	feature   *f1 = (feature *) PG_GETARG_VARLENA_P(0);
	feature   *f2 = (feature *) PG_GETARG_VARLENA_P(1);

	PG_RETURN_DATUM(compare(f1, f2, fcinfo, &array_le));
}

/*
* checks whether a >= b,
* where a is first feature argument, b second feature argument
*/
Datum
	feature_ge(PG_FUNCTION_ARGS)
{
	feature   *f1 = (feature *) PG_GETARG_VARLENA_P(0);
	feature   *f2 = (feature *) PG_GETARG_VARLENA_P(1);

	PG_RETURN_DATUM(compare(f1, f2, fcinfo, &array_ge));
}

/*
* checks relation of two feature arguments and returns -1, 0, 1
*/
Datum
	feature_cmp(PG_FUNCTION_ARGS)
{
	feature   *f1 = (feature *) PG_GETARG_VARLENA_P(0);
	feature   *f2 = (feature *) PG_GETARG_VARLENA_P(1);

	PG_RETURN_DATUM(compare(f1, f2, fcinfo, &btarraycmp));
}

/*
* given a function pointer, this function checks two features by passing
* its data to the function given
*/
Datum
	compare(feature *f1, feature *f2, FunctionCallInfo fcinfo, Datum (*fpointer)(FunctionCallInfo))
{
	fcinfo->arg[0] = PointerGetDatum(&f1->data);
	fcinfo->arg[1] = PointerGetDatum(&f2->data);
	fcinfo->fncollation = InvalidOid;
	PG_RETURN_DATUM(fpointer(fcinfo));
}

/*
* calculates the hash for a feature
*/
Datum
	feature_hash(PG_FUNCTION_ARGS)
{
	feature   *f = (feature *) PG_GETARG_VARLENA_P(0);
	void *     ptr = palloc(VARSIZE_ANY_EXHDR(f) - sizeof(int32));

	memcpy(ptr, &f->data, VARSIZE_ANY_EXHDR(f) - sizeof(int32));

	fcinfo->arg[0] = PointerGetDatum(ptr);

	return hash_array(fcinfo);
}

/*
* dummy feature eq, returns always true
* used to adjust indexing
*/
Datum
	feature_dummy_eq(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(true);
}

/*
* returns minimum of every entry of two arrays
* if the arrays have not the same size, only i = min(dim f1, dim f2), 
* i.e. the smallest number of dimensions are considered
*/
Datum
	feature_min(PG_FUNCTION_ARGS)
{
		feature   *f1 = (feature *) PG_GETARG_VARLENA_P(1);
	ArrayType *a;

	ArrayIterator f1_it;
	Datum		f1_val;
	bool		f1_isnull;

	ArrayIterator f2_it;
	Datum		f2_val;
	bool		f2_isnull;

	int			i = 0;
	int			dims = 0;
	
	int			cmpResult;
	Datum		*transResult;
	
	ArrayType *result;

	if(PG_ARGISNULL(0)){
		//its not enough to copy the array (via memcopy) since the datums contain 
		//pointer to external values (this is true for large numeric values!), 
		//since the caller function, however, might clean this data after getting
		//and processing the tuple, we have to copy the external values too
		//thus we use the "numeric" function to copy the numeric
		dims = ArrayGetNItems(ARR_NDIM(&f1->data), ARR_DIMS(&f1->data));

		transResult = palloc(dims * sizeof(Datum));

		f1_it = array_create_iterator(&(f1->data), 0);
		
		while(array_iterate(f1_it, &f1_val, &f1_isnull)){
			if(!f1_isnull){
				transResult[i] = f1_val;
			}

			i++;
		}

		result = construct_array(transResult, dims, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');

		pfree(transResult);
		array_free_iterator(f1_it);

		PG_RETURN_ARRAYTYPE_P(result);
	}
	
	a =  PG_GETARG_ARRAYTYPE_P(0);
	dims = MIN(ArrayGetNItems(ARR_NDIM(&f1->data), ARR_DIMS(&f1->data)),
		ArrayGetNItems(ARR_NDIM(a), ARR_DIMS(a)));

	transResult = palloc(dims * sizeof(Datum));
		
	f1_it = array_create_iterator(&(f1->data), 0);
	f2_it = array_create_iterator(a, 0);	
	
	while(array_iterate(f1_it, &f1_val, &f1_isnull) && array_iterate(f2_it, &f2_val, &f2_isnull)){
		if(!f1_isnull && !f2_isnull && i < dims){
	
			cmpResult = DirectFunctionCall2(btfloat8cmp, f1_val, f2_val);
	
			//call to numeric copies the numeric value
			if(cmpResult > 0){
				transResult[i] = f2_val;
			} else if (cmpResult < 0){
				transResult[i] = f1_val;
			} else {
				transResult[i] = f2_val;
			}
	
			i++;
		}	
	}
	
	result = construct_array(transResult, dims, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
	
	pfree(transResult);
	array_free_iterator(f1_it);
	array_free_iterator(f2_it);
	
	PG_RETURN_ARRAYTYPE_P(result);
}

/*
* returns maximum of every entry of two arrays
* if the arrays have not the same size, only i = min(dim f1, dim f2), 
* i.e. the smallest number of dimensions are considered
*/
Datum
	feature_max(PG_FUNCTION_ARGS)
{
	feature   *f1 = (feature *) PG_GETARG_VARLENA_P(1);
	ArrayType *a;

	ArrayIterator f1_it;
	Datum		f1_val;
	bool		f1_isnull;

	ArrayIterator f2_it;
	Datum		f2_val;
	bool		f2_isnull;

	int			i = 0;
	int			dims = 0;
	
	int			cmpResult;
	Datum		*transResult;
	
	ArrayType *result;

	if(PG_ARGISNULL(0)){
		//its not enough to copy the array (via memcopy) since the datums contain 
		//pointer to external values (this is true for large numeric values!), 
		//since the caller function, however, might clean this data after getting
		//and processing the tuple, we have to copy the external values too
		//thus we use the "numeric" function to copy the numeric
		dims = ArrayGetNItems(ARR_NDIM(&f1->data), ARR_DIMS(&f1->data));

		transResult = palloc(dims * sizeof(Datum));

		f1_it = array_create_iterator(&(f1->data), 0);
		
		while(array_iterate(f1_it, &f1_val, &f1_isnull)){
			if(!f1_isnull){
				transResult[i] = f1_val;
			}

			i++;
		}

		result = construct_array(transResult, dims, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');

		pfree(transResult);
		array_free_iterator(f1_it);

		PG_RETURN_ARRAYTYPE_P(result);
	}
	
	a =  PG_GETARG_ARRAYTYPE_P(0);
	dims = MIN(ArrayGetNItems(ARR_NDIM(&f1->data), ARR_DIMS(&f1->data)),
		ArrayGetNItems(ARR_NDIM(a), ARR_DIMS(a)));
		
	transResult = palloc(dims * sizeof(Datum));

	f1_it = array_create_iterator(&(f1->data), 0);
	f2_it = array_create_iterator(a, 0);	
	
	while(array_iterate(f1_it, &f1_val, &f1_isnull) && array_iterate(f2_it, &f2_val, &f2_isnull)){
		if(!f1_isnull && !f2_isnull && i < dims){
	
			cmpResult = DirectFunctionCall2(btfloat8cmp, f1_val, f2_val);
	
			//call to numeric copies the numeric value
			if(cmpResult > 0){
				transResult[i] = f1_val;
			} else if (cmpResult < 0){
				transResult[i] = f2_val;
			} else {
				transResult[i] = f1_val;
			}
	
			i++;
		}	
	}
	
	result = construct_array(transResult, dims, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
	
	pfree(transResult);
	array_free_iterator(f1_it);
	array_free_iterator(f2_it);
	
	PG_RETURN_ARRAYTYPE_P(result);
}

/*
*  end function for MIN/MAX for features (transforms array into feature)
*/
Datum
	feature_minmax_end(PG_FUNCTION_ARGS)
{
	ArrayType *a;

	if(PG_ARGISNULL(0)){
		return PointerGetDatum(NULL);
	}

	a = PG_GETARG_ARRAYTYPE_P(0);

	return DirectFunctionCall1(arrayFeatureCast, PointerGetDatum(a));
}