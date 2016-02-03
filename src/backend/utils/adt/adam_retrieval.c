/* 
* ADAM - retrieval distance function
* name: adam_retrieval
* description: functions for retrieving the distance
* 
* developed in the course of the MSc thesis at the University of Basel
*
* author: Ivan Giangreco
* email: ivan.giangreco@unibas.ch
*
* src/backend/utils/adt/adam_retrieval.c
*
* 
* 
*
*/
#include "postgres.h"

#include "utils/adam_retrieval.h"
#include "utils/adam_retrieval_normalization.h"

#include "commands/adam_data_featurefunctioncmds.h"
#include "parser/adam_data_parse_featurefunction.h"
#include "utils/adam_data_feature.h"
#include "utils/adam_retrieval_minkowski.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "windowapi.h"


/*
* dummy feature distance function that should not be called
*/
Datum 
	dummyFeatureDistance(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
		(errcode(ERRCODE_INTERNAL_ERROR),
		errmsg("an internal error caused cancelling this query; no distance function could be found"),
		errhint("either remove the manual setting of the distance function or set the normalization function explicitly")));

	return ObjectIdGetDatum(InvalidOid);
}

/*
* tries to find the proc id of the distance function given the adam function options statement
* the proc id can also be "guessed", i.e. looked up if stored at creation time
*/
Oid
	getDistanceProcId(AdamFunctionOptionsStmt *distanceOp, 
	FieldSelect *ltree, 
	FeatureFunctionOpt **distanceOptions, MinkowskiNorm* nn_minkowski)
{
	Oid distanceProcId = InvalidOid;

	*distanceOptions = palloc(sizeof(FeatureFunctionOpt));
	(*distanceOptions)->opts = NIL;

	if(distanceOp && distanceOp->funname && IsA(distanceOp->funname, RangeVar)){
		//a distance function has been specified manually
		Oid ffunction = getDistanceOidFromRange((RangeVar *)distanceOp->funname, false);
		distanceProcId = getProcIdForFeatureFunId(ffunction);
	
	} else if(distanceOp && distanceOp->funname && IsA(distanceOp->funname, MinkowskiDistanceStmt)){
		MinkowskiDistanceStmt *distance = (MinkowskiDistanceStmt *)  distanceOp->funname;
		Node *minkowski_arg;

		minkowski_arg = (Node *) make_const(NULL, (Value *) distance->norm, -1);
		*nn_minkowski = getMinkowskiNormFromInput(distance->norm);

		if(!distance->weights){
			distanceProcId = MINKOWSKI_PROCOID;
			(*distanceOptions)->opts = list_make1(minkowski_arg);
		} else {
			distanceProcId = MINKOWSKI_WEIGHTED_PROCOID;
			(*distanceOptions)->opts = list_make2(minkowski_arg, make_const(NULL, (Value *) distance->weights, -1));
		}

		
		
	} else { 
		distanceProcId = InvalidOid;
	}
		
	//error checking
	if(!OidIsValid(distanceProcId)){
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("the distance function specified is not valid")));
	}

	return distanceProcId;
}

/*
* uses the inputs to create a distance-calculation node
* d(x,y)
*/
static Node *
	createDistanceCalculationNode(ParseState *pstate, List *args, FieldSelect *ltree,
	AdamFunctionOptionsStmt *distanceOp, Node **adamQueryClause, Oid *extDistanceProcId, List **extDistanceArguments, int location)
{
	Oid					 distanceProcId;
	FuncExpr			*distanceExpr;
	AdamQueryClause		*resultClause;
	
	Oid					*actual_arg_types;
	Oid					*declared_arg_types;

	MinkowskiNorm		 nn_minkowski = 0;
	Datum				*nn_minkowski_datum = palloc(sizeof(Datum));

	ListCell			*cell;
	int					 i = 0;

	int					 n = 0;

	FeatureFunctionOpt	*distanceOptions = NULL;

	// get proc id
	distanceProcId = getDistanceProcId(distanceOp, ltree, &distanceOptions, &nn_minkowski);

	if(distanceOp && distanceOp->defaults && distanceOp->defaults != NIL){
		//defaults set
		ListCell *cell;
		foreach(cell, distanceOp->defaults){
			A_Const *con = (A_Const *) lfirst(cell);
			args = list_concat(args, list_make1(make_const(pstate, &con->val, -1)));
		}
	} else {
		args = list_concat(args, distanceOptions->opts);
	}

	actual_arg_types = palloc(sizeof(Oid) * list_length(args));
	declared_arg_types = getParameterTypesFeatureFunction(distanceProcId, &n);
	
	if(list_length(args) != n){
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("the number of specified parameters for the distance function is wrong")));
	}

	foreach(cell, args){
		Node *node  = lfirst(cell);
		actual_arg_types[i] = exprType(node);
		i++;
	}

	*nn_minkowski_datum = Float8GetDatum(nn_minkowski);
	*extDistanceArguments =  list_make1(nn_minkowski_datum);
	*extDistanceProcId = distanceProcId;
	
	make_fn_arguments(pstate, args, actual_arg_types, declared_arg_types, NULL);

	// create node
	distanceExpr = makeNode(FuncExpr);
	distanceExpr->funcid = distanceProcId;
	distanceExpr->funcresulttype = getReturnTypeOfProcOid(distanceProcId);
	distanceExpr->funcretset = false;
	/* opcollid and inputcollid will be set by parse_collate.c */
	distanceExpr->args = args;
	distanceExpr->location = location;

	resultClause = makeNode(AdamQueryClause);
	resultClause->check_tid = false; //default
	resultClause->nn_minkowski = nn_minkowski;
	*adamQueryClause = (Node *) resultClause;

	return (Node *) distanceExpr;
}


/*
* tries to find the proc id of the normalization function given the adam function options statement
* the proc id can also be "guessed", i.e. looked up if stored at creation time
*/
static Oid
	getNormalizationProcId(AdamFunctionOptionsStmt *normalizationOp, 
	FieldSelect *ltree, 
	FeatureFunctionOpt **normalizationOptions)
{
	Oid normalizationProcId = InvalidOid;
	bool normalize = false;

	*normalizationOptions = palloc(sizeof(FeatureFunctionOpt));
	(*normalizationOptions)->opts = NIL;

	if(normalizationOp && normalizationOp->funname && IsA(normalizationOp->funname, RangeVar)){
		//a normalization function has been specified
		Oid ffunction = getNormalizationOidFromRange((RangeVar *)normalizationOp->funname, false);
		normalizationProcId = getProcIdForFeatureFunId(ffunction);
		normalize = true;
	} else if(normalizationOp){
		normalizationProcId = InvalidOid;
	} else {
		//no optional normalization, thus not that tragic
	}

	if(normalize && !OidIsValid(normalizationProcId)){
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("the normalization function specified is not valid")));
	}

	return normalizationProcId;
}

/*
* uses the inputs to create a normalization node
* n(input)
*/
static Node *
	createNormalizationNode(ParseState *pstate, Node *result, FieldSelect *ltree,
	AdamFunctionOptionsStmt *normalizationOp, Oid distanceProcId, List* distanceArguments, int location)
{
	Oid normalizationProcId;
	List *args = list_make1(result);

	FuncExpr *normalizationExpr;
	FeatureFunctionOpt *normalizationOptions = NULL;
		
	Oid					*actual_arg_types;
	Oid					*declared_arg_types;

	Oid					 relid;
	char			    *attname;
	int					 attnr;
	
	ListCell			*cell;
	int					 i = 0;

	int					 n = 0;

	//early termination of function, if no normalization set
	if(!normalizationOp){
		return result;
	}

	normalizationProcId = getNormalizationProcId(normalizationOp, ltree, &normalizationOptions);

	if(normalizationOp && normalizationOp->defaults && normalizationOp->defaults != NIL){
		//defaults set
		ListCell *cell;
		foreach(cell, normalizationOp->defaults){
			A_Const *con = (A_Const *) lfirst(cell);
			args = list_concat(args, list_make1(make_const(pstate, &con->val, -1)));	
		}
	} else {
		if(normalizationOptions->opts && list_length(normalizationOptions->opts) > 0 && IsA(linitial(normalizationOptions->opts), List)){
			//we are in the list mode of the normalization, dont add elements yet
		} else {
			args = list_concat(list_make1(result), normalizationOptions->opts);
		}
	}

	fieldSelectGetAttribute(ltree, &relid, &attname, &attnr);
		
	actual_arg_types = palloc(sizeof(Oid) * list_length(args));
	declared_arg_types = getParameterTypesFeatureFunction(normalizationProcId, &n);

	//use precomputed parameters
	if((normalizationProcId == MINMAX_NORMALIZATION 
		|| normalizationProcId == GAUSSIAN_NORMALIZATION)
		&& list_length(args) != n){
		
		Datum *values = getNormalizationStatistics(relid, attname, distanceProcId, distanceArguments, false);

		if(normalizationProcId == MINMAX_NORMALIZATION){
			float4   floatVal = DatumGetFloat8(values[0]);
			A_Const *constVal = (A_Const *) makeFloatAConstFloat(floatVal);
			args = lappend(args, make_const(pstate, &constVal->val, -1));
		}

		if(normalizationProcId == GAUSSIAN_NORMALIZATION){
			float4   floatVal1 = DatumGetFloat8(values[1]);
			A_Const *constVal1 = (A_Const *) makeFloatAConstFloat(floatVal1);
			float4   floatVal2 = DatumGetFloat8(values[2]);
			A_Const *constVal2 = (A_Const *) makeFloatAConstFloat(floatVal2);

			args = lappend(args, make_const(pstate, &constVal1->val, -1));
			args = lappend(args, make_const(pstate, &constVal2->val, -1));
		}
	}
	
	if(list_length(args) != n){
		if(normalizationProcId == MINMAX_NORMALIZATION 
		|| normalizationProcId == GAUSSIAN_NORMALIZATION){
			//better error message with hint to do a precomputation
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("the number of specified parameters for the normalization function is wrong"),
				errhint("use PRECOMPUTE NORMALIZATION FOR <field> FROM <table> to pre-compute the parameter values")));
		} else {
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("the number of specified parameters for the normalization function is wrong")));
		}
	}

	foreach(cell, args){
		Node *node  = lfirst(cell);
		actual_arg_types[i] = exprType(node);
		i++;
	}
	
	make_fn_arguments(pstate, args, actual_arg_types, declared_arg_types, NULL);
	
	if(OidIsValid(normalizationProcId)){
		normalizationExpr = makeNode(FuncExpr);
		normalizationExpr->funcid = normalizationProcId;
		normalizationExpr->funcresulttype = getReturnTypeOfProcOid(normalizationProcId);
		normalizationExpr->funcretset = false;
		/* opcollid and inputcollid will be set by parse_collate.c */
		normalizationExpr->args = args;
		normalizationExpr->location = location;
		result = (Node *) normalizationExpr;
	}

	return result;
}




/*
* uses the inputs to create a weighting node
* i.e. constant multiplication
* t * input
*/
static Node *
	createDistanceWeightNode(ParseState *pstate, Node *result, 
	Expr *weight, int location)
{

	if(weight){
		return (Node *) makeSimpleA_Expr(AEXPR_OP, "*", (Node *) weight, result, location);
	}

	return result;
}



/*
* uses the inputs to create a weighting node for except clauses
* i.e. complement calculation
* c(input)
*/
static Node *
	createExceptComplementNode(ParseState *pstate, Node *result, 
	Node *except, int location)
{

	if(except && IsA(except, FuncCall)){
		FuncCall *exceptfunc = (FuncCall*) except;
		exceptfunc->args = lcons(result, exceptfunc->args);
		result = (Node *) exceptfunc;
	}

	return result;
}




/*
* creates the appropriate expression for inserting in the parse tree of which functions to use for the calculation
* of the distance and the normalization; since this is done in the parse tree, the tree might at the end look differently
* if optimized
*/
Node* 
	adjustParseTreeForFeatureSearch(ParseState *pstate, AdamSelectStmt *adamSelectStmt, Node **adamQueryClause, int location)
{
	List					*args;
	Node					*result;

	AdamFunctionOptionsStmt *distanceOp = (AdamFunctionOptionsStmt *) adamSelectStmt->distance;
	AdamFunctionOptionsStmt *normalizationOp = (AdamFunctionOptionsStmt *) adamSelectStmt->normalization;
	Expr					*weight = (Expr*) adamSelectStmt->weight;
	Node					*except = adamSelectStmt->except;

	Node					*transLExpr;
	Node					*transRExpr;
	Node					*transExpr;

	Oid						 distanceProcId;
	List					 *distanceArguments;

	//guessing of the normalization is not allowed, if the distance has been set manually
	//we allow this now because the parameters can have been precomputed for various distances
	/*if( distanceOp != NULL && distanceOp->funname != NULL &&
		normalizationOp != NULL && normalizationOp->funname == NULL){
		ereport(ERROR,
		(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		errmsg("distance function manually set; cannot use normalization specified in field")));
	}*/

	transLExpr = transformExpr(pstate, adamSelectStmt->l_expr, EXPR_KIND_FUNCTION_DEFAULT);
	transRExpr = transformExpr(pstate, adamSelectStmt->r_expr, EXPR_KIND_FUNCTION_DEFAULT);

	args = list_make2(transLExpr, transRExpr);

	if(IsA(transLExpr, FieldSelect)){
		transExpr = transLExpr;
	} else if(IsA(transLExpr, Const) && IsA(transRExpr, FieldSelect)){
		transExpr = transRExpr;
	} else {
		if(!distanceOp || !distanceOp->funname){
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("distance function must be manually set")));
		}

		if(normalizationOp && (!normalizationOp->funname || !IsA(normalizationOp->funname, RangeVar))){
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("normalization function must be manually set")));
		}
	}


	//d(x,y)
	result = createDistanceCalculationNode(pstate, args, (FieldSelect *) transExpr, distanceOp, adamQueryClause, &distanceProcId, &distanceArguments, location);
	
	//n(d(x,y))
	result = createNormalizationNode(pstate, result, (FieldSelect *) transExpr, normalizationOp, distanceProcId, distanceArguments, location);
	
	//t * n(d(x,y))
	result = createDistanceWeightNode(pstate, result, weight, location);

	//c(t * n(d(x,y))
	result = createExceptComplementNode(pstate, result, except, location);

	return result;
}


/*
* use field select and try to find the default distance function for that field
*/
void
	fieldSelectGetAttribute(FieldSelect *field, Oid *relid, char** attribute, int *fieldNum)
{
	HeapTuple        tuple;
	*attribute = NULL;

	*relid = InvalidOid;

	if(IsA(field->arg, Var)){
		Var *var = (Var *) field->arg;

		*relid = typeidTypeRelid(var->vartype);
		tuple = SearchSysCache2(ATTNUM, ObjectIdGetDatum(*relid), Int32GetDatum(field->fieldnum));

		*fieldNum = field->fieldnum;

		if(HeapTupleIsValid(tuple)){
			*attribute = ((Form_pg_attribute) GETSTRUCT(tuple))->attname.data;
			ReleaseSysCache(tuple);
		}
	}
}


/*
* default name for distance field
*/
char*
	getDistanceFieldName()
{
	return pstrdup("d");
}