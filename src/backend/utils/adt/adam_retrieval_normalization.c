/* 
* ADAM - Min/Max-Normalization
* name: adam_retrieval_minmax
* description: functions for calculating the Min/Max-Normalization
* 
* developed in the course of the MSc thesis at the University of Basel
*
* author: Ivan Giangreco
* email: ivan.giangreco@unibas.ch
*
* src/backend/utils/adt/adam_retrieval_normalization.c
*
* 
* 
*
*/
#include "postgres.h"

#include "utils/adam_retrieval_normalization.h"

#include "parser/adam_data_parse_featurefunction.h"
#include "utils/adam_retrieval.h"
#include "utils/adam_data_feature.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "commands/vacuum.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "parser/parse_clause.h"
#include "parser/parse_node.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"
#include "utils/typcache.h"
#include "funcapi.h"

#define EPSILON	0.001

static void getSampledRows(Relation rel, HeapTuple **rows, double *totalRowsReturned);

/*
* calculates the min/max-normalization given a distance
*/
Datum
	normalizeMinMax(PG_FUNCTION_ARGS)
{
	Datum distance = PG_GETARG_DATUM(0);
	Datum max_distance = PG_GETARG_DATUM(1);

	Datum result;

	if(DatumGetBool(DirectFunctionCall2(numeric_ge, distance, max_distance))){
		result = NumericGetDatum(get_const_one());
	} else {
		//  1/max * dist
		result = DirectFunctionCall2(numeric_mul,
			DirectFunctionCall2(numeric_div, NumericGetDatum(get_const_one()), max_distance),
			distance
			);
	}

	//make sure result is element [0,1]
	if(DatumGetBool(DirectFunctionCall2(numeric_gt, result, NumericGetDatum(get_const_one())))){
		result = NumericGetDatum(get_const_one());
	}
	if(DatumGetBool(DirectFunctionCall2(numeric_lt, result, NumericGetDatum(get_const_zero())))){
		result = NumericGetDatum(get_const_zero());
	}

	PG_RETURN_DATUM(result);
}

Datum
	normalizeGaussian(PG_FUNCTION_ARGS)
{
	Datum distance = PG_GETARG_DATUM(0);
	Datum mu = PG_GETARG_DATUM(1);
	Datum sigma = PG_GETARG_DATUM(2);

	Datum result;

	// 0.5 * (((dist - mu) / (3 sigma)) + 1) = ((dist - mu) / (6 sigma)) + 0.5
	result = DirectFunctionCall2(numeric_add,
		DirectFunctionCall2(numeric_div,
		DirectFunctionCall2(numeric_sub, distance, mu),
		DirectFunctionCall2(numeric_mul, DirectFunctionCall1(int4_numeric, 6), sigma)),
		DirectFunctionCall1(float8_numeric, Float8GetDatum(0.5))
		);
	
	//make sure result is element [0,1]
	if(DatumGetBool(DirectFunctionCall2(numeric_gt, result, NumericGetDatum(get_const_one())))){
		result = NumericGetDatum(get_const_one());
	}
	if(DatumGetBool(DirectFunctionCall2(numeric_lt, result, NumericGetDatum(get_const_zero())))){
		result = NumericGetDatum(get_const_zero());
	}

	PG_RETURN_DATUM(result);
}

Datum* 
	getNormalizationStatistics(Oid relid, char* colname, Oid distanceProcid, List*arguments, bool noError)
{
	Datum  *results = palloc(sizeof(Datum) * 3);
	
	//TODO: set type here
	List   *opt = NIL;

	int i = 0;
	ListCell *cell;
	
	if(opt == NIL){
		if(distanceProcid == MINKOWSKI_PROCOID){
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("normalization parameters missing"),
				errhint("Use PRECOMPUTE NORMALIZATION FOR <field> FROM <table>")));
		} else {
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("normalization parameters missing"),
				errhint("add normalization function with all function parameters to statement")));
		}
	}

	
	foreach(cell, opt){
		Value* retrarg = (Value *) lfirst(cell);
		results[i] = Float8GetDatum(floatVal(retrarg));
		i++;
	}

	return results;
}

/*
* performs call for distance calculation
*/
static Datum 
	calculateDistance(FunctionCallInfoData *infoData, feature *f1, feature *f2, List *arguments)
{
	ListCell *cell;
	int i = 0;

	infoData->arg[0] = PointerGetDatum(f1);
	infoData->argnull[0] = false;
	infoData->arg[1] = PointerGetDatum(f2);
	infoData->argnull[1] = false;

	foreach(cell, arguments){
		Datum *argument = (Datum *) lfirst(cell);

		infoData->arg[i + 2] = *argument;
		infoData->argnull[i + 2] = false;

		i++;
	}
	
	infoData->isnull = false;
	return FunctionCallInvoke(&*infoData);
}

/*
* iterates through rows and does calculation of distance
*/
static void
	calculateNormalizationParameters (Relation rel, IndexInfo *indexInfo, 
	Oid distanceOid, List *arguments, HeapTuple *rows, double numRows, List **results)
{
	FmgrInfo distance;
	FunctionCallInfoData distanceInfoData;

	TupleTableSlot *slot1, *slot2;
	EState *estate1, *estate2;
	ExprContext *econtext1, *econtext2;

	Datum		f1_value, f2_value;
	bool		f1_isnull, f2_isnull;

	feature    *f1, *f2;

	Datum		calcDist;
	Datum		*transMaxDist = palloc(sizeof(Datum));   
	Datum		*transMuDist = palloc(sizeof(Datum));      
	Datum		*transSigmaDist = palloc(sizeof(Datum));   
	int			rows_cntr = 0;

	int i1 = 0, i2 = 0;
	
	*transMaxDist = DirectFunctionCall1(int4_numeric, Int32GetDatum(0));
	*transMuDist = DirectFunctionCall1(int4_numeric, Int32GetDatum(0));
	*transSigmaDist = DirectFunctionCall1(int4_numeric, Int32GetDatum(0));

	*results = NIL;

	slot1 = MakeSingleTupleTableSlot(RelationGetDescr(rel));
	slot2 = MakeSingleTupleTableSlot(RelationGetDescr(rel));

	estate1 = CreateExecutorState();
	estate2 = CreateExecutorState();

	econtext1 = GetPerTupleExprContext(estate1);
	econtext1->ecxt_scantuple = slot1;
	econtext2 = GetPerTupleExprContext(estate2);
	econtext2->ecxt_scantuple = slot2;

	fmgr_info(distanceOid, &distance);
	InitFunctionCallInfoData(distanceInfoData, &distance, 2 + list_length(arguments), InvalidOid, NULL, NULL);


	for(i1 = 0; i1 < numRows; i1++){
		//get f1
		if(!rows[i1]){
			continue;
		}

		ResetExprContext(econtext1);
		ExecStoreTuple(rows[i1], slot1, InvalidBuffer, false);
		FormIndexDatum(indexInfo, slot1,  estate1, &f1_value,  &f1_isnull);

		if(f1_isnull){
			continue;
		}

		f1 = (feature *) DatumGetPointer(f1_value);

		for(i2 = 0; i2 < numRows; i2++){
			//get f2
			if(!rows[i2]){
				continue;
			}

			ResetExprContext(econtext2);
			ExecStoreTuple(rows[i2], slot2, InvalidBuffer, false);
			FormIndexDatum(indexInfo, slot2,  estate2, &f2_value,  &f2_isnull);

			if(f2_isnull){
				continue;
			}
			
			f2 = (feature *) DatumGetPointer(f2_value);

			
			//distance calculation
			calcDist = calculateDistance(&distanceInfoData, f1, f2, arguments);


			//calculate maximum
			if(DatumGetBool(DirectFunctionCall2(numeric_gt, calcDist, *transMaxDist))){
				*transMaxDist = calcDist;
			}

			//calculate mu and sigma
			*transMuDist = DirectFunctionCall2(numeric_add, *transMuDist, calcDist);
			*transSigmaDist = DirectFunctionCall2(numeric_add, *transSigmaDist, DirectFunctionCall2(numeric_mul, calcDist, calcDist));
			rows_cntr++;
		}
	}

	//max
	*transMaxDist = DirectFunctionCall2(numeric, *transMaxDist, Int32GetDatum(-1));

	//sigma = ((sqrt(N * s2 - s1^2)) /  N) where sj = SUM(x^j)
	//attention: must come before mu!
	*transSigmaDist = DirectFunctionCall2(numeric_mul, DirectFunctionCall1(int4_numeric, Int32GetDatum(rows_cntr)), *transSigmaDist);
	*transSigmaDist = DirectFunctionCall2(numeric_sub, *transSigmaDist, DirectFunctionCall2(numeric_mul, *transMuDist, *transMuDist));
	*transSigmaDist = DirectFunctionCall2(numeric_div, *transSigmaDist, 
		DirectFunctionCall1(int4_numeric, Int32GetDatum(rows_cntr * (rows_cntr - 1))));
	*transSigmaDist = DirectFunctionCall1(numeric_sqrt, *transSigmaDist);

	//mu = 1/N SUM(xi)
	*transMuDist = DirectFunctionCall2(numeric_div, *transMuDist, DirectFunctionCall1(int4_numeric, Int32GetDatum(rows_cntr)));
	*transMuDist = DirectFunctionCall2(numeric, *transMuDist, Int32GetDatum(-1));

	
	//add to results for storage
	*results = lappend(*results, transMaxDist);
	*results = lappend(*results, transMuDist);
	*results = lappend(*results, transSigmaDist);
		
	ExecDropSingleTupleTableSlot(slot1);
	FreeExecutorState(estate1);
	
	ExecDropSingleTupleTableSlot(slot2);
	FreeExecutorState(estate2);
}

/*
* gets field information for field for which precomputation is done
*/
TargetEntry*
	getTransformedTargetEntry(Node *relation, List *targetList)
{
	ParseState *pstate	= make_parsestate(NULL);

	List *transformedTargetList = NIL;
	Node *targetEntry = NULL;
	Expr *targetExpr = NULL;

	Node *result = NULL;

	if(!targetList || targetList == NIL || list_length(targetList) != 1){
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			errmsg("precomputation can only be performed for one element at a time")));
	}

	transformFromClause(pstate, list_make1(relation));
	transformedTargetList = transformTargetList(pstate, targetList, EXPR_KIND_SELECT_TARGET);

	targetEntry = linitial(transformedTargetList);

	if(IsA(targetEntry, TargetEntry)){
		targetExpr = ((TargetEntry *) targetEntry)->expr;

		if(targetExpr && IsA(targetExpr, FieldSelect)){
			result = targetEntry;
		}
	}

	if(!result){
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			errmsg("precomputation has an internal error")));
	}

	return (TargetEntry *) result;
}


/*
* precomputes values necessary for normalization
*/
void
	adjustAdamNormalizationPrecomputeStmt(AdamNormalizationPrecomputeStmt *stmt)
{
	Oid relid			= RangeVarGetRelid(stmt->relation, ShareLock, false);
	Relation rel		= RelationIdGetRelation(relid);

	HeapTuple *rows	    = palloc(sizeof(HeapTuple) * N_SAMPLES);
	double numRows		= 0;

	TargetEntry *targetEntry = getTransformedTargetEntry((Node *) stmt->relation, stmt->targetList);
	FieldSelect	*targetFieldSelect =  (FieldSelect *) targetEntry->expr;
	Var         *targetVar = (Var *) targetFieldSelect->arg;
	Oid distance;
	FeatureFunctionOpt *distanceOptions = NULL;
	MinkowskiNorm nn_minkowski = 0;
	Datum		  nn_minkowski_datum;
	
	IndexInfo *indexInfo;
	List *arguments;
	List *results;
	
	getSampledRows(rel, &rows, &numRows);
		
	distance = getDistanceProcId((AdamFunctionOptionsStmt *) stmt->distance, targetFieldSelect, &distanceOptions, &nn_minkowski);
	if(distance != MINKOWSKI_PROCOID){
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			errmsg("precomputation can only be performed with Minkowski distances")));
	}
	
	nn_minkowski_datum = DirectFunctionCall1(float8_numeric, Float8GetDatum(nn_minkowski));
	arguments = list_make1(&nn_minkowski_datum);
		
	//build index info node
	indexInfo = makeNode(IndexInfo);
	indexInfo->ii_NumIndexAttrs = 1;
	indexInfo->ii_Expressions = list_make1(targetFieldSelect);
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_Predicate = NIL;
	indexInfo->ii_PredicateState = NIL;
	
	calculateNormalizationParameters(rel, indexInfo, distance, arguments, rows, numRows, &results);
	
	//TODO: print results?
	//storeResults(typeidTypeRelid(targetVar->vartype), targetEntry->resname, distance, arguments, results);

	pfree(rows);
	relation_close(rel, ShareLock);
}


/*
* get sampled rows
*/
static void 
	getSampledRows(Relation rel, HeapTuple **rows, double *totalRowsReturned)
{
	HeapTuple *results;
	double returnedRows;

	double totRows;			//total in relation (unimportant here)
	double totDeadRows;		//total in relation (unimportant here)

	//retrieve sample data
	results = palloc(sizeof(HeapTuple) * N_SAMPLES);
	returnedRows = acquire_sample_rows(rel, DEBUG1, results, N_SAMPLES, &totRows, &totDeadRows);

	if(returnedRows < 100){
		ereport(LOG,
			(errcode(ERRCODE_INTERNAL_ERROR),
			errmsg("too few sample data to create normalization statistics")));
	}

	(*rows) = results;
	(*totalRowsReturned) = returnedRows;
}