/* 
* ADAM - index marks
* name: adam_index_marks
* description: functions to establish the marks for VA approximation
* 
* developed in the course of the MSc thesis at the University of Basel
*
* author: Ivan Giangreco
* email: ivan.giangreco@unibas.ch
*
* src/backend/utils/adt/adam_index_marks.c
*
* 
* 
*
*/
#include "postgres.h"

#include "utils/adam_index_marks.h"

#include "utils/adam_data_feature.h"

#include <math.h>
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/index.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "commands/vacuum.h"
#include "nodes/execnodes.h"
#include "parser/parse_node.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"


#define MIN(X, Y) ((X) < (Y) ? (X) : (Y))

//equidistant funcs
static Datum equiDistantMarks(Relation rel, IndexInfo *indexInfo);
static void getEquidistantMarks(int dimensions, feature *min, feature *max, Datum **marks);

//equifrequent funcs
static Datum equiFrequentMarks(Relation rel, IndexInfo *indexInfo);
static void getEquifrequentMarks(int numRowsUsed, int dimensions, feature *min, feature *max, int *frequencies, Datum **marks);
static void getFrequencies(HeapTuple *rows, double numRows,  TupleTableSlot *slot, IndexInfo *indexInfo, List *predicate, EState *estate, 
	ExprContext *econtext, int dimensions, 	feature *min, feature *max,	int **frequencies, int* data_ctr);

//min-max-funcs
static void getMinMax(HeapTuple *rows, double numRows, TupleTableSlot *slot, IndexInfo *indexInfo, List *predicate, EState *estate, ExprContext *econtext,
	feature **min, feature **max, int *data_ctr);
static Datum getMinMaxResult(FunctionCallInfoData *infoData, Datum trans_result, bool firstRun, feature *f);

static void getSampledRows(Relation rel, HeapTuple **rows, double *totalRowsReturned);

/*
* calculates the marks given a relation and a indexInfo struct;
* the choice which strategy is chosen to calculate the marks depends on the entry in ii_MarksStrategy
* (see parsenodes.h for all options)
*/
Datum
	calculateMarks(Relation rel, IndexInfo *indexInfo)
{
	switch(indexInfo->ii_MarksStrategy){
	case VA_MARKS_EQUIDISTANT:
		return equiDistantMarks(rel, indexInfo);
	case VA_MARKS_EQUIFREQUENT:
	default:
		return equiFrequentMarks(rel, indexInfo);
	}

	return BoolGetDatum(false);
}

/*
* even distribution of marks
* this only works for uniformly distributed data! with skewed data, a great portion of the data points will
* fall into the same slice; hence, a lot of points will have the same approximation and, thus, the filtering
* will not be as efficient
*
* (Weber, 2000, Section 5.2.2)
*/
static Datum 
	equiDistantMarks(Relation rel, IndexInfo *indexInfo)
{
	TupleTableSlot *slot;
	EState *estate;
	ExprContext *econtext;
	List       *predicate;

	int dimensions;

	feature *min, *max;
	Datum *marks;

	ArrayType *arr_marks;

	HeapTuple *rows;
	double totalRows;
	int data_ctr;

	int		arr_dims[2] = {-1, MAX_MARKS};
	int		arr_lbs[2]  = {0, 0};
    
    int dim1;
    int dim2;

	MemoryContext old_ctx;
	MemoryContext ctx;

	//switch memory context
	ctx = AllocSetContextCreate(CurrentMemoryContext, "Marks build temporary context",
		ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
	old_ctx = MemoryContextSwitchTo(ctx);

	slot = MakeSingleTupleTableSlot(RelationGetDescr(rel));

	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	econtext->ecxt_scantuple = slot;
	predicate = (List *) ExecPrepareExpr((Expr *) indexInfo->ii_Predicate, estate);

	getSampledRows(rel, &rows, &totalRows);

	//get min/max values for each dimension
	getMinMax(rows, totalRows, slot, indexInfo, predicate, estate, econtext, &min, &max, &data_ctr);

	//now we know what the minimum number of dimensions is
    dim1 = ArrayGetNItems(ARR_NDIM(&min->data), ARR_DIMS(&min->data));
    dim2 = ArrayGetNItems(ARR_NDIM(&max->data), ARR_DIMS(&max->data));

	dimensions = MIN(dim1, dim2);
	arr_dims[0] = dimensions;

	//using the min/max values get the marks
	getEquidistantMarks(dimensions, min, max, &marks);

	//these may have been pointing to the now-gone estate
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NIL;
	
	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);
	MemoryContextSwitchTo(old_ctx);

	//create array out of Datum*
	arr_marks = construct_md_array(marks, NULL, 2, arr_dims, arr_lbs, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');

	MemoryContextResetAndDeleteChildren(ctx);
	MemoryContextDelete(ctx);

	PG_RETURN_ARRAYTYPE_P(arr_marks);
}


static void 
	getEquidistantMarks(int dimensions, feature *min, feature *max, Datum **marks)
{
	ArrayIterator min_it = array_create_iterator(&min->data, 0);
	Datum		min_val;
	bool		min_isnull;

	ArrayIterator max_it = array_create_iterator(&max->data, 0);
	Datum		max_val;
	bool		max_isnull;

	Datum		scale_precision_dat;
	Datum		truncate_dat;

	int			dim_ctr = 0;
	int			mark_ctr = 0;

	Datum max_sub_min;
	Datum mark_ctr_dat;
	Datum partitions_dat = DirectFunctionCall1(i4tod, Int32GetDatum(MAX_PARTITIONS));

	Datum *result;
	
	scale_precision_dat = Int32GetDatum(NUM_SCALE_PRECISION);
	truncate_dat = Int32GetDatum(NUM_TRUNCATE);
    
	result = palloc(sizeof(Datum) * dimensions * MAX_MARKS);
	
	while(array_iterate(min_it, &min_val, &min_isnull) && array_iterate(max_it, &max_val, &max_isnull)){
		result[dim_ctr * MAX_MARKS + 0] = min_val;
		result[dim_ctr * MAX_MARKS + MAX_PARTITIONS] = max_val;

		max_sub_min = DirectFunctionCall2(float8mi, max_val, min_val);

		for(mark_ctr = 1; mark_ctr < MAX_PARTITIONS; mark_ctr++){
			mark_ctr_dat = DirectFunctionCall1(i4tod, Int32GetDatum(mark_ctr));

			result[dim_ctr * MAX_MARKS + mark_ctr] = 
				DirectFunctionCall2(float8pl,
				min_val,
				DirectFunctionCall2(float8mul,
				max_sub_min,
				DirectFunctionCall2(float8div, mark_ctr_dat, partitions_dat)));
		}

		dim_ctr++;
	}

	array_free_iterator(min_it);
	array_free_iterator(max_it);

	*marks = result;
}





/*
* distribution aware marks
* partitioning poitns are chosen such that each slice contains about the same number of points
*
* (Weber, 2000, Section 5.2.2)
*/
static Datum 
	equiFrequentMarks(Relation rel, IndexInfo *indexInfo)
{
	TupleTableSlot *slot;
	EState *estate;
	ExprContext *econtext;
	List	   *predicate;

	int dimensions;

	feature *min, *max;
	int *frequencies;
	Datum *marks;

	ArrayType *arr_marks;
		
	HeapTuple *rows;
	double totalRows = 0;
	int data_ctr1 = 0;
	int data_ctr2 = 0;

	int		arr_dims[2] = {-1, MAX_MARKS};
	int		arr_lbs[2] = {0, 0};
    
    int     dim1;
    int     dim2;

	MemoryContext old_ctx;
	MemoryContext ctx;

	//switch memory context
	ctx = AllocSetContextCreate(CurrentMemoryContext, "Marks build temporary context",
		ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
	old_ctx = MemoryContextSwitchTo(ctx);

	slot = MakeSingleTupleTableSlot(RelationGetDescr(rel));

	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	econtext->ecxt_scantuple = slot;

	predicate = (List *) ExecPrepareExpr((Expr *) indexInfo->ii_Predicate, estate);

	//retrieve sample data
	getSampledRows(rel, &rows, &totalRows);

	//get min/max values for each dimension
	getMinMax(rows, totalRows, slot, indexInfo, predicate, estate, econtext, &min, &max, &data_ctr1);

	//now we know what the minimum number of dimensions is
    dim1 = ArrayGetNItems(ARR_NDIM(&min->data), ARR_DIMS(&min->data));
    dim2 = ArrayGetNItems(ARR_NDIM(&max->data), ARR_DIMS(&max->data));
	dimensions = MIN(dim1, dim2);
	arr_dims[0] = dimensions;

	//calculate the frequencies using the min and max values
	getFrequencies(rows, totalRows, slot, indexInfo, predicate, estate, econtext, dimensions, min, max, &frequencies, &data_ctr2);
	
	//using the frequencies and min/max values get the marks
	getEquifrequentMarks(data_ctr1, dimensions, min, max, frequencies, &marks);

	//these may have been pointing to the now-gone estate
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NIL;

	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);
	MemoryContextSwitchTo(old_ctx);
		
	//create array out of Datum*
	arr_marks = construct_md_array(marks, NULL, 2, arr_dims, arr_lbs, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');

	MemoryContextDelete(ctx);

	PG_RETURN_ARRAYTYPE_P(arr_marks);
}


/*
* returns the frequency array
*/
static void 
	getFrequencies(HeapTuple *rows, double numRows, 
	TupleTableSlot *slot, IndexInfo *indexInfo, List *predicate, EState *estate, ExprContext *econtext, 
	int dimensions, feature *min, feature *max,	int **frequencies, int *data_ctr)
{
	Datum			f_value;
	bool			f_isnull;

	ArrayIterator	min_it;
	Datum			min_val;
	bool			min_isnull;

	ArrayIterator	max_it;
	Datum			max_val;
	bool			max_isnull;

	ArrayIterator	dim_it;
	Datum			dim_val;
	bool			dim_isnull;

	int				dim_ctr = 0;
	int				cell = 0;
	Datum			cell_dat;

	int				i = 0;

	Datum nfreq_dat = DirectFunctionCall1(i4tod, Int32GetDatum(SAMPLING_FREQUENCY));

	int *result = palloc(sizeof(int) * dimensions * SAMPLING_FREQUENCY);
	memset(result, 0, sizeof(int) * dimensions * SAMPLING_FREQUENCY);

	/* get samples that are used for equi frequent partitioning */
	for(i = 0; i < numRows; i++){
		dim_ctr = 0;

		min_it = array_create_iterator(&min->data, 0);
		max_it = array_create_iterator(&max->data, 0);

		ExecStoreTuple(rows[i], slot, InvalidBuffer, false);

		if (predicate != NIL){
			if (!ExecQual(predicate, econtext, false))
				continue;
		}

		FormIndexDatum(indexInfo, slot,  estate, &f_value,  &f_isnull);

		if(!f_isnull){
			feature *f = (feature *) DatumGetPointer(PG_DETOAST_DATUM(f_value));
			dim_it = array_create_iterator(&f->data, 0);	

			while(array_iterate(min_it, &min_val, &min_isnull) 
				&& array_iterate(max_it, &max_val, &max_isnull) 
				&& array_iterate(dim_it, &dim_val, &dim_isnull)){

				if (isnan(DatumGetFloat8(max_val)) || isnan(DatumGetFloat8(min_val))){
					ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("vector contains NaN")));
				}

				if (DatumGetFloat8(max_val) - DatumGetFloat8(min_val) == 0){
					cell = 0;
				} else {
					cell_dat = DirectFunctionCall2(float8mul,
						nfreq_dat,
						DirectFunctionCall2(float8div,
						DirectFunctionCall2(float8mi, dim_val, min_val),
						DirectFunctionCall2(float8mi, max_val, min_val)));

					cell = DatumGetInt32(DirectFunctionCall1(dtoi4, cell_dat));
				}

                    

					if(cell <   0)					cell = 0;
					if(cell >= SAMPLING_FREQUENCY)  cell = SAMPLING_FREQUENCY - 1;

					result[dim_ctr * SAMPLING_FREQUENCY + cell]++; 
					
					dim_ctr++;
			}

			
			(*data_ctr)++;
		}
	}

	array_free_iterator(min_it);
	array_free_iterator(max_it);
	array_free_iterator(dim_it);

	*frequencies = result;
}

/*
* given the frequency vector, the min and max values, it calculates the marks
*/
static void 
	getEquifrequentMarks(int numRowsUsed, int dimensions, feature *min, feature *max, int *frequencies, Datum **marks)
{
	ArrayIterator min_it = array_create_iterator(&min->data, 0);
	Datum		min_val;
	bool		min_isnull;

	ArrayIterator max_it = array_create_iterator(&max->data, 0);
	Datum		max_val;
	bool		max_isnull;

	int dim_ctr = 0;
	int mark_ctr = 0;

	int sum = 0;

	int k = 0;
	Datum k_dat;

	Datum nfreq_dat;
	
	Datum truncate_dat;
	Datum scale_precision_dat;
	
	Datum *result;

	scale_precision_dat = Int32GetDatum(NUM_SCALE_PRECISION);
	truncate_dat = Int32GetDatum(NUM_TRUNCATE);

	nfreq_dat = DirectFunctionCall1(i4tod, Int32GetDatum(SAMPLING_FREQUENCY));
	result = palloc(sizeof(Datum) * dimensions * (MAX_PARTITIONS + 1));

	while(array_iterate(min_it, &min_val, &min_isnull) && array_iterate(max_it, &max_val, &max_isnull)){
		sum = 0;
		k = 0;
		result[dim_ctr * MAX_MARKS + 0] = min_val;
		result[dim_ctr * MAX_MARKS + MAX_PARTITIONS] = max_val;
		for(mark_ctr = 1; mark_ctr < MAX_PARTITIONS; mark_ctr++){
			int n = (mark_ctr * numRowsUsed) / MAX_PARTITIONS;
			
			while(sum < n){
				sum = sum + frequencies[dim_ctr * SAMPLING_FREQUENCY + k];
				k++;
			}

			k_dat = DirectFunctionCall1(i4tod, Int32GetDatum(k));
			result[dim_ctr * MAX_MARKS + mark_ctr] = 
				DirectFunctionCall2(float8pl,
				min_val,
				DirectFunctionCall2(float8mul,
				k_dat,
				DirectFunctionCall2(float8div,
				DirectFunctionCall2(float8mi, max_val, min_val),
				nfreq_dat)));
        }
		dim_ctr++;
	}
	array_free_iterator(min_it);
	array_free_iterator(max_it);

	*marks = result;
}


/*
* returns the minimum value and the maximum value for each dimension of the feature vector
* see MIN/MAX operation of feature
*/
static void
	getMinMax(HeapTuple *rows, double numRows, 
	TupleTableSlot *slot, IndexInfo *indexInfo, List *predicate, EState *estate, ExprContext *econtext,
	feature **min, feature **max, int *data_ctr)
{
	FmgrInfo minFunc;
	FmgrInfo maxFunc;

	FunctionCallInfoData minInfoData;
	FunctionCallInfoData maxInfoData;

	Datum min_result = PointerGetDatum(NULL);
	Datum max_result = PointerGetDatum(NULL);

	bool firstRun = true;

	Datum		f_value;
	bool		f_isnull;

	int i = 0;

	fmgr_info(FEATURE_MIN, &minFunc);
	fmgr_info(FEATURE_MAX, &maxFunc);

	InitFunctionCallInfoData(minInfoData, &minFunc, 2, InvalidOid, NULL, NULL);
	InitFunctionCallInfoData(maxInfoData, &maxFunc, 2, InvalidOid, NULL, NULL);

	for(i = 0; i < numRows; i++){
		HeapTuple	heapTuple = rows[i];

		if(!heapTuple){
			continue;
		}

		ResetExprContext(econtext);
		ExecStoreTuple(heapTuple, slot, InvalidBuffer, false);

		if (predicate != NIL){
			if (!ExecQual(predicate, econtext, false))
				continue;
		}

		FormIndexDatum(indexInfo, slot, estate, &f_value, &f_isnull);

		if(!f_isnull){
			feature *f = (feature *) DatumGetPointer(PG_DETOAST_DATUM(f_value));

			min_result = getMinMaxResult(&minInfoData, min_result, firstRun, f);
			max_result = getMinMaxResult(&maxInfoData, max_result, firstRun, f);

			(*data_ctr)++;

			firstRun = false;
		}
	}

	if(!firstRun){
		min_result = DirectFunctionCall1(feature_minmax_end, min_result);
		max_result = DirectFunctionCall1(feature_minmax_end, max_result);
	} else {
		ereport(ERROR, (errmsg("not enough sample data for VA indexing available")));
	}

	*min = (feature *) DatumGetPointer(min_result);
	*max = (feature *) DatumGetPointer(max_result);
}


/*
* fills the min_max function call array correctly and does function call;
* returns result of function call
*/
static Datum 
	getMinMaxResult(FunctionCallInfoData *infoData, Datum trans_result, bool firstRun, feature *f)
{
	infoData->arg[0] = trans_result;
	infoData->arg[1] = PointerGetDatum(f);

	if(firstRun){
		infoData->argnull[0] = true;
	} else {
		infoData->argnull[0] = false;
	}

	infoData->argnull[1] = false;
	infoData->isnull = false;

	return FunctionCallInvoke(&*infoData);
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
	
	if(returnedRows < 256){
		ereport(ERROR,
		(errcode(ERRCODE_INTERNAL_ERROR),
		errmsg("too few sample data to create marks")));
	}

	(*rows) = results;
	(*totalRowsReturned) = returnedRows;
}


	