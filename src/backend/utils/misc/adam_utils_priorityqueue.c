/* 
* ADAM - priority queue
* name: adam_utils_priorityqueue
* description: functions for a very simple priority queue
* 
* developed in the course of the MSc thesis at the University of Basel
*
* author: Ivan Giangreco
* email: ivan.giangreco@unibas.ch
*
* src/backend/utils/misc/adam_utils_priorityqueue.c
*
* 
* 
*
*/
#include "postgres.h"

#include "utils/adam_utils_priorityqueue.h"
#include "fmgr.h"
#include "utils/builtins.h"

static int compare_array_elements(const void *a, const void *b, void *arg);

/* 
* Creates a priority queue with maximum size num_of_elements and a 
* comparison function fcinfo (that returns -1, 0, 1);
*/
PriorityQueue *
	createQueue(size_t numOfElements, FmgrInfo *fcinfo)
{
	PriorityQueue *q = (PriorityQueue *) palloc(sizeof(PriorityQueue) + sizeof(Datum) * numOfElements);
	
	q->ctx.flinfo = *fcinfo;
	q->maxSize = numOfElements;
	q->currentSize = 0;
	q->queue = (Datum *) ((char *) q + sizeof(PriorityQueue));

	

	return q;
}

/*
* tries to check if element can be inserted into queue or not
*/
bool
	insertIntoQueueCheck(PriorityQueue *q, Datum comparableElement)
{
	bool inserted = false;

	if(q->currentSize < q->maxSize){
		inserted = true;
	} else {
		if(DatumGetInt32(FunctionCall2(&(q->ctx.flinfo), comparableElement, PointerGetDatum(q->queue[q->maxSize - 1]))) <= 0){
			inserted = true;
		}
	}

	return inserted;
}

/* 
* inserts datum into queue if it fulfills priority requirements
* and re-sorts queue
* the function returns true if the element has been inserted into queue
*
* the second parameter is the element to compare, the thrid parameter the
* element to insert
*/
bool
	insertIntoQueue(PriorityQueue *q, Datum comparableElement, Datum insertedElement)
{
	if(q->currentSize < q->maxSize){
		q->queue[q->currentSize] = insertedElement;
		qsort_arg((void *) q->queue, q->currentSize, sizeof(Datum), compare_array_elements, (void *) &(q->ctx));
		q->currentSize++;
	} else {
		if(DatumGetInt32(
			FunctionCall2(&(q->ctx.flinfo), insertedElement, PointerGetDatum(q->queue[q->maxSize - 1]))) <= 0){
			//replace highest element
			q->queue[q->maxSize - 1] = insertedElement;
			qsort_arg((void *) q->queue, q->currentSize, sizeof(Datum), compare_array_elements, (void *) &(q->ctx));
		} 
	}

	return true;
}

/*
* qsort comparator
*/
static int
compare_array_elements(const void *a, const void *b, void *arg)
{
	Datum		da = *((const Datum *) a);
	Datum		db = *((const Datum *) b);
	SortContext	*cxt = (SortContext *) arg;
	int32		compare;

	compare = DatumGetInt32(FunctionCall2(&cxt->flinfo, da, db));
	return compare;
}

/*
* returns element at position i
*/
Datum*
	getElement(PriorityQueue *q, int i)
{
	if(i < q->currentSize){
		return &(q->queue[i]);
	}
	
	return NULL;
}

/*
* returns maximum element
*/
Datum*
	getMaximumElement(PriorityQueue *q)
{
	return &(q->queue[q->currentSize - 1]);
}