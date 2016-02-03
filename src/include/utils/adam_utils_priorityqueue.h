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
 * src/include/utils/adam_utils_priorityqueue.h
 *
 * 
 * 
 *
 */
#ifndef ADAM_UTILS_PRIORITYQUEUE_H
#define ADAM_UTILS_PRIORITYQUEUE_H

#include "fmgr.h"


typedef struct SortContext
{
	FmgrInfo	flinfo;
} SortContext;

typedef struct PriorityQueue 
{
	size_t			maxSize;
	size_t			currentSize;
	SortContext		ctx;
	Datum			*queue;
} PriorityQueue;

extern PriorityQueue* createQueue(size_t numOfElements, FmgrInfo *fcinfo);

extern bool insertIntoQueue(PriorityQueue *q, Datum comparableElement, Datum insertedElement);
extern bool insertIntoQueueCheck(PriorityQueue *q, Datum comparableElement);

extern Datum* getElement(PriorityQueue *q, int i);
extern Datum* getMaximumElement(PriorityQueue *q);

#endif   /* ADAM_UTILS_PRIORITYQUEUE_H */

