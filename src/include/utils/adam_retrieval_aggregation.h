/* 
* ADAM - retrieval aggregation functions
* name: adam_retrieval_aggregation
* description: functions for calculating the aggregations
* 
* developed in the course of the MSc thesis at the University of Basel
*
* author: Ivan Giangreco
* email: ivan.giangreco@unibas.ch
*
* src/include/utils/adam_retrieval_aggregation.ch
*
* 
* 
*
*/
#ifndef ADAM_RETRIEVAL_AGGREGATION_H
#define ADAM_RETRIEVAL_AGGREGATION_H

#include "fmgr.h"

extern Datum standard_union_sfunc(PG_FUNCTION_ARGS);
extern Datum standard_union_final(PG_FUNCTION_ARGS);
extern Datum algebraic_union_sfunc(PG_FUNCTION_ARGS);
extern Datum algebraic_union_final(PG_FUNCTION_ARGS);
extern Datum bounded_union_sfunc(PG_FUNCTION_ARGS);
extern Datum bounded_union_final(PG_FUNCTION_ARGS);
extern Datum drastic_union_sfunc(PG_FUNCTION_ARGS);
extern Datum drastic_union_final(PG_FUNCTION_ARGS);
extern Datum standard_intersect_sfunc(PG_FUNCTION_ARGS);
extern Datum standard_intersect_final(PG_FUNCTION_ARGS);
extern Datum algebraic_intersect_sfunc(PG_FUNCTION_ARGS);
extern Datum algebraic_intersect_final(PG_FUNCTION_ARGS);
extern Datum bounded_intersect_sfunc(PG_FUNCTION_ARGS);
extern Datum bounded_intersect_final(PG_FUNCTION_ARGS);
extern Datum drastic_intersect_sfunc(PG_FUNCTION_ARGS);
extern Datum drastic_intersect_final(PG_FUNCTION_ARGS);
extern Datum standard_except(PG_FUNCTION_ARGS);
extern Datum sugeno_except(PG_FUNCTION_ARGS);
extern Datum yager_except(PG_FUNCTION_ARGS);

#endif   /* ADAM_RETRIEVAL_AGGREGATION_H */

