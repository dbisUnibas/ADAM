/* 
 * ADAM - Min/Max Normalization
 * name: adam_retrieval_minmax
 * description: functions for calculating the Min/Max-Normalization
 * 
 * developed in the course of the MSc thesis at the University of Basel
 *
 * author: Ivan Giangreco
 * email: ivan.giangreco@unibas.ch
 *
 * src/include/utils/adam_retrieval_normalization.h
 *
 * 
 * 
 *
 */
#ifndef ADAM_RETRIEVAL_MINMAX_H
#define ADAM_RETRIEVAL_MINMAX_H

#include "parser/parse_node.h"

#define N_SAMPLES 100

extern Datum normalizeMinMax(PG_FUNCTION_ARGS);
extern Datum normalizeGaussian(PG_FUNCTION_ARGS);

extern void adjustAdamNormalizationPrecomputeStmt(AdamNormalizationPrecomputeStmt *stmt);

extern Datum* getNormalizationStatistics(Oid relid, char* colname, Oid distanceProcid, List*arguments, bool noError);

#endif   /* ADAM_RETRIEVAL_MINMAX_H */

