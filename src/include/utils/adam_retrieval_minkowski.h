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
 * src/include/utils/adam_retrieval_minkowski.h
 *
 * 
 * 
 *
 */
#ifndef ADAM_RETRIEVAL_MINKOWKSI_H
#define ADAM_RETRIEVAL_MINKOWKSI_H

#include "fmgr.h"
#include "nodes/nodes.h"

#define MINKOWSKI_MAX_NORM  -1

typedef double MinkowskiNorm;

extern Datum calculateMinkowski(PG_FUNCTION_ARGS);
extern Datum calculateWeightedMinkowski(PG_FUNCTION_ARGS);

extern MinkowskiNorm getMinkowskiNormFromInput(Node *val);

#endif   /* ADAM_RETRIEVAL_MINKOWKSI_H */

