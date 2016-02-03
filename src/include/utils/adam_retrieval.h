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
 * src/include/utils/adam_retrieval.h
 *
 * 
 * 
 *
 */
#ifndef ADAM_RETRIEVAL_H
#define ADAM_RETRIEVAL_H

#include "commands/adam_data_featurefunctioncmds.h"
#include "utils/adam_retrieval_minkowski.h"

#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "parser/parse_node.h"

/*
 * operators
 */
extern Datum dummyFeatureDistance(PG_FUNCTION_ARGS);

/*
 * analyze
 */
extern Node* adjustParseTreeForFeatureSearch(ParseState *pstate, AdamSelectStmt *adamSelectStmt, Node **adamQueryClause, int location);

/*
 * specialized functions for feature functions
 */
extern Oid getDistanceProcId(AdamFunctionOptionsStmt *distanceOp, FieldSelect *ltree, FeatureFunctionOpt **distanceOptions, MinkowskiNorm* nn_minkowski);

/*
 * utilities
 */
extern void fieldSelectGetAttribute(FieldSelect *field, Oid *relid, char** attribute, int *fieldNum);
extern char* getDistanceFieldName();

#endif   /* ADAM_RETRIEVAL_H */

