/* 
 * ADAM - adam feature functions
 * name: adam_data_featurefunctionscmds
 * description: functions for supporting functions in ADAM
 * 
 * developed in the course of the MSc thesis at the University of Basel
 *
 * author: Ivan Giangreco
 * email: ivan.giangreco@unibas.ch
 *
 * src/backend/includes/commands/adam_data_featurefunctioncmds.h
 *
 * 
 * 
 *
 */
#ifndef ADAM_DATA_FEATURECMDS_H
#define ADAM_DATA_FEATURECMDS_H

#include "nodes/parsenodes.h"

/*
 * structs
 */
typedef struct FeatureFunctionOpt
{
	Oid fun;
	List* opts;
} FeatureFunctionOpt;

/*
 * creation
 */
extern Oid defineAdamFeatureFunction(CreateAdamFunctionStmt *stmt, const char *queryString);

/*
 * lookup
 */
extern Oid getProcIdForFeatureFunId(Oid ffunid);

extern Oid* getParameterTypesFeatureFunction(Oid procid, int *n);

/*
 * removal
 */
extern void removeFeatureFun(Oid typeOid);
extern void removeAttributeFeatureFun(Oid typeOid);

/*
 * parametrization
 */
extern bool checkAdjustParameters(Oid ffunid, List** opts, int n, bool coercable);

#endif   /* ADAM_DATA_FEATURECMDS_H */