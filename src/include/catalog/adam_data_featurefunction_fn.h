/* 
 * ADAM - feature functions
 * name: adam_data_featurefunction_fn
 * description: system catalogs table supporting functions for adam
 * 
 * developed in the course of the MSc thesis at the University of Basel
 *
 * author: Ivan Giangreco
 * email: ivan.giangreco@unibas.ch
 *
 * src/backend/includes/catalog/adam_data_featurefunction_fn.h
 *
 * 
 * 
 *
 */
#ifndef ADAM_DATA_FEATUREFUNCTION_FN_H
#define ADAM_DATA_FEATUREFUNCTION_FN_H

#include "nodes/nodes.h"

/*
 * tuple creation
 */
extern Oid internCreateFeatureFunction(Oid newffoid, const char *ffname,  Oid ffnamespace,	Oid ffnownerId,	Oid ffoid, Oid fftype);

/*
 * string formatting
 */
extern char * formatFeatureFun(Oid ffoid);
extern char * formatFeatureFunType(Oid ffoid);
extern char * formatFeatureColName(Oid ffoid);

#endif   /* ADAM_DATA_FEATUREFUNCTION_FN_H */