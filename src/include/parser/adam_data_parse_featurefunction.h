/* 
 * ADAM - feature functions parser
 * name: adam_data_parse_featurefunction
 * description: supports parsing feature functions
 * 
 * developed in the course of the MSc thesis at the University of Basel
 *
 * author: Ivan Giangreco
 * email: ivan.giangreco@unibas.ch
 *
 * src/includes/catalog/adam_data_parse_featurefunction.h
 *
 * 
 * 
 *
 */

#ifndef ADAM_DATA_PARSE_FEATUREFUNCTION_H
#define ADAM_DATA_PARSE_FEATUREFUNCTION_H

#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "parser/parse_node.h"

/*
 * lookup
 */
extern Oid getFeatureFunOidFromList(List *featurefunname, Oid featurefuntype, bool noError);
extern Oid getFeatureFunOidFromRange(RangeVar *featureFunName,  Oid featureFunType, bool noError);

#define getAlgorithmOidFromList(featurefunname, noError) getFeatureFunOidFromList(featurefunname, ALGORITHMOID, noError)
#define getDistanceOidFromList(featurefunname, noError) getFeatureFunOidFromList(featurefunname, DISTANCEOID, noError)
#define getNormalizationOidFromList(featurefunname, noError) getFeatureFunOidFromList(featurefunname, NORMALIZATIONOID, noError)

#define getAlgorithmOidFromRange(featurefunname, noError) getFeatureFunOidFromRange(featurefunname, ALGORITHMOID, noError)
#define getDistanceOidFromRange(featurefunname, noError) getFeatureFunOidFromRange(featurefunname, DISTANCEOID, noError)
#define getNormalizationOidFromRange(featurefunname, noError) getFeatureFunOidFromRange(featurefunname, NORMALIZATIONOID, noError)


#endif   /* ADAM_DATA_PARSE_FEATUREFUNCTION_H */
