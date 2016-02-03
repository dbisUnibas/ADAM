/* 
 * ADAM - feature type
 * name: adam_data_feature
 * description: functions defining the feature datatype in ADAM
 * 
 * developed in the course of the MSc thesis at the University of Basel
 *
 * author: Ivan Giangreco
 * email: ivan.giangreco@unibas.ch
 *
 * src/include/utils/adam_data_feature.h
 *
 * 
 * 
 *
 */
#ifndef ADAM_DATA_FEATURE_H
#define ADAM_DATA_FEATURE_H

#include "utils/array.h"

/*
 * data structures
 */
typedef struct feature {
	int32		vl_len_;
	int32		typid;	   /* oid of type of data (in non-array-form) */
	ArrayType   data;      /* actual data */
} feature;

/*
 * I/O functions
 */
extern Datum feature_in(PG_FUNCTION_ARGS);
extern Datum feature_out(PG_FUNCTION_ARGS);
extern Datum featureFromArray(FmgrInfo *fmgr, Datum d, Oid typioparam, int32 typmod);

/*
 * data casting
 */
extern Datum featureArrayCast(PG_FUNCTION_ARGS);
extern Datum arrayFeatureCast(PG_FUNCTION_ARGS);

/*
 * data display
 */
extern char * getFeatureName(int32 typemod);


/*
 * operators
 */
extern Datum feature_eq(PG_FUNCTION_ARGS);
extern Datum feature_neq(PG_FUNCTION_ARGS);
extern Datum feature_hash(PG_FUNCTION_ARGS);

extern Datum feature_lt(PG_FUNCTION_ARGS);
extern Datum feature_gt(PG_FUNCTION_ARGS);
extern Datum feature_le(PG_FUNCTION_ARGS);
extern Datum feature_ge(PG_FUNCTION_ARGS);

extern Datum feature_cmp(PG_FUNCTION_ARGS);

extern Datum feature_dummy_eq(PG_FUNCTION_ARGS);

extern Datum feature_min(PG_FUNCTION_ARGS);
extern Datum feature_max(PG_FUNCTION_ARGS);
extern Datum feature_minmax_end(PG_FUNCTION_ARGS);

/*
 * adam validation for options
 */
extern void adamValidateOption (Oid value);
#define featureValidateAlgorithmOption adamValidateOption
#define featureValidateNormalizationOption adamValidateOption
#define featureValidateDistanceOption adamValidateOption
#define featureValidateIndexOption adamValidateOption

#endif   /* ADAM_DATA_FEATURE_H */

