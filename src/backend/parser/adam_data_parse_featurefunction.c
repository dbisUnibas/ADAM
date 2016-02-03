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
 * src/backend/parser/adam_data_parse_featurefunction.c
 *
 * 
 * 
 *
 */
#include "postgres.h"

#include "parser/adam_data_parse_featurefunction.h"

#include "access/htup_details.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/*
 * given a list of names (schema.objectname), and the type of the function
 * we check here whether such an object exisits, e.g. whether an algorithm
 * with the given name exists or not
 * the last attribute chooses whether we just return InvalidOid or throw
 * an error
 * it is important to note that this function returns an oid of the adam catalog and not
 * yet a proc oid
 */
Oid
getFeatureFunOidFromList(List *featureFunName,  Oid featureFunType, bool noError)
{
	Oid		candidate = FeatureFunctionGetIdWithNameList(featureFunName, featureFunType);

	if (!noError && !OidIsValid(candidate))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("%s \"%s\" does not exist", 
					 typeTypeName(typeidType(featureFunType)), 
					NameListToString(featureFunName))));

	return candidate;
}


/*
 * similar to getFeatureFunOidFromList
 */
Oid
getFeatureFunOidFromRange(RangeVar *featureFunName,  Oid featureFunType, bool noError)
{
	Oid		candidate = FeatureFunctionGetIdWithRangeVar(featureFunName, featureFunType);

	if (!noError && !OidIsValid(candidate))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("%s \"%s\" does not exist", 
					 typeTypeName(typeidType(featureFunType)), 
					featureFunName->relname)));

	return candidate;
}



