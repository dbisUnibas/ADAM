/* 
 * ADAM - feature functions
 * name: adam_data_featurefunction
 * description: system catalogs table supporting functions for adam
 * 
 * developed in the course of the MSc thesis at the University of Basel
 *
 * author: Ivan Giangreco
 * email: ivan.giangreco@unibas.ch
 *
 * src/backend/catalog/adam_data_featurefunction.c
 *
 * 
 * 
 *
 */
#include "postgres.h"

#include "catalog/adam_data_featurefunction.h"
#include "catalog/adam_data_featurefunction_fn.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/namespace.h"
#include "commands/typecmds.h"
#include "miscadmin.h"
#include "parser/scansup.h"
#include "parser/parse_type.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/* Potentially set by contrib/pg_upgrade_support functions */
Oid			binary_upgrade_next_pg_adamfun_oid = InvalidOid;


// for local use only
static void internGenerateFeatureFunDependencies(Oid newffoid, Oid ffnamespace, Oid ffnownerId, Oid ffoid,	Oid fftype,	bool rebuild);

Oid
internCreateFeatureFunction(Oid newffoid,
		   const char *ffname,
		   Oid ffnamespace,	/* namespace that owns adamfun */
		   Oid ffnownerId,	/* owner that owns adamfun */
		   Oid ffoid,		/* function oid to be called */
		   Oid fftype		/* type of function */
		  )
{
	Relation	pg_adamfun_desc;
	Oid			funoid;
	bool		rebuildDeps = false;
	HeapTuple	tup;
	bool		nulls[Natts_adam_featurefun];
	bool		replaces[Natts_adam_featurefun];
	Datum		values[Natts_adam_featurefun];
	NameData	name;
	int			i;

	// initialize arrays
	for (i = 0; i < Natts_adam_featurefun; ++i)
	{
		nulls[i] = false;
		replaces[i] = true;
		values[i] = (Datum) 0;
	}

	// insert data values
	namestrcpy(&name, ffname);
	values[Anum_adam_featurefun_fname - 1] = NameGetDatum(&name);
	values[Anum_adam_featurefun_fnamespace - 1] = ObjectIdGetDatum(ffnamespace);
	values[Anum_adam_featurefun_fowner - 1] = ObjectIdGetDatum(ffnownerId);

	values[Anum_adam_featurefun_foid - 1] = ObjectIdGetDatum(ffoid);
	values[Anum_adam_featurefun_ftype - 1] = ObjectIdGetDatum(fftype);

	// prepare to insert or update
	pg_adamfun_desc = heap_open(AdamFeatureFunRelationId, RowExclusiveLock);

	tup = SearchSysCacheCopy3(FEATUREFUNTYPENAME,
							  CStringGetDatum(ffname),
							  fftype,
							  ObjectIdGetDatum(ffnamespace));
	
	if (!OidIsValid(newffoid) && HeapTupleIsValid(tup)) {
		ereport(ERROR,
			(errcode(ERRCODE_DUPLICATE_OBJECT),
			 errmsg("%s \"%s\" already exists", 
			 					 typeTypeName(typeidType(fftype)), 
								 ffname)));
	} else {
		if(OidIsValid(newffoid) && HeapTupleGetOid(tup) == newffoid){
			tup = heap_modify_tuple(tup,
				  RelationGetDescr(pg_adamfun_desc), values, nulls, replaces);
			simple_heap_update(pg_adamfun_desc, &tup->t_self, tup);
			funoid = newffoid;
		} else {
			tup = heap_form_tuple(RelationGetDescr(pg_adamfun_desc), values, nulls);
			// force it of caller
			if (OidIsValid(newffoid))
				HeapTupleSetOid(tup, newffoid);
		
			//allow system to attach own  oid
			funoid = simple_heap_insert(pg_adamfun_desc, tup);
		}
	}

	// update indexes
	CatalogUpdateIndexes(pg_adamfun_desc, tup);

	// create dependencies (skip at bootstrap)
	if (!IsBootstrapProcessingMode())
		internGenerateFeatureFunDependencies(funoid, ffnamespace, ffnownerId, ffoid, fftype, rebuildDeps);

	// finishing
	heap_close(pg_adamfun_desc, RowExclusiveLock);

	return funoid;
}

/*
 * generate dependencies for feature function created, e.g. dependance of an attribute on a feature function, etc.
 *
 */
static void
internGenerateFeatureFunDependencies(Oid newffoid,
						Oid ffnamespace,	/* namespace that owns adamfun */
						Oid ffnownerId,		/* owner of adamfun */
						Oid ffoid,			/* function oid to be called */
						Oid fftype,			/* type of function */
						bool rebuild
		   )
{
	ObjectAddress myself,
				referenced;

	// on rebuild flush first old dependencies
	if (rebuild)
	{
		deleteDependencyRecordsFor(AdamFeatureFunRelationId, newffoid, true);
		deleteSharedDependencyRecordsFor(AdamFeatureFunRelationId, newffoid, 0);
	}

	myself.classId = AdamFeatureFunRelationId;
	myself.objectId = newffoid;
	myself.objectSubId = 0;

	// dependecies on namespace, etc.
	referenced.classId = NamespaceRelationId;
	referenced.objectId = ffnamespace;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	recordDependencyOnOwner(AdamFeatureFunRelationId, newffoid, ffnownerId);

	recordDependencyOnCurrentExtension(&myself, rebuild);

	// dependency on IO function
	if (OidIsValid(ffoid))
	{
		referenced.classId = ProcedureRelationId;
		referenced.objectId = ffoid;
		referenced.objectSubId = 0;
		recordDependencyOn(&referenced, &myself, DEPENDENCY_INTERNAL);
	}
}



/*
 * converts feature oid to a formatted string  returning the name of the function
 */
char *
formatFeatureFun(Oid ffoid)
{
	char	   *result;
	HeapTuple	ffuntup, proctup;
	Form_adam_featurefun ffform;
	Form_pg_proc procform;
	char	   *ffunname;
	int			nargs;
	int			i;
	char	   *nspname;
	StringInfoData buf;

	ffuntup =  SearchSysCache1(FEATUREFUNOID, ObjectIdGetDatum(ffoid));
	
	if (!HeapTupleIsValid(ffuntup)){
		result = (char *) palloc(NAMEDATALEN);
		snprintf(result, NAMEDATALEN, "%u", ffoid);
		return result;
	}

	ffform = (Form_adam_featurefun) GETSTRUCT(ffuntup);

	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(ffform->adamfoid));

	
	if (!HeapTupleIsValid(proctup)){
		/* If OID doesn't match any pg_proc entry, return it numerically */
		result = (char *) palloc(NAMEDATALEN);
		snprintf(result, NAMEDATALEN, "%u", ffoid);
	}

	procform = (Form_pg_proc) GETSTRUCT(proctup);
	ffunname = NameStr(ffform->adamfname);
	nargs = procform->pronargs;

	initStringInfo(&buf);

	if (FunctionIsVisible(ffform->adamfoid))
		nspname = NULL;
	else
		nspname = get_namespace_name(procform->pronamespace);

	appendStringInfo(&buf, "%s(",
						 quote_qualified_identifier(nspname, ffunname));
	for (i = 0; i < nargs; i++){
		Oid			thisargtype = procform->proargtypes.values[i];

		if (i > 0)
			appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, format_type_be(thisargtype));
	}
	appendStringInfoChar(&buf, ')');

	result = buf.data;

	ReleaseSysCache(proctup);

	return result;
}

/*
 * converts feature oid to a formatted string returning the type of the function
 */
char *
formatFeatureFunType(Oid ffoid)
{
	char	   *result;
	HeapTuple	ffuntup;
	Form_adam_featurefun ffform;

	ffuntup =  SearchSysCache1(FEATUREFUNOID, ObjectIdGetDatum(ffoid));

	if (!HeapTupleIsValid(ffuntup)){
		result = (char *) palloc(NAMEDATALEN);
		snprintf(result, NAMEDATALEN, "%u", ffoid);
		return result;
	}

	ffform = (Form_adam_featurefun) GETSTRUCT(ffuntup);


	return typeTypeName(typeidType(ffform->adamftype));
}