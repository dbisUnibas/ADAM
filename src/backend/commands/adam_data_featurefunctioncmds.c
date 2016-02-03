/* 
* ADAM - feature function commands
* name: adam_data_featurefunctioncmds
* description: functions for supporting features in ADAM, e.g. algorithm, distance, index, normalization
* 
* developed in the course of the MSc thesis at the University of Basel
*
* author: Ivan Giangreco
* email: ivan.giangreco@unibas.ch
*
* src/backend/commands/adam_data_featurefunctioncmds.c
*
* 
* 
*
*/
#include "postgres.h"

#include "commands/adam_data_featurefunctioncmds.h"

#include "catalog/adam_data_featurefunction.h"
#include "catalog/adam_data_featurefunction_fn.h"

#include "commands/defrem.h"
#include "nodes/nodeFuncs.h"
#include "parser/parser.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/*
 * given a CreateAdamFunctionStmt we create an adam function
 * this includes creating a true procedure (in pg_proc) plus
 * creating an entry in the adam feature function catalog
 */
Oid
	defineAdamFeatureFunction(CreateAdamFunctionStmt *stmt, const char *queryString)
{
	Oid			fftype;
	Oid			procoid;

	char	   *probin_str;
	char	   *prosrc_str;
	Oid			prorettype;
	bool		returnsSet;
	char	   *language;
	Oid			languageOid;
	Oid			languageValidator;
	char	   *funcname;
	char	   *ffname;
	Oid			ffnamespace;
	AclResult	aclresult;
	oidvector  *parameterTypes;
	ArrayType  *allParameterTypes;
	ArrayType  *parameterModes;
	ArrayType  *parameterNames;
	List	   *parameterDefaults;
	Oid			requiredResultType;
	bool		isWindowFunc,
		isStrict,
		security,
		isLeakProof;
	char		volatility;
	ArrayType  *proconfig;
	float4		procost;
	float4		prorows;
	HeapTuple	languageTuple,
		checkExistsTuple;
	Form_pg_language languageStruct;
	List	   *as_clause;
	TypeName   *typname;
	Oid			ffunoid = InvalidOid;

	/* Convert list of names to a name and namespace */
	ffnamespace = QualifiedNameGetCreationNamespace(stmt->fstmt.funcname, &funcname);

	/* Check we have creation rights in target namespace */
	aclresult = pg_namespace_aclcheck(ffnamespace, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
		get_namespace_name(ffnamespace));

	/* default attributes */
	isWindowFunc = false;
	isStrict = false;
	security = false;
	isLeakProof = false;
	volatility = PROVOLATILE_VOLATILE;
	proconfig = NULL;
	procost = -1;				/* indicates not set */
	prorows = -1;				/* indicates not set */

	/* override attributes from explicit list */
	compute_attributes_sql_style(stmt->fstmt.options,
		&as_clause, &language,
		&isWindowFunc, &volatility,
		&isStrict, &security, &isLeakProof,
		&proconfig, &procost, &prorows);

	/* Look up the language and validate permissions */
	languageTuple = SearchSysCache1(LANGNAME, PointerGetDatum(language));
	if (!HeapTupleIsValid(languageTuple))
		ereport(ERROR,
		(errcode(ERRCODE_UNDEFINED_OBJECT),
		errmsg("language \"%s\" does not exist", language),
		(PLTemplateExists(language) ?
		errhint("Use CREATE LANGUAGE to load the language into the database.") : 0)));

	languageOid = HeapTupleGetOid(languageTuple);
	languageStruct = (Form_pg_language) GETSTRUCT(languageTuple);

	if (languageStruct->lanpltrusted){
		/* if trusted language, need USAGE privilege */
		AclResult	aclresult;

		aclresult = pg_language_aclcheck(languageOid, GetUserId(), ACL_USAGE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_LANGUAGE,
			NameStr(languageStruct->lanname));
	} else {
		/* if untrusted language, must be superuser */
		if (!superuser())
			aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_LANGUAGE,
			NameStr(languageStruct->lanname));
	}

	languageValidator = languageStruct->lanvalidator;

	ReleaseSysCache(languageTuple);

	/*
	* Only superuser is allowed to create leakproof functions because it
	* possibly allows unprivileged users to reference invisible tuples to be
	* filtered out using views for row-level security.
	*/
	if (isLeakProof && !superuser())
		ereport(ERROR,
		(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
		errmsg("only superuser can define a leakproof function")));

	
	fftype = typenameTypeId(NULL, stmt->funtype);

	// if the function type is distance, then add an additional input parameter
	// specifying the current maximum distance
	if(fftype == DISTANCEOID){
	/*	FunctionParameter *param = makeNode(FunctionParameter);
		A_Const *const_def = makeNode(A_Const);

		const_def->val.type = T_Integer;
		const_def->val.val.ival = 0;
		const_def->location = -1;

		param->argType = SystemTypeName("numeric");
		param->mode = FUNC_PARAM_IN;
		param->name = pstrdup("current_max_distance");
		param->defexpr = (Node *) const_def;
		lappend(stmt->fstmt.parameters, param);*/
	}

	/*
	* Convert remaining parameters of CREATE to form wanted by
	* ProcedureCreate.
	*/
	examine_parameter_list(stmt->fstmt.parameters, languageOid, queryString,
		&parameterTypes,
		&allParameterTypes,
		&parameterModes,
		&parameterNames,
		&parameterDefaults,
		&requiredResultType);

	// change the return type, if the function is of type algorithm
	// to the return type given in brackets, e.g.
	// FEATURE(integer) returns actually an integer
	if(fftype == ALGORITHMOID){
		if(stmt->fstmt.returnType->typmods != NIL){
			typname = (TypeName *) linitial(stmt->fstmt.returnType->typmods);
			typname->arrayBounds = list_make1(makeInteger(-1));
		}
	}  else {
		typname = stmt->fstmt.returnType;
	}

	compute_return_type(typname, languageOid, &prorettype, &returnsSet);

	if (OidIsValid(requiredResultType) && prorettype != requiredResultType)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
			errmsg("function result type must be %s because of OUT parameters",
			format_type_be(requiredResultType))));

	compute_attributes_with_style(stmt->fstmt.withClause, &isStrict, &volatility);

	interpret_AS_clause(languageOid, language, funcname, as_clause,
		&prosrc_str, &probin_str);

	/*
	* Set default values for COST and ROWS depending on other parameters;
	* reject ROWS if it's not returnsSet.  NB: pg_dump knows these default
	* values, keep it in sync if you change them.
	*/
	if (procost < 0){
		/* SQL and PL-language functions are assumed more expensive */
		if (languageOid == INTERNALlanguageId ||
			languageOid == ClanguageId)
			procost = 1;
		else
			procost = 100;
	}
	
	if (prorows < 0){
		if (returnsSet)
			prorows = 1000;
		else
			prorows = 0;		/* dummy value if not returnsSet */
	} else if (!returnsSet)
		ereport(ERROR,
		(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		errmsg("ROWS is not applicable when function does not return a set")));
	
	/* adjust distance cost such that it is treated specially and only seldom executed */
	if(fftype == DISTANCEOID){
		procost = 1000;
	}
	
	// generate the internal name of the function; as often done in postgresql, we
	// just append the oid
	ffname = palloc(strlen(funcname) + 1);
	strcpy(ffname, funcname);

	funcname = palloc(2 + strlen(funcname) + sizeof(Oid) + 1);
	snprintf(funcname, 2 + strlen(funcname) + sizeof(Oid) + 1, "__%s%i", ffname, fftype);

	// do some checks (that are already performed in creating a function) here already, since the function to create
	// an entry in the catalog is only called after the function has already been created
	checkExistsTuple = SearchSysCacheCopy3(FEATUREFUNTYPENAME,
		CStringGetDatum(ffname),
		fftype,
		ObjectIdGetDatum(ffnamespace));
	if (!stmt->fstmt.replace && HeapTupleIsValid(checkExistsTuple)){
		ereport(ERROR,
			(errcode(ERRCODE_DUPLICATE_OBJECT),
			errmsg("%s \"%s\" already exists", typeTypeName(typeidType(fftype)), ffname)));
	} else if (stmt->fstmt.replace && HeapTupleIsValid(checkExistsTuple)){
		ffunoid = HeapTupleGetOid(checkExistsTuple);
	}

	// do the checks depending on the function type
	if(fftype == ALGORITHMOID){
		//check that there is a return type
		if (!stmt->fstmt.returnType){
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				errmsg("%s \"%s\" needs a return type", 
				typeTypeName(typeidType(fftype)), 
				ffname)));
		}
	}

	if(fftype == DISTANCEOID){
		ListCell *cell;
		Oid typemod = InvalidOid;
		int i;

		//check that there is a return type
		if (!stmt->fstmt.returnType){
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				errmsg("%s \"%s\" needs a return type", 
				typeTypeName(typeidType(fftype)), 
				ffname)));
		}

		//function parameters are identical
		cell = list_head(stmt->fstmt.parameters);
		for (i = 0; i <= 1 && cell != NULL; i++){
			Oid temptypmod = InvalidOid;
			FunctionParameter *fp = (FunctionParameter *) lfirst(cell);

			if(fp->argType->typmods != NULL){
				TypeName *name = (TypeName *) linitial(fp->argType->typmods);
				temptypmod = typenameTypeId(NULL, name);
			} else {
				temptypmod = -1;
			}

			if(typemod == InvalidOid){
				typemod = temptypmod;
			} else if(typemod == temptypmod){
				// do nothing
			} else {
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errmsg("%s \"%s\" should only take features of the same type", 
					typeTypeName(typeidType(fftype)), 
					ffname)));
			}

			cell = lnext(cell);
		}
	}


	if(fftype == NORMALIZATIONOID){
		//check that there is a return type
		if (!stmt->fstmt.returnType){
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				errmsg("%s \"%s\" needs a return type", 
				typeTypeName(typeidType(fftype)), 
				ffname)));
		}
	}

	//create the function
	procoid = ProcedureCreate(funcname,				/* using a special funcname */
		PG_CATALOG_NAMESPACE,						/* store in catalog namespace, only the adam feature function entry is for the actual namespace */
		stmt->fstmt.replace,
		returnsSet,
		prorettype,
		GetUserId(),
		languageOid,
		languageValidator,
		prosrc_str, /* converted to text later */
		probin_str, /* converted to text later */
		false,		/* not an aggregate */
		isWindowFunc,
		security,
		isLeakProof,
		isStrict,
		volatility,
		parameterTypes,
		PointerGetDatum(allParameterTypes),
		PointerGetDatum(parameterModes),
		PointerGetDatum(parameterNames),
		parameterDefaults,
		PointerGetDatum(proconfig),
		procost,
		prorows);

	// after having created the feature function, now create an entry in the adam catalog
	return internCreateFeatureFunction(ffunoid, ffname, ffnamespace, GetUserId(), procoid, fftype);
}



/*
 * gets the oid of the procedure that is hidden behind a feature function id;
 * thus, using this oid the true function can readily be called
 */
Oid
	getProcIdForFeatureFunId(Oid ffunid)
{
	Relation	ffrel;
	HeapTuple	fftup;
	Form_adam_featurefun ffform;
	Oid			result = InvalidOid;

	ffrel = heap_open(AdamFeatureFunRelationId, AccessShareLock);
	fftup = SearchSysCache1(FEATUREFUNOID, ObjectIdGetDatum(ffunid));

	if (!HeapTupleIsValid(fftup))
		elog(ERROR, "cache lookup failed for feature function %u", ffunid);

	ffform = (Form_adam_featurefun) GETSTRUCT(fftup);
	result = ffform->adamfoid;

	ReleaseSysCache(fftup);

	heap_close(ffrel, AccessShareLock);

	return result;
}

/*
* gets the parameter types of the distance function given by the proc id
*/
Oid*
	getParameterTypesFeatureFunction(Oid procid, int *n)
{
	Relation			pgprocrel;
	HeapTuple			proctup;
	Form_pg_proc	    procres;

	Oid					*result;


	pgprocrel = heap_open(ProcedureRelationId, AccessShareLock);
	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(procid));

	if(HeapTupleIsValid(proctup)){
		procres = (Form_pg_proc) GETSTRUCT(proctup);
		
		result = &procres->proargtypes.values[0];
		*n = procres->pronargs;
		ReleaseSysCache(proctup);
	}

	heap_close(pgprocrel, AccessShareLock);

	return result;
}


/*
 * removes a feature function entry given the feature function oid
 * (removal from adam catalog)
 */
void 
	removeFeatureFun(Oid procoid)
{
	Relation	ffrel;
	HeapTuple	tup;

	ffrel = heap_open(AdamFeatureFunRelationId, RowExclusiveLock);

	tup = SearchSysCache1(FEATUREFUNOID, ObjectIdGetDatum(procoid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for feature function %u", procoid);

	simple_heap_delete(ffrel, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(ffrel, RowExclusiveLock);
}


/*
* checks if given parameters for parametrized attribute-feature-function
* are allowed in that form or not (number of parameters and type)
*/
bool
	checkAdjustParameters(Oid ffunid, List** opts, int n, bool coercable)
{
	Relation			pgprocrel;
	HeapTuple			proctup;
	Form_pg_proc	    procres;

	Oid					procid;

	bool				result = false;

	List				*new_opts = NIL;
	
	if(ffunid == -1 || !OidIsValid(ffunid)){
		return false;
	}

	procid =  getProcIdForFeatureFunId(ffunid);
	pgprocrel = heap_open(ProcedureRelationId, AccessShareLock);
	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(procid));

	if(HeapTupleIsValid(proctup)){
		procres = (Form_pg_proc) GETSTRUCT(proctup);

		if((procres->pronargs - n) >= list_length(*opts)){
			
			Oid  *target = &procres->proargtypes.values[n];
			ListCell *cell;
			int i = 0;
			result = true;

			foreach(cell, *opts){
				Value *option = (Value *) lfirst(cell);
				Const *c = make_const(NULL, option, -1);
				
				Oid inputType = c->consttype;
				Oid targetType = target[i];

				if(inputType != targetType && !coercable){
					result = false;	
					break;
				} else if(!can_coerce_type(1, &inputType, &targetType, COERCION_IMPLICIT)){
					result = false;
					break;
				}
				
				new_opts = lappend(new_opts, c);

				i++;
			}
		}

		ReleaseSysCache(proctup);
	}

	heap_close(pgprocrel, AccessShareLock);

	*opts = new_opts;
	return result;
}