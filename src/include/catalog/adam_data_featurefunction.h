/* 
 * ADAM - adam functions catalog table
 * name: adam_data_featurefunction
 * description: system catalogs table supporting functions for adam
 * 
 * developed in the course of the MSc thesis at the University of Basel
 *
 * author: Ivan Giangreco
 * email: ivan.giangreco@unibas.ch
 *
 * src/backend/includes/catalog/adam_data_featurefunction.h
 *
 * 
 * 
 *
 */
#ifndef ADAM_DATA_FEATUREFUNCTION_H
#define ADAM_DATA_FEATUREFUNCTION_H

#include "catalog/genbki.h"

#define AdamFeatureFunRelationId			4318
#define AdamFeatureFunRelation_Rowtype_Id	4711

CATALOG(adam_featurefun,4318) BKI_BOOTSTRAP BKI_ROWTYPE_OID(4711) BKI_SCHEMA_MACRO
{
	NameData	adamfname;			/* adam function's name */
	Oid			adamfnamespace;		/* OID of namespace containing this adam function */
	Oid			adamfowner;			/* adam function owner */
	Oid			adamfoid;			/* OID of function to be called */
	Oid			adamftype;			/* ALGORITHM/DISTANCE/INDEX/NORMALIZATION; see below */
#ifdef CATALOG_VARLEN
#endif
} FormData_adam_featurefun;

/* ----------------
 *		Form_adam_featurefun corresponds to a pointer to a tuple with
 *		the format of adam_featurefun relation.
 * ----------------
 */
typedef FormData_adam_featurefun *Form_adam_featurefun;

/* ----------------
 *		compiler constants for pg_featurefun
 * ----------------
 */
#define Natts_adam_featurefun				5
#define Anum_adam_featurefun_fname			1
#define Anum_adam_featurefun_fnamespace		2
#define Anum_adam_featurefun_fowner			3
#define Anum_adam_featurefun_foid			4
#define Anum_adam_featurefun_ftype			5

DATA(insert OID = 4235 (  minkowski				 PGNSP PGUID 4215 4714));
#define MINKOWSKI 4235
DATA(insert OID = 4236 (  minkowski_weighted	 PGNSP PGUID 4216 4714));
#define MINKOWSKI_WEIGHTED 4236
DATA(insert OID = 4237 (  minmax				 PGNSP PGUID 4220 4713));
DATA(insert OID = 4238 (  gaussian				 PGNSP PGUID 4221 4713));

#endif   /* ADAM_DATA_FEATUREFUNCTION_H */