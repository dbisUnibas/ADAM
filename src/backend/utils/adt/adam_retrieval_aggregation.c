/* 
* ADAM - retrieval aggregation functions
* name: adam_retrieval_aggregation
* description: functions for calculating the aggregations
* 
* developed in the course of the MSc thesis at the University of Basel
*
* author: Ivan Giangreco
* email: ivan.giangreco@unibas.ch
*
* src/backend/utils/adt/adam_retrieval_aggregation.c
*
* 
* 
*
*/
#include "postgres.h"

#include "utils/adam_retrieval_aggregation.h"

#include "fmgr.h"
#include "catalog/pg_proc.h"
#include "commands/defrem.h"
#include "utils/array.h"

#define EPSILON	0.00001

/*
 * given a bounded distance returns a similarity
 */
static float8
	distance_to_similarity(float8 distance)
{
	if (distance > 1){
        distance = 1;
    }
    
    if(distance < 0){
        distance = 0;
    }

	return 1 - distance;
}

/*
 * given a similarity returns a bounded distance
 */
static float8
similarity_to_distance(float8 similarity)
{
	if (similarity > 1){
		similarity = 1;
	}

	if (similarity < 0){
		similarity = 0;
	}
   
	return 1 - similarity;

}


/*
 * standard union
 * transition function
 *
 * u(m_a, m_b) = max(m_a, m_b)
 */
Datum
	standard_union_sfunc(PG_FUNCTION_ARGS)
{
	float8 transnum = PG_GETARG_FLOAT8(0);
	float8 newnum = distance_to_similarity(PG_GETARG_FLOAT8(1));
	
	if (transnum > newnum){
		PG_RETURN_FLOAT8(transnum);
	}
	else {
		PG_RETURN_FLOAT8(newnum);
	}
}

/*
 * standard union
 * final function
 *
 * u(m_a, m_b) = max(m_a, m_b)
 */
Datum
	standard_union_final(PG_FUNCTION_ARGS)
{
	float8		num = PG_GETARG_FLOAT8(0);
	PG_RETURN_FLOAT8(similarity_to_distance(num));
}

/*
 * algebraic union
 * transition function, calculates the sum and product part separately and returns array
 *
 * u(m_a, m_b) = m_a + m_b - m_a * m_b
 *
 */
Datum
	algebraic_union_sfunc(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8		newval = distance_to_similarity(PG_GETARG_FLOAT8(1));
    float8     *transdatums;
    
	float8		multX, sumX;
	ArrayType  *result;
	Datum	   *resultdatums = palloc(2 * sizeof(Datum));

	transdatums = (float8 *) ARR_DATA_PTR(transarray);
	
	//m_a + m_b
	sumX = transdatums[0];
	sumX = sumX + newval;

	//m_a * m_b
	multX = transdatums[1];
	multX = multX *  newval;

	resultdatums[0] = Float8GetDatum(sumX);
	resultdatums[1] = Float8GetDatum(multX);

	result = construct_array(resultdatums, 2, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');

	PG_RETURN_ARRAYTYPE_P(result);
}

/*
 * algebraic union
 * final function, combines results from array
 *
 * u(m_a, m_b) = m_a + m_b - m_a * m_b
 *
 */
Datum
	algebraic_union_final(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transdatums;
	float8		multX, sumX;

	transdatums = (float8 *) ARR_DATA_PTR(transarray);

    //m_a + m_b
	sumX = transdatums[0];
	//m_a * m_b
	multX = transdatums[1];
    
	PG_RETURN_FLOAT8(similarity_to_distance(sumX - multX));
}

/*
 * bounded union
 * transition function, calculates sum
 *
 * u(m_a, m_b) = min(1, m_a + m_b)
 *
 */
Datum
	bounded_union_sfunc(PG_FUNCTION_ARGS)
{
	float8		trans  = PG_GETARG_FLOAT8(0);
	float8		newval = distance_to_similarity(PG_GETARG_FLOAT8(1));

	float8		result;

	result = trans + newval;

	PG_RETURN_FLOAT8(result);
}

/*
 * bounded union
 * final function, combines results numeric sum
 *
 * u(m_a, m_b) = min(1, m_a + m_b)
 *
 */
Datum
	bounded_union_final(PG_FUNCTION_ARGS)
{
		float8		num = PG_GETARG_FLOAT8(0);

	if (num < 1){
		PG_RETURN_FLOAT8(similarity_to_distance(num));
	}
	else {
		PG_RETURN_FLOAT8(similarity_to_distance(1));
	}
}


/*
 * drastic union
 * transition function
 *
 *               m_a  when m_b = 0
 * u(m_a, m_b) = m_b  when m_a = 0
 *                1   otherwise
 */
Datum
	drastic_union_sfunc(PG_FUNCTION_ARGS)
{
	float8  transnum = PG_GETARG_FLOAT8(0);
	float8	 newnum = distance_to_similarity(PG_GETARG_FLOAT8(1));

	// m_a  when m_b = 0
	if (newnum < EPSILON){
		PG_RETURN_FLOAT8(transnum);
	}

	if (transnum < EPSILON){
		PG_RETURN_FLOAT8(newnum);
	}

	// 1 otherwise
    PG_RETURN_FLOAT8(1);
}

/*
 * drastic union
 * final function
 *
 *               m_a  when m_b = 0
 * u(m_a, m_b) = m_b  when m_a = 0
 *                1   otherwise
 */
Datum
	drastic_union_final(PG_FUNCTION_ARGS)
{
	float8		num =    PG_GETARG_FLOAT8(0);
	
	PG_RETURN_FLOAT8(similarity_to_distance(num));
}


/*
 * standard intersect
 * final function
 *
 * n(m_a, m_b) = min(m_a, m_b)
 */
Datum
	standard_intersect_sfunc(PG_FUNCTION_ARGS)
{
	float8  transnum = PG_GETARG_FLOAT8(0);
	float8	 newnum = distance_to_similarity(PG_GETARG_FLOAT8(1));

	if (transnum < newnum){
		PG_RETURN_FLOAT8(transnum);
	} else {
		PG_RETURN_FLOAT8(newnum);
	}
}

/*
 * standard intersect
 * final function
 *
 * n(m_a, m_b) = min(m_a, m_b)
 */
Datum
	standard_intersect_final(PG_FUNCTION_ARGS)
{
	float8		num =    PG_GETARG_FLOAT8(0);
	
	PG_RETURN_FLOAT8(similarity_to_distance(num));
}

/*
 * algebraic intersect
 * transit function
 *
 * n(m_a, m_b) = m_a * m_b
 *
 */
Datum
	algebraic_intersect_sfunc(PG_FUNCTION_ARGS)
{
	float8		trans = PG_GETARG_FLOAT8(0);
	float8		num = distance_to_similarity(PG_GETARG_FLOAT8(1));

	PG_RETURN_FLOAT8(trans * num);
}

/*
 * algebraic intersect
 * final function, combines results numeric sum
 *
 * n(m_a, m_b) = m_a * m_b
 *
 */
Datum
	algebraic_intersect_final(PG_FUNCTION_ARGS)
{
	float8		num = PG_GETARG_FLOAT8(0);

	PG_RETURN_FLOAT8(similarity_to_distance(num));
}

/*
 * bounded intersect
 * transit function
 *
 * n(m_a, m_b) = max(0, m_a + m_b - 1)
 *
 */
Datum
	bounded_intersect_sfunc(PG_FUNCTION_ARGS)
{
	float8		trans = PG_GETARG_FLOAT8(0);
	float8		num = distance_to_similarity(PG_GETARG_FLOAT8(1));

	PG_RETURN_FLOAT8(trans + num);
}

/*
 * bounded intersect
 * final function, combines results numeric sum
 *
 * n(m_a, m_b) = max(0, m_a + m_b - 1)
 *
 */
Datum
	bounded_intersect_final(PG_FUNCTION_ARGS)
{
	float8		num = PG_GETARG_FLOAT8(0);
	float8		result = num - 1;

	if (result > 0){
		PG_RETURN_FLOAT8(similarity_to_distance(result));
	}
	else {
		PG_RETURN_FLOAT8(similarity_to_distance(0));
	}
}

/*
 * drastic intersection
 * transition function
 *
 *               m_a  when m_b = 1
 * n(m_a, m_b) = m_b  when m_a = 1
 *                0   otherwise
 */
Datum
	drastic_intersect_sfunc(PG_FUNCTION_ARGS)
{
	float8  transnum = PG_GETARG_FLOAT8(0);
	float8	 newnum = distance_to_similarity(PG_GETARG_FLOAT8(1));

	bool ma_is_1 = false;
	bool mb_is_1 = false;

	if(newnum - 1 < EPSILON){
		PG_RETURN_FLOAT8(transnum);
	}

	// m_b  when m_a = 1
	if(transnum - 1 < EPSILON){
		PG_RETURN_FLOAT8(newnum);
	}

	// 0 otherwise
	PG_RETURN_FLOAT8(0);
}

/*
 * drastic intersection
 * final function
 *
 *               m_a  when m_b = 1
 * n(m_a, m_b) = m_b  when m_a = 1
 *                0   otherwise
 */
Datum
	drastic_intersect_final(PG_FUNCTION_ARGS)
{
	Datum  result = PG_GETARG_DATUM(0);

	PG_RETURN_FLOAT8(similarity_to_distance(result));
}





/*
 * standard complement
 *
 * c(m_b) = 1 - m_b
 *
 * the except is calculated by using an intersection between set m_a and the
 * complement of m_b
 * m_a / m_b = m_a n c(m_b)
 * (see analyze.c where this is done)
 */
Datum
	standard_except(PG_FUNCTION_ARGS)
{
	float8	num;

	if(PG_NARGS() != 1){
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("no parameters are allowed for standard except"),
			errhint("parameters are only allowed for the Yager and Sugeno complement")));
	}

	num = distance_to_similarity(PG_GETARG_FLOAT8(0));
	PG_RETURN_FLOAT8(similarity_to_distance(1 - num));
}

/*
 * sugeno complement
 *             1 - m_b
 * c(m_b) = -------------
 *            1 + l*m_b
 *
 * where l is a free parameter
 *
 * the except is calculated by using an intersection between set m_a and the
 * complement of m_b
 * m_a / m_b = m_a n c(m_b)
 * (see analyze.c where this is done)
 */
Datum
	sugeno_except(PG_FUNCTION_ARGS)
{
	float8	num;
	float8	param;
	float8	result;

	if(PG_NARGS() != 2){
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("exactly one parameter is necessary for sugeno complement")));
	}

	num = distance_to_similarity(PG_GETARG_FLOAT8(0));
	param = PG_GETARG_FLOAT8(1);
    
    
	result = (1 - num) / (1 + param * num);
    
	PG_RETURN_FLOAT8(similarity_to_distance(result));
}

/*
 * yager complement
 *
 * c(m_b) = (1 - m_b ^ w) ^ (1/w)
 *
 * where w is a free parameter
 *
 * the except is calculated by using an intersection between set m_a and the
 * complement of m_b
 * m_a / m_b = m_a n c(m_b)
 * (see analyze.c where this is dyagone)
 */
Datum
	yager_except(PG_FUNCTION_ARGS)
{
	float8	num;
	float8	param;
	float8	result;

	if(PG_NARGS() != 2){
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("exactly one parameter is necessary for yager complement")));
	}

	num = distance_to_similarity(PG_GETARG_FLOAT8(0));
	param = PG_GETARG_FLOAT8(1);
    
    
	result = DatumGetFloat8(DirectFunctionCall2(dpow,
		Float8GetDatum(1 - DatumGetFloat8(DirectFunctionCall2(dpow, Float8GetDatum(num), Float8GetDatum(param)))),
                        Float8GetDatum(1 / param)));

	PG_RETURN_FLOAT8(similarity_to_distance(result)); 
}