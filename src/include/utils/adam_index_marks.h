/* 
 * ADAM - index marks
 * name: adam_index_marks
 * description: functions to establish the marks for VA approximation
 * 
 * developed in the course of the MSc thesis at the University of Basel
 *
 * author: Ivan Giangreco
 * email: ivan.giangreco@unibas.ch
 *
 * src/include/utils/adam_index_marks.h
 *
 * 
 * 
 *
 */
#ifndef ADAM_INDEX_MARKS_H
#define ADAM_INDEX_MARKS_H

#include "postgres.h"

#include "utils/adam_data_feature.h"

#include "nodes/execnodes.h"
#include "utils/relcache.h"

#define NUM_TRUNCATE		    5
//corresponds to 16 places before comma, 5 places after comma
//this is calculated like that:
//the first 16 bits (least significant) denote the scale, i.e. what comes after the comma
//the next  16 bits (most significant) denote the precision, i.e. the number of total digits in the number
#define NUM_SCALE_PRECISION	    1048585
#define MAX_MARKS				64
#define SAMPLING_FREQUENCY		10000
#define N_SAMPLES				10000
#define MAX_PARTITIONS			MAX_MARKS - 1

extern Datum calculateMarks(Relation rel, IndexInfo *indexInfo);



#endif   /* ADAM_INDEX_MARKS_H */

