/* 
 * ADAM - bitstring functions
 * name: adam_utils_bitstring
 * description: functions to create and manipulate a very simple bitstring
 *				the code is kept very simple and not yet generic, but for the
 *				purpose used, it is more than enough
 * 
 * developed in the course of the MSc thesis at the University of Basel
 *
 * author: Ivan Giangreco
 * email: ivan.giangreco@unibas.ch
 *
 * src/include/utils/adam_utils_bitstring.h
 *
 * 
 * 
 *
 */
#ifndef ADAM_UTILS_BITSTRING_H
#define ADAM_UTILS_BITSTRING_H

typedef uint8 BitStringElement;

#define CREATE_BIT_STRING(size) palloc0(size * sizeof(BitStringElement))
#define SET_BITS(bstring, category, value) bstring[category] = (BitStringElement) value
#define GET_WORD(bstring, category) bstring[category]

#endif   /* ADAM_UTILS_BITSTRING_H */

