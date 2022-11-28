/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "../../value.h"
#include "../arithmetic_expression.h"

// reduce function context object
typedef struct {
	const char *variable;     // closure varaible name
	const char *accumulator;  // closure accumulator name
	int variable_idx;         // closure variable record index
	int accumulator_idx;      // closure accumulator record index
	AR_ExpNode *exp;          // expression used for reduction
	Record record;            // internal private record
} ListReduceCtx;

void Register_ListFuncs();

#define preprocess_list_argument(arg, value) ({ \
	if (SI_TYPE(arg) == T_NULL) {				\
        value = SI_EmptyArray();				\
    } else if (SI_TYPE(value) != T_ARRAY) {		\
        value = SI_Array(1);					\
        SIArray_Append(&value, arg);			\
    }											\
})

