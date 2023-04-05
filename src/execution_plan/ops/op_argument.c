/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "op_argument.h"
#include "RG.h"

// Forward declarations
static Record ArgumentConsume(OpBase *opBase);
static OpResult ArgumentReset(OpBase *opBase);
static OpBase *ArgumentClone(const ExecutionPlan *plan, const OpBase *opBase);
static void ArgumentFree(OpBase *opBase);

static void ArgumentToString(
	const OpBase *ctx, 
	sds *buf
) {
	Argument *op = (Argument *)ctx;

	*buf = sdscatprintf(*buf, "%s | ", op->op.name);

	size_t record_str_cap = 0;
	char *record_str = NULL;
	// TODO: I expected the data to print in op->r, but it is null
	// GRAPH.QUERY g "MATCH (n1), (n2), (n3), (n4) WHERE (n3)-[:R]->(n4 {val:n3.val+1}) AND n1.val + n2.val = n3.val RETURN n1"
	if(op->r) {
		size_t record_str_len = Record_ToString(op->r, &record_str, &record_str_cap);
		*buf = sdscatprintf(*buf, "%s ", record_str);
	} else {
		*buf = sdscatprintf(*buf, "record is null");
	}

	rm_free(record_str);
}

OpBase *NewArgumentOp(const ExecutionPlan *plan, const char **variables) {
	Argument *op = rm_malloc(sizeof(Argument));
	op->r = NULL;

	// Set our Op operations
	OpBase_Init((OpBase *)op, OPType_ARGUMENT, "Argument", NULL,
				ArgumentConsume, ArgumentReset, ArgumentToString, ArgumentClone, ArgumentFree, false, plan);

	uint variable_count = array_len(variables);
	for(uint i = 0; i < variable_count; i ++) {
		OpBase_Modifies((OpBase *)op, variables[i]);
	}

	return (OpBase *)op;
}

static Record ArgumentConsume(OpBase *opBase) {
	Argument *arg = (Argument *)opBase;

	// Emit the record only once.
	// arg->r can already be NULL if the op is depleted.
	Record r = arg->r;
	arg->r = NULL;
	return r;
}

static OpResult ArgumentReset(OpBase *opBase) {
	// Reset operation, freeing the Record if one is held.
	Argument *arg = (Argument *)opBase;

	if(arg->r) {
		OpBase_DeleteRecord(arg->r);
		arg->r = NULL;
	}

	return OP_OK;
}

void Argument_AddRecord(Argument *arg, Record r) {
	ASSERT(!arg->r && "tried to insert into a populated Argument op");
	arg->r = r;
}

static inline OpBase *ArgumentClone(const ExecutionPlan *plan, const OpBase *opBase) {
	ASSERT(opBase->type == OPType_ARGUMENT);
	return NewArgumentOp(plan, opBase->modifies);
}

static void ArgumentFree(OpBase *opBase) {
	Argument *arg = (Argument *)opBase;
	if(arg->r) {
		OpBase_DeleteRecord(arg->r);
		arg->r = NULL;
	}
}

