/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "op_call_subquery.h"

// forward declarations
static void CallSubqueryFree(OpBase *opBase);
static OpResult CallSubqueryInit(OpBase *opBase);
static Record CallSubqueryConsume(OpBase *opBase);
static OpResult CallSubqueryReset(OpBase *opBase);
static Record CallSubqueryConsumeEager(OpBase *opBase);
static OpBase *CallSubqueryClone(const ExecutionPlan *plan, const OpBase *opBase);

// creates a new CallSubquery operation
OpBase *NewCallSubqueryOp
(
	const ExecutionPlan *plan,  // execution plan
    bool is_eager,              // if an updating clause lies in the body, eagerly consume the records
    bool is_returning           // is the subquery returning or unit
) {
    OpCallSubquery *op = rm_calloc(1, sizeof(OpCallSubquery));

	op->r             = NULL;
	op->lhs           = NULL;
    op->body          = NULL;
	op->first         = true;
	op->records       = NULL;
    op->argument      = NULL;
	op->argument_list = NULL;
	op->is_eager      = is_eager;
    op->is_returning  = is_returning;

    // set the consume function according to eagerness of the op
    Record (*consumeFunc)(OpBase *opBase) = is_eager ? CallSubqueryConsumeEager :
                                        CallSubqueryConsume;

    OpBase_Init((OpBase *)op, OPType_CallSubquery, "CallSubquery", CallSubqueryInit,
			consumeFunc, CallSubqueryReset, NULL, CallSubqueryClone, CallSubqueryFree,
			false, plan);

	return (OpBase *)op;
}

static OpResult CallSubqueryInit
(
    OpBase *opBase  // CallSubquery operation to initialize
) {
    OpCallSubquery *op = (OpCallSubquery *)opBase;

	// set the lhs (supplier) branch to be the first child, and rhs branch
    // (body) to be the second
    if(op->op.childCount == 2) {
        op->lhs = op->op.children[0];
        op->body = op->op.children[1];
    } else {
        op->body = op->op.children[0];
    }

	// search for the ArgumentList\Argument op, depending if the op is eager
    // the second will stay NULL
    // find the deepest operation
    OpBase *deepest = op->body;
    while(deepest->childCount > 0) {
        deepest = deepest->children[0];
    }
    if(op->is_eager) {
        op->argument_list = (ArgumentList *)deepest;
        // validate found operation type, expecting ArgumentList
        ASSERT(OpBase_Type((const OpBase *)op->argument_list) == OPType_ARGUMENT_LIST);
    } else {
        // search for Argument op
        op->argument = (Argument *)deepest;
        ASSERT(OpBase_Type((const OpBase *)op->argument) == OPType_ARGUMENT);
    }

    return OP_OK;
}

// passes a record to the parent op.
// if the subquery is non-returning, all the records have already been
// consumed from the body (child depleted), so that we only need to return the
// received records.
// if the subquery is returning, return a the record received from the body
static Record _handoff_eager(OpCallSubquery *op) {
    ASSERT(op->is_returning || op->records != NULL);

    if(!op->is_returning) {
        // if there is a record to return from the input records, return it
        // NOTICE: The order of records reverses here.
        return array_len(op->records) > 0 ? array_pop(op->records) : NULL;
    }

    // returning subquery
    else {
        // get a record from the body, and pass it on (start from a clone of it, optimize later)
        return OpBase_Consume(op->body);
    }
}

// eagerly consume and aggregate all the records from the lhs (if exists). pass
// the aggregated record-list to the ArgumentList operation.
// after aggregating, return the records one-by-one to the parent op
// merges the records if is_returning is on.
static Record CallSubqueryConsumeEager
(
    OpBase *opBase  // operation
) {
    OpCallSubquery *op = (OpCallSubquery *)opBase;

    // if eager consumption has already occurred, don't consume again
    if(!op->first) {
        return _handoff_eager(op);
    }

    // make sure next entries don't get here
    op->first = false;

    op->records = array_new(Record, 1);

    // eagerly consume all records from lhs if exists or create a
    // dummy-record, and place them op->records
    Record r;
    if(op->lhs) {
        while((r = OpBase_Consume(op->lhs))) {
            array_append(op->records, r);
        }
    } else {
        r = OpBase_CreateRecord(op->body);
        array_append(op->records, r);
    }

    if(op->is_returning) {
        // we can pass op->records (rather than a clone), since we later return
        // the consumed records from the body
        ArgumentList_AddRecordList(op->argument_list, op->records);
        // responsibility for the records is passed to the argumentList op
        op->records = NULL;
    } else {
        // pass a clone of op->records to arglist, since we need to later return
        // the received records
        Record *records_clone;
        array_clone_with_cb(records_clone, op->records, OpBase_DeepCloneRecord);
        ArgumentList_AddRecordList(op->argument_list, records_clone);

        // consume and free all records from body
        while((r = OpBase_Consume(op->body))) {
            OpBase_DeleteRecord(r);
        }
    }

    return _handoff_eager(op);
}

static Record _consume_and_return
(
    OpCallSubquery *op
) {
    Record consumed;
    consumed = OpBase_Consume(op->body);
    if(consumed == NULL) {
        OpBase_DeleteRecord(op->r);
        op->r = NULL;
        if(op->lhs && (op->r = OpBase_Consume(op->lhs)) != NULL) {
            // plant a clone of the record consumed at the Argument op
            Argument_AddRecord(op->argument, OpBase_DeepCloneRecord(op->r));
        } else {
            // lhs depleted --> CALL {} depleted as well
            return NULL;
        }
        // goto return_cycle;
        return _consume_and_return(op);
    }

    Record clone = OpBase_DeepCloneRecord(op->r);
    // Merge consumed record into a clone of the received record.
    // Note: Must use this instead of `Record_Merge()` in cases where the
    // last op isn't a projection (e.g., Sort due to an ORDER BY etc.)
    Record_Merge_Into(clone, consumed);
    OpBase_DeleteRecord(consumed);
    return clone;
}

// tries to consume a record from the body. if successful, return the
// merged\unmerged record with the input record (op->r) according to whether the
// sq is returning\non-returning.
// Depletes child if non-returning (body records not needed).
// if unsuccessful (child depleted), returns NULL
static Record _handoff(OpCallSubquery *op) {
    ASSERT(op->r != NULL);

    // returning subquery: consume --> merge --> return merged.
    if(op->is_returning) {
        return _consume_and_return(op);
    }

    Record consumed;
    // non-returning subquery: drain the body, deleting (freeing) the records
    // return current record
    while((consumed = OpBase_Consume(op->body))) {
        OpBase_DeleteRecord(consumed);
    }
    Record r = op->r;
    op->r = NULL;
    return r;
}

// similar consume to the Apply op, differing from it in that a lhs is optional,
// and that it merges the records if is_returning is on.
// responsibility for the records remains within the op.
static Record CallSubqueryConsume
(
    OpBase *opBase  // operation
) {
    OpCallSubquery *op = (OpCallSubquery *)opBase;

    // if there are more records to consume from body, consume them before
    // consuming another record from lhs
    if(op->r) {
        return _handoff(op);
    }

    // consume from lhs if exists, otherwise create dummy-record to pass to rhs
    // (the latter case will happen AT MOST once)
    if(op->lhs) {
        op->r = OpBase_Consume(op->lhs);
    } else {
        op->r = OpBase_CreateRecord(op->body);
    }

    // plant the record consumed at the Argument op
    if(op->r) {
        Argument_AddRecord(op->argument, OpBase_DeepCloneRecord(op->r));
    } else {
        // no records
        return NULL;
    }

    return _handoff(op);
}

// free CallSubquery internal data structures
static void _freeInternals
(
	OpCallSubquery *op  // operation to free
) {
    if(op->records != NULL) {
        uint n_records = array_len(op->records);
        for(uint i = 0; i < n_records; i++) {
            OpBase_DeleteRecord(op->records[i]);
        }
        array_free(op->records);
        op->records = NULL;
    }
}

static OpResult CallSubqueryReset
(
    OpBase *opBase  // operation
) {
    OpCallSubquery *op = (OpCallSubquery *)opBase;

    _freeInternals(op);

	return OP_OK;
}

static OpBase *CallSubqueryClone
(
    const ExecutionPlan *plan,  // plan
    const OpBase *opBase        // operation to clone
) {
    ASSERT(opBase->type == OPType_CallSubquery);
	OpCallSubquery *op = (OpCallSubquery *) opBase;

	return NewCallSubqueryOp(plan, op->is_eager, op->is_returning);
}

static void CallSubqueryFree
(
	OpBase *op
) {
	OpCallSubquery *_op = (OpCallSubquery *) op;

	_freeInternals(_op);
}