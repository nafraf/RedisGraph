/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "execution_plan_construct.h"
#include "execution_plan_modify.h"
#include "../../RG.h"
#include "../ops/ops.h"
#include "../../errors.h"
#include "../../query_ctx.h"
#include "../execution_plan.h"
#include "../../ast/ast_build_op_contexts.h"
#include "../../arithmetic/arithmetic_expression_construct.h"

// Handle ORDER entities
static AR_ExpNode **_BuildOrderExpressions(AR_ExpNode **projections,
										   const cypher_astnode_t *order_clause) {
	uint count = cypher_ast_order_by_nitems(order_clause);
	AR_ExpNode **order_exps = array_new(AR_ExpNode *, count);

	for(uint i = 0; i < count; i++) {
		const cypher_astnode_t *item = cypher_ast_order_by_get_item(order_clause, i);
		const cypher_astnode_t *ast_exp = cypher_ast_sort_item_get_expression(item);
		AR_ExpNode *exp = AR_EXP_FromASTNode(ast_exp);
		exp->resolved_name = AST_ToString(ast_exp);
		array_append(order_exps, exp);
	}

	return order_exps;
}

// Handle projected entities
// (This function is not static because it is relied upon by unit tests)
AR_ExpNode **_BuildProjectionExpressions(const cypher_astnode_t *clause) {
	uint count = 0;
	AR_ExpNode **expressions = NULL;
	cypher_astnode_type_t t = cypher_astnode_type(clause);

	ASSERT(t == CYPHER_AST_RETURN || t == CYPHER_AST_WITH);

	if(t == CYPHER_AST_RETURN) {
		// if we have a "RETURN *" at this point, it is because we raised 
		// an error in AST rewriting
		if(cypher_ast_return_has_include_existing(clause)) return NULL;
		count = cypher_ast_return_nprojections(clause);
	} else {
		ASSERT(cypher_ast_with_has_include_existing(clause) == false);
		count = cypher_ast_with_nprojections(clause);
	}

	expressions = array_new(AR_ExpNode *, count);

	rax *rax = raxNew();

	for(uint i = 0; i < count; i++) {
		const cypher_astnode_t *projection = NULL;
		if(t == CYPHER_AST_RETURN) {
			projection = cypher_ast_return_get_projection(clause, i);
		} else {
			projection = cypher_ast_with_get_projection(clause, i);
		}

		// The AST expression can be an identifier, function call, or constant
		const cypher_astnode_t *ast_exp =
			cypher_ast_projection_get_expression(projection);

		// Find the resolved name of the entity - its alias,
		// its identifier if referring to a full entity,
		// the entity.prop combination ("a.val"),
		// or the function call ("MAX(a.val)")
		const char *identifier = NULL;
		const cypher_astnode_t *alias_node =
			cypher_ast_projection_get_alias(projection);

		if(alias_node) {
			// The projection either has an alias (AS), is a function call,
			// or is a property specification (e.name).
			identifier = cypher_ast_identifier_get_name(alias_node);
		} else {
			// This expression did not have an alias,
			// so it must be an identifier
			ASSERT(cypher_astnode_type(ast_exp) == CYPHER_AST_IDENTIFIER);
			// Retrieve "a" from "RETURN a" or "RETURN a AS e"
			// (theoretically; the latter case is already handled)
			identifier = cypher_ast_identifier_get_name(ast_exp);
		}

		if(raxTryInsert(rax, (unsigned char *)identifier, strlen(identifier), NULL, NULL) != 0) {
			// Construction an AR_ExpNode to represent this projected entity.
			AR_ExpNode *exp = AR_EXP_FromASTNode(ast_exp);
			exp->resolved_name = identifier;
			array_append(expressions, exp);
		}
	}

	raxFree(rax);

	return expressions;
}

// Add WITH LHS projections to projection_names rax
static void _add_WITH_LHS_projections
(
	const cypher_astnode_t *clause, //
	rax *projection_names		    // rax projection names
) {
	uint count = 0;
	cypher_astnode_type_t t = cypher_astnode_type(clause);

	if(t == CYPHER_AST_WITH) {
		count = cypher_ast_with_nprojections(clause);
		for(uint i = 0; i < count; i++) {
			const cypher_astnode_t *projection = NULL;
			projection = cypher_ast_with_get_projection(clause, i);

			// The AST expression can be an identifier, function call, or constant
			const cypher_astnode_t *ast_exp = cypher_ast_projection_get_expression(projection);

			// Add to rax the LHS of "AS" projected entities. "WITH a AS x" will just collect "a".
			const char *resolved_name = NULL;
			cypher_astnode_type_t type1 = cypher_astnode_type(projection);
			cypher_astnode_type_t type = cypher_astnode_type(ast_exp);

			if(type == CYPHER_AST_PROPERTY_OPERATOR) {
				//--------------------------------------------------------------------------
				// Extract entity and property name expressions.
				//--------------------------------------------------------------------------
				const cypher_astnode_t *prop_expr = cypher_ast_property_operator_get_expression(ast_exp);
				const cypher_astnode_t *prop_name_node = cypher_ast_property_operator_get_prop_name(ast_exp);
				const char *prop_name = cypher_ast_prop_name_get_value(prop_name_node);

				if(cypher_astnode_type(prop_expr) == CYPHER_AST_IDENTIFIER) {
					const char *alias = cypher_ast_identifier_get_name(prop_expr);
					uint len_resolved_name = strlen(alias)+strlen(prop_name)+2;
					char *resolved_name = rm_malloc(sizeof(char)*(len_resolved_name));
					snprintf(resolved_name, len_resolved_name, "%s.%s", alias, prop_name);
					raxTryInsert(projection_names, (unsigned char *)resolved_name, strlen(resolved_name), NULL, NULL);
				}
			} else if (type == CYPHER_AST_APPLY_OPERATOR) {
				//TO DO: Add aggregate functions: 
				// "WITH avg(a) AS x" will just collect "avg(a)"
				// "WITH max(me.age+round(me.age)*2) as max_age" will just collect "WITH max(me.age+round(me.age)*2)"
			}
		}
	}
}

// Validate if order expression is part of projection expressions
bool _validateOrderExpression(AR_ExpNode *order_exp, rax *projection_names) {
	if(order_exp->type == AR_EXP_OPERAND) {
		AR_OperandNodeType type = order_exp->operand.type;
		switch(type) {
			case AR_EXP_CONSTANT:
				return true;
				break;
			case AR_EXP_VARIADIC:
				const char *entity_alias = order_exp->operand.variadic.entity_alias;
				if(raxFind(projection_names, (unsigned char *)entity_alias, strlen(entity_alias)) != raxNotFound) {
					return true;
				}
				break;
			case AR_EXP_PARAM:
				const char *param_name = order_exp->operand.param_name;
				if(raxFind(projection_names, (unsigned char *)param_name, strlen(param_name)) != raxNotFound) {
					return true;
				}
				break;
			case AR_EXP_BORROW_RECORD:
				// ??
				return true;
				break;
			default:
				// not supposed to get here
				ASSERT(false);
		}
	} else if(order_exp->type == AR_EXP_OP) {
		printf("Nafraf order_exp->type == AR_EXP_OP, %s, resolved_name=%s\n", order_exp->op.f->name, order_exp->resolved_name);

		if(strcmp(AR_EXP_GetFuncName(order_exp), "property") == 0) {
			// Validate using resolved_name
			const char *identifer = order_exp->resolved_name;
			if(raxFind(projection_names, (unsigned char *)identifer, strlen(identifer)) != raxNotFound) {
				return true;
			}
		} else {
			bool children_valid = true;
			for(uint i = 0; i < order_exp->op.child_count; i++){
				AR_ExpNode *child = order_exp->op.children[i];
				if (_validateOrderExpression(child, projection_names) == false) {
					return false;
				}
			}
		}
		
		return true;
	}
	return false;
}

// Merge all order expressions into the projections array without duplicates,
static void _combine_projection_arrays
(
	AR_ExpNode ***exps_ptr,   // projection expressions
	AR_ExpNode **order_exps,  // expressions in ORDER BY clause
	bool aggregate,           // is an agg-func used in one of the projections
	rax *projection_names     // 
) {
	
	AR_ExpNode **project_exps = *exps_ptr;
	uint order_count = array_len(order_exps);
	uint project_count = array_len(project_exps);

	// Add all WITH/RETURN projection names to rax
	for(uint i = 0; i < project_count; i ++) {
		const char *name = project_exps[i]->resolved_name;
		raxTryInsert(projection_names, (unsigned char *)name, strlen(name), NULL, NULL);
	}

	// if an aggregation is performed in one of the projections, only projected
	// variables are valid in the ORDER BY clause
	if(aggregate) {
		for(uint i = 0; i < order_count; i++) {
			bool found = false;
			for(uint j = 0; j < project_count && !found; j++) {
				found = strcmp(order_exps[i]->resolved_name,
							   project_exps[j]->resolved_name) == 0 ||
						AR_EXP_Equal(order_exps[i], project_exps[j]);
			}
			if(!found) {
				if(_validateOrderExpression(order_exps[i], projection_names)) {
					continue;
				}
				ErrorCtx_SetError("In a WITH/RETURN with an aggregation,\
it is not possible to access variables not projected by the WITH/RETURN.");
			}
		}
	}


	// Merge non-duplicate order expressions into projection array.
	for(uint i = 0; i < order_count; i ++) {
		const char *name = order_exps[i]->resolved_name;
		int new_name = raxTryInsert(projection_names, (unsigned char *)name, strlen(name), NULL, NULL);
		// If it is a new projection, add a clone to the array.
		if(new_name) array_append(project_exps, AR_EXP_Clone(order_exps[i]));
	}

	*exps_ptr = project_exps;
}

// build an aggregate or project operation and any required modifying operations
// this logic applies for both WITH and RETURN projections
static inline void _buildProjectionOps(ExecutionPlan *plan,
									   const cypher_astnode_t *clause) {

	OpBase                  *op               =  NULL  ;
	OpBase                  *distinct_op      =  NULL  ;
	bool                    distinct          =  false ;
	bool                    aggregate         =  false ;
	int                     *sort_directions  =  NULL  ;
	AR_ExpNode              **order_exps      =  NULL  ;
	AR_ExpNode              **projections     =  NULL  ;
	const cypher_astnode_t  *skip_clause      =  NULL  ;
	const cypher_astnode_t  *limit_clause     =  NULL  ;
	const cypher_astnode_t  *order_clause     =  NULL  ;

	cypher_astnode_type_t t = cypher_astnode_type(clause);
	ASSERT(t == CYPHER_AST_WITH || t == CYPHER_AST_RETURN);

	aggregate = AST_ClauseContainsAggregation(clause);
	projections = _BuildProjectionExpressions(clause);

	if(t == CYPHER_AST_WITH) {
		distinct      =  cypher_ast_with_is_distinct(clause);
		skip_clause   =  cypher_ast_with_get_skip(clause);
		limit_clause  =  cypher_ast_with_get_limit(clause);
		order_clause  =  cypher_ast_with_get_order_by(clause);
	} else {
		distinct      =  cypher_ast_return_is_distinct(clause);
		skip_clause   =  cypher_ast_return_get_skip(clause);
		limit_clause  =  cypher_ast_return_get_limit(clause);
		order_clause  =  cypher_ast_return_get_order_by(clause);
	}

	if(distinct) {
		// Prepare the distinct op but do not add it to op tree.
		// This is required so that it does not operate on order expressions.
		uint n = array_len(projections);

		// Populate a stack array with the aliases to perform Distinct on
		const char *aliases[n];
		for(uint i = 0; i < n; i ++) aliases[i] = projections[i]->resolved_name;
		distinct_op = NewDistinctOp(plan, aliases, n);
	}

	if(order_clause) {
		AST_PrepareSortOp(order_clause, &sort_directions);
		order_exps = _BuildOrderExpressions(projections, order_clause);

		rax *projection_names = raxNew();
		if(aggregate) {
			_add_WITH_LHS_projections(clause, projection_names);
		}
		// Merge order expressions into the projections array.
		_combine_projection_arrays(&projections, order_exps, aggregate, projection_names);
		raxFree(projection_names);
	}

	// our fundamental operation will be a projection or aggregation
	if(aggregate) {
		// an aggregate op's caching policy depends on
		// whether its results will be sorted
		op = NewAggregateOp(plan, projections);
	} else {
		op = NewProjectOp(plan, projections);
	}
	ExecutionPlan_UpdateRoot(plan, op);

	/* Add modifier operations in order such that the final execution plan will follow the sequence:
	 * Limit -> Skip -> Sort -> Distinct -> Project/Aggregate */

	if(distinct_op) {
		ExecutionPlan_UpdateRoot(plan, distinct_op);
	}

	if(sort_directions) {
		// The sort operation will obey a specified limit, but must account for skipped records
		op = NewSortOp(plan, order_exps, sort_directions);
		ExecutionPlan_UpdateRoot(plan, op);
	}

	if(skip_clause) {
		op = buildSkipOp(plan, skip_clause);
		ExecutionPlan_UpdateRoot(plan, op);
	}

	if(limit_clause) {
		op = buildLimitOp(plan, limit_clause);
		ExecutionPlan_UpdateRoot(plan, op);
	}
}

// RETURN builds a subtree of projection ops with Results as the root.
void buildReturnOps(ExecutionPlan *plan, const cypher_astnode_t *clause) {
	_buildProjectionOps(plan, clause);

	// follow up with a Result operation
	OpBase *op = NewResultsOp(plan);
	ExecutionPlan_UpdateRoot(plan, op);
}

// RETURN builds a subtree of projection ops.
void buildWithOps(ExecutionPlan *plan, const cypher_astnode_t *clause) {
	_buildProjectionOps(plan, clause);
}

