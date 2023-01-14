/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "cmd_bulk_insert.h"
#include "query_ctx.h"
#include "bulk_insert/bulk_insert.h"

// process "BEGIN" token, expected to be present only on first bulk-insert
// batch, make sure graph key doesn't exists, fails if "BEGIN" token is present
// and graph key 'graphname' already exists
static int _Graph_Bulk_Begin(RedisModuleCtx *ctx, RedisModuleString ***argv,
							  int *argc, RedisModuleString *rs_graph_name, const char *graphname,
							  bool *begin) {
	ASSERT(argv           !=  NULL);
	ASSERT(argc           !=  NULL);
	ASSERT(begin          !=  NULL);
	ASSERT(graphname      !=  NULL);
	ASSERT(rs_graph_name  !=  NULL);

	const char *token = RedisModule_StringPtrLen(**argv, NULL);
	*begin = strcmp(token, "BEGIN") == 0;

	// do nothing if this is not the first BULK call
	if(*begin == false) return BULK_OK;

	// "BEGIN" token present, skip "BEGIN" token
	(*argv) ++;
	(*argc) --;

	// lock GIL, verify that graph does not already exist
	RedisModuleKey *key = NULL;
	key = RedisModule_OpenKey(ctx, rs_graph_name, REDISMODULE_READ);
	RedisModule_CloseKey(key);

	if(key) {
		char *err;
		int rc __attribute__((unused));
		rc = asprintf(&err, "Graph with name '%s' cannot be created, "\
                      "as key '%s' already exists.", graphname, graphname);
		RedisModule_ReplyWithError(ctx, err);
		free(err);
		return BULK_FAIL;
	}

	return BULK_OK;
}

int Graph_BulkInsert(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	if(argc < 3) return RedisModule_WrongArity(ctx);

	// bulk-insert process a batch of node/edge creation generated by the
	// bulk-insert utility

	GraphContext *gc     = NULL;
	long long node_count = 0;  // number of declared nodes
	long long edge_count = 0;  // number of declared edges

	// get graph name
	argv += 1; // skip "GRAPH.BULK"
	RedisModuleString *rs_graph_name = *argv++;
	const char *graphname = RedisModule_StringPtrLen(rs_graph_name, NULL);
	argc -= 2; // skip "GRAPH.BULK [GRAPHNAME]"

	bool begin = false;
	if(_Graph_Bulk_Begin(ctx, &argv, &argc, rs_graph_name, graphname, &begin)
			!= BULK_OK) goto cleanup;

	gc = GraphContext_Retrieve(ctx, rs_graph_name, false, begin);

	// failed to retrieve GraphContext; an error has been emitted
	if(gc == NULL) goto cleanup;

	// read the user-provided counts for nodes and edges in the current query
	if(RedisModule_StringToLongLong(*argv++, &node_count) != REDISMODULE_OK) {
		RedisModule_ReplyWithError(ctx, "Error parsing node count.");
		goto cleanup;
	}

	if(RedisModule_StringToLongLong(*argv++, &edge_count) != REDISMODULE_OK) {
		RedisModule_ReplyWithError(ctx, "Error parsing relation count.");
		goto cleanup;
	}

	argc -= 2; // already read node count and edge count

	int rc = BulkInsert(ctx, gc, argv, argc, node_count, edge_count);

	if(rc == BULK_FAIL) {
		// if insertion failed, clean up keyspace and free added entities
		GraphContext_DecreaseRefCount(gc);
		RedisModuleKey *key = NULL;

		key = RedisModule_OpenKey(ctx, rs_graph_name, REDISMODULE_WRITE);
		RedisModule_DeleteKey(key);
		RedisModule_CloseKey(key);

		gc = NULL;
		goto cleanup;
	}

	// successful bulk commands should always modify slaves
	RedisModule_ReplicateVerbatim(ctx);

	// replay to caller
	char reply[1024];
	int len = snprintf(reply, 1024, "%llu nodes created, %llu edges created",
			node_count, edge_count);
	RedisModule_ReplyWithStringBuffer(ctx, reply, len);

cleanup:
	if(gc) GraphContext_DecreaseRefCount(gc);

	return REDISMODULE_OK;
}

