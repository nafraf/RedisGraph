/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "map_funcs.h"
#include "RG.h"
#include "../func_desc.h"
#include "../../errors.h"
#include "../../util/arr.h"
#include "../../datatypes/map.h"
#include "../../datatypes/array.h"
#include "../string_funcs/string_funcs.h"
#include "../../graph/entities/graph_entity.h"

SIValue AR_TOMAP(SIValue *argv, int argc, void *private_data) {
	/* create a new SIMap object
	 * expecting an even number of arguments
	 * argv[even] = key
	 * argv[odd] = value */

	// validate number of arguments
	if(argc % 2 != 0) {
		ErrorCtx_RaiseRuntimeException("map expects even number of elements");
	}

	SIValue map = SI_Map(argc / 2);

	for(int i = 0; i < argc; i += 2) {
		SIValue key = argv[i];
		SIValue val = argv[i + 1];

		// make sure key is a string
		if(!(SI_TYPE(key) & T_STRING)) {
			Error_SITypeMismatch(key, T_STRING);
			break;
		}

		Map_Add(&map, key, val);
	}

	return map;
}

SIValue AR_KEYS(SIValue *argv, int argc, void *private_data) {
	ASSERT(argc == 1);
	switch(SI_TYPE(argv[0])) {
		case T_NULL:
			return SI_NullVal();
		case T_NODE:
		case T_EDGE:
			return GraphEntity_Keys(argv[0].ptrval);
		case T_MAP:
			return Map_Keys(argv[0]);
		default:
			ASSERT(false);
	}
	return SI_NullVal();
}

SIValue AR_PROPERTIES(SIValue *argv, int argc, void *private_data) {
	ASSERT(argc == 1);
	switch(SI_TYPE(argv[0])) {
		case T_NULL:
			return SI_NullVal();
		case T_NODE:
		case T_EDGE:
			return GraphEntity_Properties(argv[0].ptrval);
		case T_MAP:
			return argv[0];
		default:
			ASSERT(false);
	}
	return SI_NullVal();
}

// Receives two maps and merges them
SIValue AR_MERGEMAP(SIValue *argv, int argc, void *private_data) {
	ASSERT(argc == 2);

	SIValue map0 = argv[0];
	SIValue map1 = argv[1];

	if ((SI_TYPE(map0) & T_NULL) && (SI_TYPE(map1) & T_NULL)) {
		return SI_NullVal();
	} else if (SI_TYPE(map0) & T_NULL) {
		return map1;
	} else if (SI_TYPE(map1) & T_NULL) {
		return map0;
	} else {
		uint keyCount0 = Map_KeyCount(map0);
		SIValue map = Map_Clone(map1);
		for(int i = 0; i < keyCount0; i++) {
			SIValue key;
			SIValue value;
			Map_GetIdx(map0, i, &key, &value);
			Map_Add(&map, key, value);
		}
		return map;
	}
}

// map.fromList(baseMap, [keyStr, val, ...]) → map
SIValue AR_FROMLIST(SIValue *argv, int argc, void *private_data) {
	ASSERT(argc == 2);
	ASSERT(SI_TYPE(argv[0]) == T_MAP && SI_TYPE(argv[1]) == T_ARRAY);
	
	uint32_t arrayLen = SIArray_Length(argv[1]);
	if (arrayLen == 0) {
		return argv[0];
	} else if(arrayLen % 2 != 0) {
		ErrorCtx_RaiseRuntimeException("List containing keyStr and values expects even number of elements");
	}

	SIValue list = argv[1];
	SIValue map  = Map_Clone(argv[0]);

	for(int i = 0; i < arrayLen; i += 2) {
		SIValue key = SIArray_Get(list, i);
		// if key is null - the pair is skipped
		if(SI_TYPE(key) == T_NULL) {
			continue;
		}
		// make sure key is a string
		if(SI_TYPE(key) != T_STRING) {
			SIValue keystr = AR_TOSTRING(&key, 1, NULL);
			if(SI_TYPE(keystr) == T_NULL) {
				Error_SITypeMismatch(key, T_STRING);
				break;
			}
			SIValue_Free(key);
			key = SI_CloneValue(keystr);
		}
		SIValue val = SIArray_Get(list, i+1);
		// update map
		if(SI_TYPE(val) == T_NULL) {
			Map_Remove(map, key);
		} else {
			Map_Add(&map, key, val);
		}
	}
	return map;
}

// map.fromTwoLists(baseMap, [keyStr, ...], [val, ...]) → map
SIValue AR_FROMTWOLISTS(SIValue *argv, int argc, void *private_data) {
	ASSERT(argc == 3);
	ASSERT(SI_TYPE(argv[0]) == T_MAP && SI_TYPE(argv[1]) == T_ARRAY && SI_TYPE(argv[2]) == T_ARRAY);

	uint32_t keyCount = SIArray_Length(argv[1]);
	if(keyCount != SIArray_Length(argv[2])) {
		ErrorCtx_RaiseRuntimeException("Number of keys must be equal to the number of values");
	}

	SIValue keys = argv[1];
	SIValue vals = argv[2];
	SIValue map  = Map_Clone(argv[0]);

	for(int i = 0; i < keyCount; i++) {
		SIValue key = SIArray_Get(keys, i);
		// if key is null - the pair is skipped
		if(SI_TYPE(key) == T_NULL) {
			continue;
		}
		// make sure key is a string
		if(SI_TYPE(key) != T_STRING) {
			SIValue keystr = AR_TOSTRING(&key, 1, NULL);
			if(SI_TYPE(keystr) == T_NULL) {
				Error_SITypeMismatch(key, T_STRING);
				break;
			}
			SIValue_Free(key);
			key = SI_CloneValue(keystr);
		}
		SIValue val = SIArray_Get(vals, i);
		// update map
		if(SI_TYPE(val) == T_NULL) {
			Map_Remove(map, key);
		} else {
			Map_Add(&map, key, val);
		}
	}
	return map;
}

// map.fromPairs(baseMap, [[keyStr, val], ...]) → map
SIValue AR_FROMPAIRS(SIValue *argv, int argc, void *private_data) {
	ASSERT(argc == 3);

	return SI_NullVal();
}

void Register_MapFuncs() {
	SIType *types;
	SIType ret_type;
	AR_FuncDesc *func_desc;

	types = array_new(SIType, 1);
	array_append(types, SI_ALL);
	ret_type = T_MAP;
	func_desc = AR_FuncDescNew("tomap", AR_TOMAP, 0, VAR_ARG_LEN, types, ret_type, true, true);
	AR_RegFunc(func_desc);

	types = array_new(SIType, 1);
	array_append(types, T_NULL | T_MAP | T_NODE | T_EDGE);
	ret_type = T_NULL | T_ARRAY;
	func_desc = AR_FuncDescNew("keys", AR_KEYS, 1, 1, types, ret_type, false, true);
	AR_RegFunc(func_desc);

	types = array_new(SIType, 1);
	array_append(types, T_NULL | T_MAP | T_NODE | T_EDGE);
	ret_type = T_NULL | T_MAP;
	func_desc = AR_FuncDescNew("properties", AR_PROPERTIES, 1, 1, types, ret_type, false, true);
	AR_RegFunc(func_desc);

	types = array_new(SIType, 2);
	array_append(types, T_NULL | T_MAP);
	array_append(types, T_NULL | T_MAP);
	ret_type = T_NULL | T_MAP;
	func_desc = AR_FuncDescNew("merge_maps", AR_MERGEMAP, 2, 2, types, ret_type, true, true);
	AR_RegFunc(func_desc);

	types = array_new(SIType, 2);
	array_append(types, T_MAP);
	array_append(types, T_ARRAY);
	ret_type = T_NULL | T_MAP;
	func_desc = AR_FuncDescNew("map.fromList", AR_FROMLIST, 2, 2, types, ret_type, false, true);
	AR_RegFunc(func_desc);

	types = array_new(SIType, 3);
	array_append(types, T_MAP);
	array_append(types, T_ARRAY);
	array_append(types, T_ARRAY);
	ret_type = T_NULL | T_MAP;
	func_desc = AR_FuncDescNew("map.fromTwoLists", AR_FROMTWOLISTS, 3, 3, types, ret_type, false, true);
	AR_RegFunc(func_desc);

	types = array_new(SIType, 2);
	array_append(types, T_MAP);
	array_append(types, T_ARRAY);
	ret_type = T_NULL | T_MAP;
	func_desc = AR_FuncDescNew("map.fromPairs", AR_FROMPAIRS, 2, 2, types, ret_type, false, true);
	AR_RegFunc(func_desc);	
}

