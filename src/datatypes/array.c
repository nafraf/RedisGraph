/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "array.h"
#include "../util/arr.h"
#include <limits.h>
#include "xxhash.h"

SIValue SIArray_New(uint32_t initialCapacity) {
	SIValue siarray;
	siarray.array = array_new(SIValue, initialCapacity);
	siarray.type = T_ARRAY;
	siarray.allocation = M_SELF;
	return siarray;
}

void SIArray_Append(SIValue *siarray, SIValue value) {
	// clone and persist incase of pointer values
	SIValue clone = SI_CloneValue(value);
	// append
	array_append(siarray->array, clone);
}

SIValue SIArray_Get(SIValue siarray, uint32_t index) {
	// check index
	if(index >= SIArray_Length(siarray)) return SI_NullVal();
	return SI_ShareValue(siarray.array[index]);
}

uint32_t SIArray_Length(SIValue siarray) {
	return array_len(siarray.array);
}

bool SIArray_Contains(SIValue siarray, SIValue value) {
	uint array_len = SIArray_Length(siarray);
	for(uint i = 0; i < array_len; i++) {
		SIValue elem = SIArray_Get(siarray, i);
		if(SI_TYPE(elem) & SI_TYPE(value)) {
			int res = SIValue_Compare(elem, value, NULL);
			if(res == 0) return true;
		}
	}
	return false;
}

bool SIArray_ContainsType(SIValue siarray, SIType t) {
	uint array_len = SIArray_Length(siarray);
	for(uint i = 0; i < array_len; i++) {
		SIValue elem = SIArray_Get(siarray, i);
		if(SI_TYPE(elem) & t) return true;

		// recursively check nested arrays
		if(SI_TYPE(elem) == T_ARRAY) {
			bool type_is_nested = SIArray_ContainsType(elem, t);
			if(type_is_nested) return true;
		}
	}

	return false;
}

bool SIArray_AllOfType(SIValue siarray, SIType t) {
	uint array_len = SIArray_Length(siarray);
	for(uint i = 0; i < array_len; i++) {
		SIValue elem = SIArray_Get(siarray, i);
		if((SI_TYPE(elem) & t) == 0) return false;
	}

	return true;
}

SIValue SIArray_Clone(SIValue siarray) {
	uint arrayLen = SIArray_Length(siarray);
	SIValue newArray = SIArray_New(arrayLen);
	for(uint i = 0; i < arrayLen; i++) {
		SIArray_Append(&newArray, SIArray_Get(siarray, i));
	}
	return newArray;
}

void SIArray_ToString(SIValue list, char **buf, size_t *bufferLen, size_t *bytesWritten) {
	if(*bufferLen - *bytesWritten < 64) {
		*bufferLen += 64;
		*buf = rm_realloc(*buf, sizeof(char) * *bufferLen);
	}
	// open array with "["
	*bytesWritten += snprintf(*buf + *bytesWritten, *bufferLen, "[");
	uint arrayLen = SIArray_Length(list);
	for(uint i = 0; i < arrayLen; i ++) {
		// write the next value
		SIValue_ToString(SIArray_Get(list, i), buf, bufferLen, bytesWritten);
		// if it is not the last element, add ", "
		if(i != arrayLen - 1) {
			if(*bufferLen - *bytesWritten < 64) {
				*bufferLen += 64;
				*buf = rm_realloc(*buf, sizeof(char) * *bufferLen);
			}
			*bytesWritten += snprintf(*buf + *bytesWritten, *bufferLen, ", ");
		}
	}
	if(*bufferLen - *bytesWritten < 2) {
		*bufferLen += 2;
		*buf = rm_realloc(*buf, sizeof(char) * *bufferLen);
	}
	// close array with "]"
	*bytesWritten += snprintf(*buf + *bytesWritten, *bufferLen, "]");
}

// this method referenced by Java ArrayList.hashCode() method, which takes
// into account the hasing of nested values
XXH64_hash_t SIArray_HashCode(SIValue siarray) {
	SIType t = T_ARRAY;
	XXH64_hash_t hashCode = XXH64(&t, sizeof(t), 0);

	uint arrayLen = SIArray_Length(siarray);
	for(uint i = 0; i < arrayLen; i++) {
		SIValue value = siarray.array[i];
		hashCode = 31 * hashCode + SIValue_HashCode(value);
	}

	return hashCode;
}

void SIArray_Free(SIValue siarray) {
	uint arrayLen = SIArray_Length(siarray);
	for(uint i = 0; i < arrayLen; i++) {
		SIValue value = siarray.array[i];
		SIValue_Free(value);
	}
	array_free(siarray.array);
}

SIValue SIArray_Flatten(SIValue siarray, uint32_t levels) {
	uint arrayLen 		= SIArray_Length(siarray);
	SIValue newArray 	= SIArray_New(arrayLen);
	for(uint i = 0; i < arrayLen; i++) {
		SIValue value = siarray.array[i];
		if ((SI_TYPE(value) != T_ARRAY) || levels < 1) {
			SIArray_Append(&newArray, value);
		} else {
			SIValue innerArray = SIArray_Flatten(value, levels - 1);
			uint innerArrayLen = SIArray_Length(innerArray);
			for(uint j = 0; j < innerArrayLen; j++) {
				SIArray_Append(&newArray, innerArray.array[j]);
			}	
		}
	}
	return newArray;
}

SIValue SIArray_Dedup(SIValue siarray) {
	SIValue newArray 	= SIArray_New(0);
	uint32_t arrayLen 	= SIArray_Length(siarray);

	if(arrayLen == 0) return newArray;

	for(uint i = 0; i < arrayLen; i++) {
		SIValue elem	= siarray.array[i];

		if(SIArray_Contains(newArray, elem) == false) {
			SIArray_Append(&newArray, elem);
		}
	}
	return newArray;
}