/*
 * safe_dict.h
 *
 *  Created on: Aug 25, 2021
 *      Author: root
 */

#ifndef SRC_UTILS_SAFE_DICT_H_
#define SRC_UTILS_SAFE_DICT_H_

#include "dict.h"
#include <pthread.h>

typedef struct rm_SafeDict {
    mr_dict* dict;
    pthread_mutex_t lock;
} rm_SafeDict;

rm_SafeDict* mr_SafeDictCreate(mr_dictType *type, void *privDataPtr);
int mr_SafeDictadd(rm_SafeDict *d, void *key, void *val);
void* mr_SafeDictFetch(rm_SafeDict *d, void *key);

#endif /* SRC_UTILS_SAFE_DICT_H_ */
