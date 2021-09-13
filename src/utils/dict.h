/* Hash Tables Implementation.
 *
 * This file implements in-memory hash tables with insert/del/replace/find/
 * get-random-element operations. Hash tables will auto-resize if needed
 * tables of power of two in size are used, collisions are handled by
 * chaining. See the source code for more information... :)
 *
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdint.h>
#include <stddef.h>

#ifndef __DICT_H
#define __DICT_H

#define DICT_OK 0
#define DICT_ERR 1

/* Unused arguments generate annoying warnings... */
#define DICT_NOTUSED(V) ((void) V)

typedef struct mr_dictEntry {
    void *key;
    union {
        void *val;
        uint64_t u64;
        int64_t s64;
        double d;
    } v;
    struct mr_dictEntry *next;
} mr_dictEntry;

typedef struct mr_dictType {
    uint64_t (*hashFunction)(const void *key);
    void *(*keyDup)(void *privdata, const void *key);
    void *(*valDup)(void *privdata, const void *obj);
    int (*keyCompare)(void *privdata, const void *key1, const void *key2);
    void (*keyDestructor)(void *privdata, void *key);
    void (*valDestructor)(void *privdata, void *obj);
} mr_dictType;

/* This is our hash table structure. Every dictionary has two of this as we
 * implement incremental rehashing, for the old to the new table. */
typedef struct mr_dictht {
    mr_dictEntry **table;
    unsigned long size;
    unsigned long sizemask;
    unsigned long used;
} mr_dictht;

typedef struct mr_dict {
    mr_dictType *type;
    void *privdata;
    mr_dictht ht[2];
    long rehashidx; /* rehashing not in progress if rehashidx == -1 */
    unsigned long iterators; /* number of iterators currently running */
} mr_dict;

/* If safe is set to 1 this is a safe iterator, that means, you can call
 * dictAdd, dictFind, and other functions against the dictionary even while
 * iterating. Otherwise it is a non safe iterator, and only dictNext()
 * should be called while iterating. */
typedef struct mr_dictIterator {
    mr_dict *d;
    long index;
    int table, safe;
    mr_dictEntry *entry, *nextEntry;
    /* unsafe iterator fingerprint for misuse detection. */
    long long fingerprint;
} mr_dictIterator;

typedef void (mr_dictScanFunction)(void *privdata, const mr_dictEntry *de);
typedef void (mr_dictScanBucketFunction)(void *privdata, mr_dictEntry **bucketref);

/* This is the initial size of every hash table */
#define DICT_HT_INITIAL_SIZE     4

/* ------------------------------- Macros ------------------------------------*/
#define mr_dictFreeVal(d, entry) \
    if ((d)->type->valDestructor) \
        (d)->type->valDestructor((d)->privdata, (entry)->v.val)

#define mr_dictSetVal(d, entry, _val_) do { \
    if ((d)->type->valDup) \
        (entry)->v.val = (d)->type->valDup((d)->privdata, _val_); \
    else \
        (entry)->v.val = (_val_); \
} while(0)

#define mr_dictSetSignedIntegerVal(entry, _val_) \
    do { (entry)->v.s64 = _val_; } while(0)

#define mr_dictSetUnsignedIntegerVal(entry, _val_) \
    do { (entry)->v.u64 = _val_; } while(0)

#define mr_dictSetDoubleVal(entry, _val_) \
    do { (entry)->v.d = _val_; } while(0)

#define mr_dictFreeKey(d, entry) \
    if ((d)->type->keyDestructor) \
        (d)->type->keyDestructor((d)->privdata, (entry)->key)

#define mr_dictSetKey(d, entry, _key_) do { \
    if ((d)->type->keyDup) \
        (entry)->key = (d)->type->keyDup((d)->privdata, _key_); \
    else \
        (entry)->key = (_key_); \
} while(0)

#define mr_dictCompareKeys(d, key1, key2) \
    (((d)->type->keyCompare) ? \
        (d)->type->keyCompare((d)->privdata, key1, key2) : \
        (key1) == (key2))

#define mr_dictHashKey(d, key) (d)->type->hashFunction(key)
#define mr_dictGetKey(he) ((he)->key)
#define mr_dictGetVal(he) ((he)->v.val)
#define mr_dictGetSignedIntegerVal(he) ((he)->v.s64)
#define mr_dictGetUnsignedIntegerVal(he) ((he)->v.u64)
#define mr_dictGetDoubleVal(he) ((he)->v.d)
#define mr_dictSlots(d) ((d)->ht[0].size+(d)->ht[1].size)
#define mr_dictSize(d) ((d)->ht[0].used+(d)->ht[1].used)
#define mr_dictIsRehashing(d) ((d)->rehashidx != -1)

/* API */
mr_dict *mr_dictCreate(mr_dictType *type, void *privDataPtr);
int mr_dictExpand(mr_dict *d, unsigned long size);
int mr_dictAdd(mr_dict *d, void *key, void *val);
mr_dictEntry *mr_dictAddRaw(mr_dict *d, void *key, mr_dictEntry **existing);
mr_dictEntry *mr_dictAddOrFind(mr_dict *d, void *key);
int mr_dictReplace(mr_dict *d, void *key, void *val);
int mr_dictDelete(mr_dict *d, const void *key);
mr_dictEntry *mr_dictUnlink(mr_dict *ht, const void *key);
void mr_dictFreeUnlinkedEntry(mr_dict *d, mr_dictEntry *he);
void mr_dictRelease(mr_dict *d);
mr_dictEntry * mr_dictFind(mr_dict *d, const void *key);
void *mr_dictFetchValue(mr_dict *d, const void *key);
int mr_dictResize(mr_dict *d);
mr_dictIterator *mr_dictGetIterator(mr_dict *d);
mr_dictIterator *mr_dictGetSafeIterator(mr_dict *d);
mr_dictEntry *mr_dictNext(mr_dictIterator *iter);
void mr_dictReleaseIterator(mr_dictIterator *iter);
mr_dictEntry *mr_dictGetRandomKey(mr_dict *d);
unsigned int mr_dictGetSomeKeys(mr_dict *d, mr_dictEntry **des, unsigned int count);
void mr_dictGetStats(char *buf, size_t bufsize, mr_dict *d);
uint64_t mr_dictGenHashFunction(const void *key, int len);
uint64_t mr_dictGenCaseHashFunction(const unsigned char *buf, int len);
void mr_dictEmpty(mr_dict *d, void(callback)(void*));
void mr_dictEnableResize(void);
void mr_dictDisableResize(void);
int mr_dictRehash(mr_dict *d, int n);
int mr_dictRehashMilliseconds(mr_dict *d, int ms);
void mr_dictSetHashFunctionSeed(uint8_t *seed);
uint8_t *mr_dictGetHashFunctionSeed(void);
unsigned long mr_dictScan(mr_dict *d, unsigned long v, mr_dictScanFunction *fn, mr_dictScanBucketFunction *bucketfn, void *privdata);
uint64_t mr_dictGetHash(mr_dict *d, const void *key);
mr_dictEntry **mr_dictFindEntryRefByPtrAndHash(mr_dict *d, const void *oldptr, uint64_t hash);

extern mr_dictType mr_dictTypeHeapStrings;
extern mr_dictType mr_dictTypeHeapStringsVals;

#endif /* __DICT_H */
