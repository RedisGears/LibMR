/* adlist.h - A generic doubly linked list implementation
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

#ifndef __ADLIST_H__
#define __ADLIST_H__

/* Node, List, and Iterator are the only data structures used currently. */

typedef struct mr_listNode {
    struct mr_listNode *prev;
    struct mr_listNode *next;
    void *value;
} mr_listNode;

typedef struct mr_listIter {
    mr_listNode *next;
    int direction;
} mr_listIter;

typedef struct mr_list {
    mr_listNode *head;
    mr_listNode *tail;
    void *(*dup)(void *ptr);
    void (*free)(void *ptr);
    int (*match)(void *ptr, void *key);
    unsigned long len;
} mr_list;

/* Functions implemented as macros */
#define mr_listLength(l) ((l)->len)
#define mr_listFirst(l) ((l)->head)
#define mr_listLast(l) ((l)->tail)
#define mr_listPrevNode(n) ((n)->prev)
#define mr_listNextNode(n) ((n)->next)
#define mr_listNodeValue(n) ((n)->value)

#define mr_listSetDupMethod(l,m) ((l)->dup = (m))
#define mr_listSetFreeMethod(l,m) ((l)->free = (m))
#define mr_listSetMatchMethod(l,m) ((l)->match = (m))

#define mr_listGetDupMethod(l) ((l)->dup)
#define mr_listGetFree(l) ((l)->free)
#define mr_listGetMatchMethod(l) ((l)->match)

/* Prototypes */
mr_list *mr_listCreate(void);
void mr_listRelease(mr_list *list);
void mr_listEmpty(mr_list *list);
mr_list *mr_listAddNodeHead(mr_list *list, void *value);
mr_list *mr_listAddNodeTail(mr_list *list, void *value);
mr_list *mr_listInsertNode(mr_list *list, mr_listNode *old_node, void *value, int after);
void mr_listDelNode(mr_list *list, mr_listNode *node);
mr_listIter *mr_listGetIterator(mr_list *list, int direction);
mr_listNode *mr_listNext(mr_listIter *iter);
void mr_listReleaseIterator(mr_listIter *iter);
mr_list *mr_listDup(mr_list *orig);
mr_listNode *mr_listSearchKey(mr_list *list, void *key);
mr_listNode *mr_listIndex(mr_list *list, long index);
void mr_listRewind(mr_list *list, mr_listIter *li);
void mr_listRewindTail(mr_list *list, mr_listIter *li);
void mr_listRotate(mr_list *list);
void mr_listJoin(mr_list *l, mr_list *o);

/* Directions for iterators */
#define AL_START_HEAD 0
#define AL_START_TAIL 1

#endif /* __ADLIST_H__ */
