#include <cstdio>
#include <string>
#include <vector>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <iostream>
#include <sys/stat.h>
#include <stdlib.h> 
#include <unistd.h>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/cache.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/options_util.h"


using namespace rocksdb;
/****************************************
# quote redis related content
****************************************/
typedef char *sds;

#define OBJ_STRING 0    /* String object. */
#define OBJ_LIST 1      /* List object. */
#define OBJ_SET 2       /* Set object. */
#define OBJ_ZSET 3      /* Sorted set object. */
#define OBJ_HASH 4      /* Hash object. */

#define OBJ_ENCODING_RAW 0     /* Raw representation */
#define OBJ_ENCODING_INT 1     /* Encoded as integer */
#define OBJ_ENCODING_HT 2      /* Encoded as hash table */
#define OBJ_ENCODING_ZIPMAP 3  /* Encoded as zipmap */
#define OBJ_ENCODING_LINKEDLIST 4 /* No longer used: old list encoding. */
#define OBJ_ENCODING_ZIPLIST 5 /* Encoded as ziplist */
#define OBJ_ENCODING_INTSET 6  /* Encoded as intset */
#define OBJ_ENCODING_SKIPLIST 7  /* Encoded as skiplist */
#define OBJ_ENCODING_EMBSTR 8  /* Embedded sds string encoding */
#define OBJ_ENCODING_QUICKLIST 9 /* Encoded as linked list of ziplists */
#define OBJ_ENCODING_STREAM 10 /* Encoded as a radix tree of listpacks */
#define LRU_BITS 24
#define LRU_CLOCK_MAX ((1<<LRU_BITS)-1) /* Max value of obj->lru */
#define LRU_CLOCK_RESOLUTION 1000 /* LRU clock resolution in ms */

#define OBJ_SHARED_REFCOUNT INT_MAX     /* Global object never destroyed. */
#define OBJ_STATIC_REFCOUNT (INT_MAX-1) /* Object allocated in the stack. */
#define OBJ_FIRST_SPECIAL_REFCOUNT OBJ_STATIC_REFCOUNT

typedef struct redisObject {
    unsigned type:4;
    unsigned encoding:4;
    unsigned lru:LRU_BITS; /* LRU time (relative to global lru_clock) or
                            * LFU data (least significant 8 bits frequency
                            * and most significant 16 bits access time). */
    int refcount;
    void *ptr;
} robj;

#define initStaticStringObject(_var,_ptr) do { \
    _var.refcount = OBJ_STATIC_REFCOUNT; \
    _var.type = OBJ_STRING; \
    _var.encoding = OBJ_ENCODING_RAW; \
    _var.ptr = _ptr; \
} while(0)

extern "C" {
    struct client;
    #include "../../src/adlist.h"
    #include "../../src/dict.h"
}

#include "magnet.h"

extern "C" void freeStringObject(robj *o);
extern "C" void addReplyBulk(client *c, robj *obj);
extern "C" sds sdsnew(const char *init);
extern "C" void addReplyArrayLen(client *c, long length);
extern "C" void decrRefCount(robj *o);
extern "C" void addReplyMapLen(client *c, long length);
extern "C" void addReplyBulkLongLong(client *c, long long ll);
extern "C"  void addReplyBulkCBuffer(client *c, const void *p, size_t len);

/****************************************
# quote magnet_db related content
****************************************/
extern "C" std::string getDBTypeKeyPath(int id,int type,char *key);
extern "C" void getDBKeyStrpath(int id,char * key,char *gpath);
extern "C" int RRhscan(struct client *c,int id,int type, char *key,unsigned long cursor,unsigned long count,int opt,char *cmd);
extern "C" const char * RRhget(int id,int type,char *key,char * field);
extern "C" int RRhdel(int id,int type,char *key,char * field);
extern "C" int RRhexist(int id,int type,char *key,char * field);
extern "C" rrdbObj *RRdbGet(const char *db_key);

/****************************************
# quote rocksredis  related content
****************************************/
extern "C" void mstimeTostr(char *time);
extern  "C" long int getSdsLen(sds s);






