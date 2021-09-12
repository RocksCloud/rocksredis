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
extern "C" std::string getDBTypepath(int id,int type);
extern "C" void getDBKeyStrpath(int id,char * key,char *gpath);
extern "C" int RRhscan(struct client *c,int id,int type, char *key,unsigned long cursor,unsigned long count,int opt,char *cmd);
extern "C" const char * RRhget(int id,int type,char *key,char * field);
extern "C" int RRhdel(int id,int type,char *key,char * field);
extern "C" int RRhexist(int id,int type,char *key,char * field);
extern "C" rrdbObj *RRdbGet(const char *db_key);
extern "C" void RRreadOptions(struct ReadOptions *op);
extern "C" void RRwriteOptions(struct WriteOptions *op);

/****************************************
# quote rocksredis  related content
****************************************/
extern "C" void mstimeTostr(char *time);
extern  "C" long int getSdsLen(sds s);

/****************************************
# magnet_string content
****************************************/

extern "C" int RRset(int id,int type,char *key,char *val);
extern "C" const char * RRget(int id,int type, char *key);
extern "C" int RRstringDel(int id,int type,char *key);

int RRstringDel(int id,int type,char *key){
    Options options;
    Status s;
    DB* db;
    options.create_if_missing = true;
    std::string keypath=rsmagnet.rocksredis_db_dir+getDBTypepath(id,type);
    std::string dbpath= keypath+"dbstring";
    s = DB::Open(options, dbpath, &db);
    if(!s.ok()){
        delete db;
        printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.ToString().c_str()); 
        return s.code();
    }

    db->Delete(WriteOptions(), Slice(key));
    delete db;
    return s.Code::kOk;
}

int RRset(int id,int type,char *key,char *val){
    Options options;
    Status s;
    DB* db;
    options.create_if_missing = true;
    std::string keypath=rsmagnet.rocksredis_db_dir+getDBTypepath(id,type);
    std::string dbpath= keypath+"dbstring";
    s = DB::Open(options, dbpath, &db);
    if(!s.ok()){
        delete db;
        printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.ToString().c_str()); 
        return s.code();
    }
    WriteOptions wp= WriteOptions();
    RRwriteOptions(&wp);
    s = db->Put(wp,Slice(key), Slice(val));
  
    delete db;
    return s.Code::kOk;
}


const char * RRget(int id,int type, char *key){
    Options options;
    Status s;
    DB* db;
    options.create_if_missing = true;
    std::string keypath=rsmagnet.rocksredis_db_dir+getDBTypepath(id,type);
    std::string dbpath= keypath+"dbstring";
    s = DB::Open(options, dbpath, &db);
    if(!s.ok()){
        delete db;
        printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.ToString().c_str()); 
        if(s.code()==s.Code::kInvalidArgument){//db not exist
            return CkInvalidArgument;
        }else if(s.code()==s.Code::kBusy){
            return CkBusy;
        }else if(s.code()==s.Code::kIOError){
             return CkIOError;
        }
    }

    ReadOptions rp=ReadOptions();
    RRreadOptions(&rp);
    std::string value;
    s=db->Get(rp ,Slice(key) ,&value);
    delete db;
    if(value == ""){
     return "";
    }else{
      return value.c_str();
    }
}



