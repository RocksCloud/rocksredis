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
extern "C" void RRreadOptions(struct ReadOptions *op);
/****************************************
# quote rocksredis  related content
****************************************/
extern "C" void mstimeTostr(char *time);
extern  "C" long int getSdsLen(sds s);


/****************************************
# quote magnet_set  related content
****************************************/
extern "C" int RRsadd(int id,int type,char *key,char *val);
extern "C" const char * RRsismember(int id,int type,char *key,char * field);
extern "C" int RRsmembers(struct client *c,int id,int type, char *key);
extern "C" int RRsdel(int id,int type,char *key,std::string field);
extern "C" int RRspop(struct client *c,int id,int type, char *key,int count);
extern "C" void RRUpdateSetCustom(int id,int type,char *key,int op);
extern "C" int RRsscan(struct client *c,int id,int type, char *key,unsigned long cursor,unsigned long count,int opt,char *cmd);
extern "C" int RRsrem(int id,int type, char *key,char *field);


void RRUpdateSetCustom(int id,int type,char *key,int op){
    Options options;
    Status s;
    DB* db;
    options.create_if_missing = true;
    std::string keypath=rsmagnet.rocksredis_db_dir+getDBTypeKeyPath(id,type,key);
    std::string custompath= keypath+"/custom";
    s = DB::Open(options, custompath, &db);
    ReadOptions rp=ReadOptions();
    std::string value;
    s=db->Get(rp ,Slice(key) ,&value);
//    if(value == "")  value = "1";
    if(!s.ok()){
        printf(" %s:%d: ERR:%s\n",__FUNCTION__, __LINE__,s.ToString().c_str()); 
    }
    char count[128];
    char * pEnd;
    memset(count,0,sizeof(count));
    unsigned long int opcount= strtoul(value.c_str(),&pEnd,10);
    if(op==RR_OP_INCREASE)
        opcount++;
    else if(op==RR_OP_REDUCE && opcount>0)
        opcount--;
        
    sprintf(count,"%ld",opcount);
    db->Put(WriteOptions(), Slice(key), Slice(count));
    delete db;
}
int RRsrem(int id,int type, char *key,char *field){
    Options options;
    Status s;
    DB* db;
    options.create_if_missing = true;
    std::string keypath=rsmagnet.rocksredis_db_dir+getDBTypeKeyPath(id,type,key);
    std::string dbpath= keypath+"/"+key;
    s = DB::Open(options, dbpath, &db);
    if(!s.ok()){
        printf(" %s:%d: ERR:%s\n",__FUNCTION__, __LINE__,s.ToString().c_str()); 
        return 0;
    }

    db->Delete(WriteOptions(), Slice(field));
    if(!s.ok()){
        printf(" %s:%d: ERR:%s\n",__FUNCTION__, __LINE__,s.ToString().c_str()); 
        return 0;
    }

    delete db;
    RRUpdateSetCustom(id,type,key,RR_OP_REDUCE);
    return 1;

}
int RRspop(struct client *c,int id,int type, char *key,int count){
    int ret;
    Iterator* iterator;
    Options options;
    DB* db;
    Status s;
    options.create_if_missing=true;
    ReadOptions rp=ReadOptions();
    rp.total_order_seek=true;
    std::string db_key=rsmagnet.rocksredis_db_dir+getDBTypeKeyPath(id,type,key)+"/"+key;
    s= DB::Open(options, db_key, &db);
    if(!s.ok()){
      printf(" %s:%d: ERR:%s\n",__FUNCTION__, __LINE__,s.ToString().c_str()); 
      return 0;
    }
    iterator=db->NewIterator(rp);  
    std::string row;
    iterator->SeekToFirst();
    long int ii=0,size;
    for(;iterator->Valid();iterator->Next()) {
        row = iterator->key().ToString();
        robj cf_kobj;
        initStaticStringObject(cf_kobj, sdsnew(row.c_str()));
        addReplyBulk(c, &cf_kobj);
        
        if(cf_kobj.type==OBJ_STRING)   freeStringObject(&cf_kobj);
        db->Delete(WriteOptions(), Slice(row));
        RRUpdateSetCustom(id,type,key,RR_OP_REDUCE);
        ii++;
        if(ii==count) break;
    }

    delete iterator;
    delete db;
    return 1;
}

int RRsdel(int id,int type,char *key,std::string field){
    Options options;
    Status s;
    DB* db;
    options.create_if_missing = true;
    std::string keypath=rsmagnet.rocksredis_db_dir+getDBTypeKeyPath(id,type,key);
    std::string dbpath= keypath+"/"+key;
    s = DB::Open(options, dbpath, &db);
    if(!s.ok()){
        delete db;
        printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.code(),s.ToString().c_str());
        return s.code();
    }

    db->Delete(WriteOptions(), Slice(field));
    delete db;
    
    RRUpdateSetCustom(id,type,key,RR_OP_REDUCE);
    return s.Code::kOk;
}
int RRsmembers(struct client *c,int id,int type, char *key){
    int ret;
    Iterator* iterator;
    Options options;
    DB* db;
    Status s;
    options.create_if_missing=true;
    ReadOptions rp=ReadOptions();
    rp.total_order_seek=true;
    std::string db_key=rsmagnet.rocksredis_db_dir+getDBTypeKeyPath(id,type,key)+"/"+key;
    s= DB::Open(options, db_key, &db);
    if(!s.ok()){
      delete db;
      printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.code(),s.ToString().c_str()); 
      return s.code();
    }
    iterator=db->NewIterator(rp);  
    std::string row;
    iterator->SeekToFirst();
    long int ii=0,size;
    for(;iterator->Valid();iterator->Next()) {
        row = iterator->key().ToString();
        robj cf_kobj;
        initStaticStringObject(cf_kobj, sdsnew(row.c_str()));
        addReplyBulk(c, &cf_kobj);
        if(cf_kobj.type==OBJ_STRING)   freeStringObject(&cf_kobj);
    }

    delete iterator;
    delete db;
    return s.Code::kOk;
}


const char * RRsismember(int id,int type,char *key,char * field){
    Options options;
    Status s;
    DB* db;
    options.create_if_missing = true;
    std::string keypath=rsmagnet.rocksredis_db_dir+getDBTypeKeyPath(id,type,key);
    std::string dbpath= keypath+"/"+key;
    s = DB::Open(options, dbpath, &db);
    if(!s.ok()){
       delete db;
       printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.code(),s.ToString().c_str()); 
        if(s.code()==s.Code::kInvalidArgument){//db not exist
            return CkInvalidArgument;
        }else if(s.code()==s.Code::kBusy){
            return CkBusy;
        }else if(s.code()==s.Code::kIOError){
             return CkIOError;
        }
    }

    ReadOptions rp=ReadOptions();
    std::string value;
    s=db->Get(rp ,Slice(field) ,&value);
    
    delete db;
    if(value == ""){
      return "0";
    }else{
      return "1";
    }

}

int RRsscan(struct client *c,int id,int type, char *key,unsigned long cursor,unsigned long count,int opt,char *cmd){
    Iterator* iterator;
    Options options;
    DB* db;
    Status s;
    options.create_if_missing=true;
    ReadOptions rp=ReadOptions();
    rp.total_order_seek=true;
    std::string db_key=rsmagnet.rocksredis_db_dir+getDBTypeKeyPath(id,type,key)+"/"+key;
    s= DB::Open(options, db_key, &db);
    if(!s.ok()){
       delete db;
       printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.code(),s.ToString().c_str()); 
       return s.code();
    }
    iterator=db->NewIterator(rp);  
    std::string row, val;
    iterator->SeekToFirst();
    
    if(!strcasecmp(cmd,"sscan")){
        addReplyArrayLen(c, 2);
        addReplyBulkLongLong(c,cursor);
        addReplyArrayLen(c, count);
    } 
    long int ii=0,size;
    for(;iterator->Valid();iterator->Next()) {
       if(ii >= cursor) {
           break;
       }
       ii++;
    }
    for(size=0;size<count;size++){
       if(iterator->Valid()){
            row = iterator->key().ToString();
            robj cf_kobj;
            initStaticStringObject(cf_kobj, sdsnew(row.c_str()));
            addReplyBulk(c, &cf_kobj);
            if(cf_kobj.type==OBJ_STRING)   freeStringObject(&cf_kobj);
       }else break;
       iterator->Next();
    }
    delete iterator;
    delete db;
    
    return s.Code::kOk;

}
int RRsadd(int id,int type,char *key,char *val){
    Options options;
    Status s;
    DB* db;
    options.create_if_missing = true;
    std::string keypath=rsmagnet.rocksredis_db_dir+getDBTypeKeyPath(id,type,key);
    std::string dbpath= keypath+"/"+key;
    s = DB::Open(options, dbpath, &db);
    if(!s.ok()){
        delete db;
        printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.code(),s.ToString().c_str()); 
        return s.code();
    }
    ReadOptions rp=ReadOptions();
    RRreadOptions(&rp);
    std::string value;
    s=db->Get(rp ,Slice(val) ,&value);
    char gpath[256];
    memset(gpath,0,sizeof(gpath));
    getDBKeyStrpath(id,key,gpath);
    if(value == ""){
        rrdbObj *rrdb=RRdbGet(gpath);
        if(rrdb != NULL) rrdb->fieldcount++;
        RRUpdateSetCustom(id,type,key,RR_OP_INCREASE);
    }  
    char tmbuf[128];
    memset(tmbuf,0,sizeof(tmbuf));
    mstimeTostr(tmbuf);
    s = db->Put(WriteOptions(),Slice(val), Slice(tmbuf));

    delete db;
    
    return s.Code::kOk;


}


