#include <cstdio>
#include <string>
#include <vector>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <sys/stat.h>
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
extern "C" dictIterator *dictGetIterator(dict *d);
extern "C" void zfree(void *ptr);
extern "C" void sdsfree(sds s) ;

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
extern "C" void RRgetOptions(struct Options *op);
extern "C" void RRreadOptions(struct ReadOptions *op);
extern "C" void RRwriteOptions(struct WriteOptions *op);
extern "C" int RRsyncKeyCountToCustom(const char *dbcustom_key,int c,int op);
extern "C" std::string getDBpath(int id);

/****************************************
# quote magnet_hash related content
****************************************/
extern "C"    int RRhsetnx(int id,int type,char *key,char * field,char *val);
extern "C"    int RRUpdateHashCustom(int id,int type,char *key,int op);
extern "C"    int RRhsetnxBatch(int id,int type,char *key,list *batchlist);


int RRUpdateHashCustom(int id,int type,char *key,int op){
    Options options;
    Status s;
    DB* db;
    int custom_key_count=0;
    options.create_if_missing = true;
    std::string keypath=rsmagnet.rocksredis_db_dir+getDBTypeKeyPath(id,type,key);
    std::string custompath = keypath+"/custom";
    
    s = DB::Open(options, custompath, &db);
    if(!s.ok()){
         delete db;
         printf("code:%d %s:%d: ERR:%s\n",__FUNCTION__, __LINE__,s.code(),s.ToString().c_str()); 
         return s.code();
    }
    
    ReadOptions rp=ReadOptions();
    RRreadOptions(&rp);
    std::string value;
    s=db->Get(rp ,Slice(key) ,&value);
  
    char count[128];
    char * pEnd;
    unsigned long int opcount=0;
    memset(count,0,sizeof(count));
    opcount = strtoul(value.c_str(),&pEnd,10);

    char gpath[256];
    memset(gpath,0,sizeof(gpath));
    getDBKeyStrpath(id,key,gpath);
    rrdbObj *rrdb=RRdbGet(gpath);
    
    if(op==RR_OP_INCREASE){
        if(rrdb != NULL){
           opcount += rrdb->dirty;
       } else {
            opcount++;
       }
    }
    else if(op==RR_OP_REDUCE && opcount>0){
         if(rrdb != NULL){
             if( rrdb->dirty > 0){
                 rrdb->dirty--;
             } 
         };
        opcount--;
    }
        
    sprintf(count,"%ld",opcount);
    db->Put(WriteOptions(), Slice(key), Slice(count));
    if(op==RR_OP_INCREASE){
        rrdb->dirty = 0;
    }
    
    delete db;
    return s.Code::kOk;
    /*sync CUSTOM_KEY_COUNT*/
//    std::string DBcustomKey=getDBpath(id)+key;
//    RRsyncKeyCountToCustom(DBcustomKey.c_str(),custom_key_count,op);
}

int RRhdel(int id,int type,char *key,char * field){
    Options options;
    Status s;
    DB* db;
    options.create_if_missing = true;
    std::string keypath=rsmagnet.rocksredis_db_dir+getDBTypeKeyPath(id,type,key);
    std::string dbpath= keypath+"/"+key;
    s = DB::Open(options, dbpath, &db);
    if(!s.ok()){
        delete db;
        printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.ToString().c_str()); 
        return s.code();
    }

    db->Delete(WriteOptions(), Slice(field));
    
    delete db;
    
    int ret =1;
    while(ret){
       ret=RRUpdateHashCustom(id,type,key,RR_OP_REDUCE);
    }
    
    return s.Code::kOk;
}

const char * RRhget(int id,int type,char *key,char * field){
    Options options;
    Status s;
    DB* db;
    std::string db_key=rsmagnet.rocksredis_db_dir+getDBTypeKeyPath(id,type,key)+"/"+key;
    s = DB::Open(options, db_key, &db);
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
    s=db->Get(rp ,Slice(field) ,&value);
    delete db;
     if(value == ""){
       return "";
    }else{
        return value.c_str();
    }
}

int RRhsetnxBatch(int id,int type,char *key,list *batchlist){
    Options options;
    Status s;
    DB* db;
    RRgetOptions(&options);
    options.create_if_missing = true;
    std::string keypath=rsmagnet.rocksredis_db_dir+getDBTypeKeyPath(id,type,key);
    std::string dbpath= keypath+"/"+key;
    s = DB::Open(options, dbpath, &db);
    if(!s.ok()){
        delete db;
        printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.ToString().c_str()); 
        return s.code();
    }
    ReadOptions rp=ReadOptions();
    RRreadOptions(&rp);
    std::string value;
    
    WriteOptions wp=WriteOptions();
    RRwriteOptions(&wp);
    WriteBatch batch;

    listNode *node, *nextnode;
    node = listFirst(batchlist);
    char gpath[256];
    memset(gpath,0,sizeof(gpath));
    getDBKeyStrpath(id,key,gpath);
    rrdbObj *rrdb=RRdbGet(gpath);
    while (node) {
        rrkv *kv = (rrkv *)listNodeValue(node);
        nextnode = listNextNode(node);
        s=db->Get(rp ,Slice(kv->key) ,&value);
        if(!s.ok()){
            if(s.code()==s.Code::kNotFound){
                if(rrdb != NULL){
                    rrdb->fieldcount++;
                    rrdb->dirty++;
                }       
             }
        }  
        batch.Put(Slice(kv->key), Slice(kv->val));
        sdsfree(kv->key);
        sdsfree(kv->val);
        zfree(kv);
        node = nextnode;
    }
    s = db->Write(wp, &batch);
    if(!s.ok()){
        delete db;
        printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.ToString().c_str()); 
        return s.code();
    }  
    
    //Ë¢ÐÂ
    int ret= 1;
    while(ret){
        ret = RRUpdateHashCustom(id,type,key,RR_OP_INCREASE);
    }

    delete db;
    return s.Code::kOk;

}
//single Hset
int RRhsetnx(int id,int type,char *key,char * field,char *val){
    Options options;
    Status s;
    DB* db;
    RRgetOptions(&options);
    options.create_if_missing = true;
    std::string keypath=rsmagnet.rocksredis_db_dir+getDBTypeKeyPath(id,type,key);
    std::string dbpath= keypath+"/"+key;
    s = DB::Open(options, dbpath, &db);
    if(!s.ok()){
        delete db;
        printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.ToString().c_str()); 
        return s.code();
    }
    ReadOptions rp=ReadOptions();
    RRreadOptions(&rp);
    std::string value;
    s=db->Get(rp ,Slice(field) ,&value);
    if(value == ""){
        char gpath[256];
        memset(gpath,0,sizeof(gpath));
        getDBKeyStrpath(id,key,gpath);
        rrdbObj *rrdb=RRdbGet(gpath);
        if(rrdb != NULL) rrdb->fieldcount++;
        //Ë¢ÐÂ
        RRUpdateHashCustom(id,type,key,RR_OP_INCREASE);
    }  

    WriteOptions wp=WriteOptions();
    RRwriteOptions(&wp);
    s = db->Put(wp,Slice(field), Slice(val));
    if(!s.ok()){
        delete db;
        printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.ToString().c_str()); 
        return s.code();
    }  

    delete db;

    return s.Code::kOk;
}

int RRhexist(int id,int type,char *key,char * field){
    Options options;
    Status s;
    DB* db;
    options.create_if_missing = true;
    std::string keypath=rsmagnet.rocksredis_db_dir+getDBTypeKeyPath(id,type,key);
    std::string dbpath= keypath+"/"+key;
    s = DB::Open(options, dbpath, &db);
    if(!s.ok()){
       delete db;
       printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.ToString().c_str()); 
       return -1;
    }

    ReadOptions rp=ReadOptions();
    RRreadOptions(&rp);
    std::string value;
    s=db->Get(rp ,Slice(field) ,&value);
    delete db;
    if(!s.ok()){
        return 0;
    }  

    if(value == ""){
        return 0;
    }else{
        return 1;
    }

}

int RRhscan(struct client *c,int id,int type, char *key,unsigned long cursor,unsigned long count,int opt,char *cmd){
    int ret;
    Iterator* iterator;
    Options options;
    RRgetOptions(&options);
    DB* db;
    Status s;
    options.create_if_missing=true;
    ReadOptions rp=ReadOptions();
    RRreadOptions(&rp);
    
    rp.total_order_seek=true;
    std::string db_key=rsmagnet.rocksredis_db_dir+getDBTypeKeyPath(id,type,key)+"/"+key;
    s= DB::OpenAsSecondary(options, db_key, db_key,&db);
    if(!s.ok()){
        delete db;
        printf("code:%d, %s:%d: ERR:%s\n",__FUNCTION__, __LINE__,s.code(),s.ToString().c_str()); 
        return s.code();
    }
    iterator=db->NewIterator(rp);  
    std::string row, val;
    iterator->SeekToFirst();
    if(!strcasecmp(cmd,"hgetall")){
         addReplyMapLen(c, count);
     }else if(!strcasecmp(cmd,"hkeys") || !strcasecmp(cmd,"hvals")){
         addReplyArrayLen(c, count);
     }if(!strcasecmp(cmd,"hscan")){
         addReplyArrayLen(c, 2);
         addReplyBulkLongLong(c,cursor);
         addReplyArrayLen(c, count*2);
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
            val = iterator->value().ToString();
            if(!strcasecmp(cmd,"hvals")){
            }else{
                 robj cf_kobj;
                 sds row_sds=sdsnew(row.c_str());
                 initStaticStringObject(cf_kobj,row_sds);
                 addReplyBulk(c, &cf_kobj);
                 if(cf_kobj.type==OBJ_STRING)   freeStringObject(&cf_kobj);
                 sdsfree(row_sds);
            }
            if(!strcasecmp(cmd,"hkeys")){
            }else{
                 robj cf_vobj;
                 sds val_sds=sdsnew(row.c_str());
                 initStaticStringObject(cf_vobj, val_sds);
                 addReplyBulk(c, &cf_vobj);
                 if(cf_vobj.type==OBJ_STRING)   freeStringObject(&cf_vobj);

                 sdsfree(val_sds);
            }
          
        }else break;
        iterator->Next();
   }
    delete iterator;
    delete db;
    return s.Code::kOk;
}

