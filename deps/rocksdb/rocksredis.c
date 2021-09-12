#include "../../src/server.h"
#include "rr.h"
#include "rocksredis.h"
#include "magnet.h"
#include <stdio.h>
#include "stdlib.h"

#include "rocksredis_debug.h"

/*rocksredis mapCommand Tbale*/
struct rocksredisCommand rocksredisCommandTable[] ={
    {"hsetnx",RRhsetnxCommand,0,DBHASH},
    {"hset",RRhsetCommand,0,DBHASH},
    {"hget",RRhgetCommand,0,DBHASH},
    {"hscan",RRhscanCommand,0,DBHASH},
    {"del",RRdelCommand,0,DB_KEY},
    {"hdel",RRhdelCommand,0,DBHASH},
    {"hlen",RRhlenCommand,0,DBHASH},
    {"hexists",RRhexistsCommand,0,DBHASH},
    {"hgetall",RRhgetallCommand,0,DBHASH},
    {"hvals",RRhvalsCommand,0,DBHASH},
    {"hstrlen",RRhstrlenCommand,0,DBHASH},
    {"hkeys",RRhkeysCommand,0,DBHASH},
    {"hmget",RRhmgetCommand,0,DBHASH},
    {"scan",RRscanCommand,0,DB_KEY},
    {"type",RRtypeCommand,0,DB_KEY},
    {"sadd",RRsaddCommand,0,DBSET},
    {"scard",RRscardCommand,0,DBSET},
    {"sismember",RRsismemberCommand,0,DBSET},
    {"smembers",RRsinterCommand,0,DBSET},
    {"spop",RRspopCommand,0,DBSET},
    {"sscan",RRsscanCommand,0,DBSET},
    {"srem",RRsremCommand,0,DBSET},
    {"strlen",RRstrlenCommand,0,DBSET},
    {"set",RRsetCommand,0,DBSTRING},
    {"get",RRgetCommand,0,DBSTRING}
};

/* =basic fun= */
void mstimeTostr(char *time){
    sprintf(time,"%lld",mstime());
}
/*=Sdslen=*/
long int getSdsLen(sds s){
    return  sdslen(s);
}
/*=rocksredis db=*/
int rocksredisdb(void){
   return RRdbNum();
}

int Get1970UtcSec(void){
	time_t timep;
	struct tm *p;
	time(&timep);
	p=localtime(&timep);
	timep = mktime(p);
	return timep;
}

/* =rocksredisCommands execution= */
int RRtypeCommand(client *c){
    char dbcustom_key[512];
    memset(dbcustom_key,0,sizeof(dbcustom_key));
    getDBKeyStrpath(c->db->id,c->argv[1]->ptr,dbcustom_key);
    const char * s=RRDBcustomVal(dbcustom_key);
    if(strcasecmp(CkInvalidArgument,s)==0){//db not exist
        s="";
    }else if(strcasecmp(CkIOError,s)==0 ){//db open err           
        while(strcasecmp(CkIOError,s)==0){
             s = RRDBcustomVal(dbcustom_key);
             usleep(500);
        }
    }
    int key_type=100;
    if(strlen(s) !=0 ){
        key_type=atoi(s);
    } 
    char* type;
    switch(key_type) {
        case OBJ_STRING: type = "string"; break;
        case OBJ_LIST: type = "list"; break;
        case OBJ_SET: type = "set"; break;
        case OBJ_ZSET: type = "zset"; break;
        case OBJ_HASH: type = "hash"; break;
        case OBJ_STREAM: type = "stream"; break;
        default: type = "unknown"; break;
    }
    addReplyStatus(c, type);
    return 1;
}

/*=redsi del field command=*/
int RRdelRrobjFieldCount(client *c){
    char  dbcustom_key[512];
    sds dbcustom_key_sds=sdsnew(dbcustom_key);
    memset(dbcustom_key,0,sizeof(dbcustom_key));
    getDBKeyStrpath(c->db->id,c->argv[1]->ptr,dbcustom_key);
    dictEntry *de=  dictFind(rrserver.rrdbs, dbcustom_key_sds);
    sdsfree(dbcustom_key_sds);
    if(de != NULL){
        rrdbObj *rrobj=dictGetVal(de);
        if(rrobj->fieldcount >0) rrobj->fieldcount--;
        if(rrobj->fieldcount==0){
            RRUpdateCustom(dbcustom_key,RR_OP_KEY_DEL,rrobj->type);
            dictDelete(rrserver.rrdbs, dictGetKey(de));
            RRdeleteDB(c->db->id,rrobj->type,c->argv[1]->ptr);
            zfree(rrobj);
            rrobj=NULL;
             return 1;
        } 
    }else return 0;
}

/*=redis del command=*/
int RRdelCommand(client *c){
    char  dbcustom_key[512];
    int numdel = 0, j;
    for (j = 1; j < c->argc; j++) {
        sds dbcustom_key_sds=sdsnew(dbcustom_key);
        memset(dbcustom_key,0,sizeof(dbcustom_key));
        getDBKeyStrpath(c->db->id,c->argv[j]->ptr,dbcustom_key);
        dictEntry *de=  dictFind(rrserver.rrdbs, dbcustom_key_sds);
        sdsfree(dbcustom_key_sds);
        
        if(de != NULL){
            rrdbObj *rrobj=dictGetVal(de);
            int deleted = RRUpdateCustom(dbcustom_key,RR_OP_KEY_DEL,rrobj->type);
            if (deleted) {
                numdel++;
                dictDelete(rrserver.rrdbs, dictGetKey(de));
                RRdeleteDB(c->db->id,rrobj->type,c->argv[j]->ptr);
                zfree(rrobj);
                rrobj=NULL;
            }
        }
    }
    addReplyLongLong(c,numdel);
    return 1;
}
/*=redis scan command=*/
int RRscanCommand(client *c){
    int size=dictSize(rrserver.rrdbs);
    if(size > 0){
        addReplyArrayLen(c, 2);
        addReplyBulkLongLong(c,0);
        addReplyArrayLen(c, size);
        dictIterator *di = dictGetIterator(rrserver.rrdbs);
        dictEntry *de;
        while ((de = dictNext(di)) != NULL) {
           rrdbObj *rrobj=dictGetVal(de);
           robj cf_kobj;
           sds key_sds=sdsnew(rrobj->key);
           initStaticStringObject(cf_kobj, key_sds);
           addReplyBulk(c, &cf_kobj);
           sdsfree(key_sds);
           
        }
    }else{
        addReply(c,shared.czero);
    }
    return 1;
}

/* =rocksredisCommands lookup and execution =*/
struct rocksredisCommand *rocksredislookupCommand(sds name) {
    return dictFetchValue(rrserver.commands, name);
}

/*=rocksredis exe command=*/
struct rocksredisCommand *rocksredislookupCommandByCString(char *s) {
    struct rocksredisCommand *cmd;
    sds name = sdsnew(s);
    cmd = dictFetchValue(rrserver.commands, name);
    sdsfree(name);
    return cmd;
}

/*=redisrocks map Commnad =*/
void populateRRCommandTable(){
    int j;
    int numcommands = sizeof(rocksredisCommandTable)/sizeof(struct rocksredisCommand);
    for (j = 0; j < numcommands; j++) {
        struct rocksredisCommand *c = rocksredisCommandTable+j;
        c->id = ACLGetCommandID(c->name); /* Assign the ID used for ACL. */
        dictAdd(rrserver.commands, sdsnew(c->name), c);
    }
}

/*=redis to rocksredis command entry=*/
int rocksrediscommand(client *c){
    struct rocksredisCommand *rrcommand;
    int ret=1;
    sds cmd=sdsnew(c->argv[0]->ptr);
    rrcommand=rocksredislookupCommand(cmd);
    if(rrcommand == NULL) {
        addReplyError(c,"Invalid instruction");
        return -1;
    }
    if(rrcommand->type != DB_KEY){
         char key[256];
         memset(key,0,sizeof(key));
         strcat(key,c->argv[1]->ptr);
         ret=rocksredisReg(c,c->db->id,rrcommand->type,key);
    } 
    if(rrcommand != NULL)  {
        if(ret == 1 ) rrcommand->proc(c);
        else if(ret == 0) addReplyError(c,"Invalid instruction");
        else if(ret == -1)  addReply(c,shared.wrongtypeerr);
    }else addReplyError(c,"Invalid instruction");


    sdsfree(cmd);

    return 1;
}

/*
re*rocksReids Key Reg
*db_key = rocksdb_dir+"db_"+key
*/
int rocksredisReg(client *c,int id,int type,char * key){
    char  dbcustom_key[512];
    char *s;
    int ret=1;
    sds dbcustom_key_sds=sdsnew(dbcustom_key);
    
    if(key==""||(strlen(key)==0)){
        return 0;
    }
    memset(dbcustom_key,0,sizeof(dbcustom_key));
    getDBKeyStrpath(id,key,dbcustom_key);
    dictEntry *de=  dictFind(rrserver.rrdbs, dbcustom_key_sds);

    if(de == NULL) {
        int ret=1;
        while(ret){
            ret=RRcreateDB(id, type, key);
        }
        rrdbObj *rrobj=zmalloc(sizeof(rrdbObj));
        memset(rrobj,0,sizeof(rrobj));
        rrobj->type=type;
        rrobj->dirty=0;
         while(ret){
             ret=RRGetrrdbObj(dbcustom_key,rrobj->type,rrobj);
        }
        dictAdd(rrserver.rrdbs, sdsnew(dbcustom_key), rrobj);
         
    }else{
        rrdbObj *rrobj=dictGetVal(de);
        if(type != rrobj->type){
            return -1;
        }
        //加载custom信息
        if(!rrobj->IscustomLoad) {
            if(rrobj->type==DBSTRING){
               while(ret){
                 ret=RRGetrrStringdbObj(dbcustom_key,rrobj->type,rrobj);
              }
           }else{
               while(ret){
                ret=RRGetrrdbObj(dbcustom_key,rrobj->type,rrobj);
              }
           }
        }
    }

    sdsfree(dbcustom_key_sds);
    
    return 1;
}

void RRdbGetOBJ(const char *db_key,rrdbObj *rrobj){
    sds db_key_sds=sdsnew(db_key);
    dictEntry *de=  dictFind(rrserver.rrdbs, db_key_sds);
    if(de == NULL) {
       rrobj= NULL;
    }else{
       rrobj=dictGetVal(de);
    }

    sdsfree(db_key_sds);
}

/*
*select db 
*/
const rrdbObj *RRdbGet(const char *db_key){
    sds db_key_sds=sdsnew(db_key);
    dictEntry *de=  dictFind(rrserver.rrdbs, db_key_sds);
    sdsfree(db_key_sds);
    if(de == NULL) {
        return NULL;
    }else{
        rrdbObj *rrobj=dictGetVal(de);
        return rrobj;
    }
}

/*expire key force to flush
* from servCorn thread
save 900 1    >15min ,at least 1 items
save 300 10   >5min ,at least 10 items
save 60 1000  >60S ,at least 1000 items
*/
void RRexpire(){
    dictIterator *di = dictGetIterator(rrserver.rrdbs);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL) {
       rrdbObj *rrobj=dictGetVal(de);
        for (int j = 0; j < server.saveparamslen; j++) {
            struct saveparam *sp = server.saveparams+j;
            //printf("key:%s,rrobj->dirty:%d,sp->changes:%d,rrobj->lastsave:%d,server.unixtime:%d\r\n",dictGetKey(de),rrobj->dirty,sp->changes,rrobj->lastsave,server.unixtime);
            if(rrobj->countForexpire > 0){
//                printf("key:%s,rrobj->dirty:%d,sp->changes:%d,rrobj->lastsave:%d,server.unixtime:%d\r\n",dictGetKey(de),rrobj->countForexpire,sp->changes,rrobj->lastsave,server.unixtime);
                if (( rrobj->countForexpire >= sp->changes) && ((server.unixtime-rrobj->lastsave )> sp->seconds))
                {
                    if(rrobj->type==OBJ_HASH) {
                        rrobj->lastsave=server.unixtime;
                        rrobj->countForexpire=0;
                        int ret=RRhsetnxBatchFlush(rocksredisdb(),rrobj->key);
                    }
                    break;
                }
            }
            
        }
    }

}
/*
*rocks redis init config
*/ 
void RRinit(){
    rrserver.commands = dictCreate(&commandTableDictType,NULL);
    populateRRCommandTable();
    rrserver.rrdbkeys =  listCreate();
    int ret=RRInitDB();
    if(ret){
        RSPRING_ERR("RocksRedis InitDB ERR!,Please Check Permission or The database may have been accidentally deleted!");
        exit(0);
    }
    
    rrserver.rrdbs=dictCreate(&setDictType,NULL);
    rrserver.customKeyCount=dictCreate(&setDictType,NULL);
    rrserver.rrmembuf[0]=dictCreate(&setDictType,NULL);
    rrserver.rrmembuf[1]=dictCreate(&setDictType,NULL);
    rrserver.rrmembuf[2]=dictCreate(&setDictType,NULL);
    rrserver.rrTobatchmembuf=0;
    rrserver.rrToWritemembuf=0;

    pool_init(rsmagnet.batch_write_background_threadpools); 
    
    listNode *node, *nextnode;
    
    ret=RRInitrrdbs(rrserver.rrdbkeys);
    if(ret){
        RSPRING_ERR("RocksRedis Initrrdbs ERR!,Please Check Permission or The database may have been accidentally deleted!");
        exit(0);
    }

//    ret = RRinitCustomKeyCount(rrserver.customKeyCount);
//    if(ret){
//        exit(0);
//    }

    node = listFirst(rrserver.rrdbkeys);
    char *s;
    dictEntry *de;
    
   
    while (node) {
        rrkv *kv = listNodeValue(node);
        sds *dbcustom_key = kv->key;
        nextnode = listNextNode(node);
        rrdbObj *rrobj=zmalloc(sizeof(rrdbObj));
        rrobj->type=atoi(kv->val);
        rrobj->IscustomLoad=0;
        rrobj->dirty=0;
        rrobj->fieldcount=0;

        memset(rrobj->key,0,sizeof(rrobj->key));
        strcpy(rrobj->key,strrchr(dbcustom_key,'/')+1);
    
        //先加载类型，custom下的先不加载，使用过的时候在异步加载
        //rrobj->IscustomLoad 异步加载标记
        dictAdd(rrserver.rrdbs,dbcustom_key, rrobj);
//        printf("scan keys:%s\r\n",rrobj->key);
//        if(rrobj->type==DBSTRING) RRGetrrStringdbObj(dbcustom_key,rrobj->type,rrobj);
//        else{
//           de = dictFind(rrserver.customKeyCount, sdsnew(kv->key));
//           if(de){
//               rrobj->fieldcount= atoi(dictGetVal(de));
//               printf("init rrobj dict find cont:%d\r\n",rrobj->fieldcount);
//               dictAdd(rrserver.rrdbs,dbcustom_key, rrobj);
//           }else{
//               s =  RRGetrrdbObj(dbcustom_key,rrobj->type,rrobj);
//                if(strcasecmp(CkOk,s)==0){
//                    dictAdd(rrserver.rrdbs,dbcustom_key, rrobj);
//                }else{
//                     RSPRING_ERR("RocksRedis Initrrdbs ERR!,Please Check Permission or The database may have been accidentally deleted!");
//                     exit(0);
//                }
//           }
//           dictDelete(rrserver.customKeyCount, sdsnew(kv->key));
//        } 
        node = nextnode;
    }
    listRelease(rrserver.rrdbkeys);
    dictRelease(rrserver.customKeyCount);
}


void rocksredisinit(int cluster_enabled){

    if(cluster_enabled && (rsmagnet.rocksredis_select != -1)){
       rsmagnet.rocksredis_select=0;
    }
    RRinit();
    //debug config parms
    printf("@rsmagnet.batch_write_background_threadpools:%d\r\n",rsmagnet.batch_write_background_threadpools);
    printf("@rsmagnet.rocksredis_db_dir:%s\r\n",rsmagnet.rocksredis_db_dir);
    printf("@rsmagnet.rocksredis_select:%d\r\n",rsmagnet.rocksredis_select);
    printf("@rsmagnet.write_buffer_size:%d\r\n",rsmagnet.write_buffer_size);
    printf("@rsmagnet.max_write_buffer_number:%d\r\n",rsmagnet.max_write_buffer_number);
    printf("@rsmagnet.max_background_flushes:%d\r\n",rsmagnet.max_background_flushes);
    printf("@rsmagnet.max_background_compactions:%d\r\n",rsmagnet.max_background_compactions);
    printf("@rsmagnet.rocks_write_batch_size:%d\r\n",rsmagnet.rocks_write_batch_size);
    if(rsmagnet.rocks_write_batch_size < 2000) rsmagnet.rocks_write_batch_size=2000;
  
}


