#include "../../src/server.h"
#include "rr.h"
#include "rocksredis.h"
#include <stdio.h>
#include "stdlib.h"
#include "magnet.h"


/*
*hash write Batch Thread
*/
void *RRhsetnxWriteBatchThread(void *arg){
    RRWriteBatch *wb=(RRWriteBatch *)arg;
    int ret=1;

    while(ret){
        ret=RRhsetnxBatch(wb->id,OBJ_HASH,wb->key,wb->ld);      
//        sleep(1);
        msleep(300);

    }

   // printf("id=%d,key:%s,len=%ld\r\n",wb->id,wb->key,listLength(wb->ld));
    listRelease(wb->ld);
    zfree(wb);
}

/*
*for Get data err
*/
void * RRhgetEXthread(void *arg){
    const char * s;
    client *c=(client *)arg;
    s = RRhget(c->db->id,OBJ_HASH,c->argv[1]->ptr,c->argv[2]->ptr);
    while(strcasecmp(CkIOError,s)==0){
      s = RRhget(c->db->id,OBJ_HASH,c->argv[1]->ptr,c->argv[2]->ptr);
       msleep(300);
    }

    s="";
    int len=strlen(s)+1;
    char  val[len];
    memset(val,0,len);
    strcpy(val,s);
    sds res=sdsnew(val);
    if(s==NULL || strlen(s)==0){
        addReply(c,shared.null[c->resp]);
    }else{
        addReplyBulkCBuffer(c, res, sdslen(res));
    }
}

/*
*servCorn expire key to force flush
*/
int RRhsetnxBatchFlush(int id,char *key){
    dict *rb=rrserver.rrmembuf[ rrserver.rrTobatchmembuf];
    sds skey=sdsnew(key);
    dictEntry *de=  dictFind(rb, skey);

    if(de == NULL){ sdsfree(skey); return 0;}
    else{
        list *ld= dictGetVal(de);
        int len=listLength(ld);
        RRWriteBatch *wb=zmalloc(sizeof(RRWriteBatch));
        wb->id=id;
        wb->type=OBJ_HASH;
        memset(wb->key,0,sizeof(wb->key));
        strcat(wb->key,key);
        wb->ld=listDup(ld);//ld;
        dictDelete(rb, skey);
        listRelease(ld);
        pool_add_worker(&RRhsetnxWriteBatchThread, (void *)wb);
    }
    sdsfree(skey);
    return 1;
}

int RRhdelByrrmemBuf(int id,char *key,char *field){
    int ret=0;
    char *retStr=NULL;
    dict *rb=rrserver.rrmembuf[ rrserver.rrTobatchmembuf];
    sds skey=sdsnew(key);
    dictEntry *de=  dictFind(rb, skey);
    if(de == NULL){sdsfree(skey);  return 0;} 
    list *ld= dictGetVal(de);
    listNode *node, *nextnode;
    node = listFirst(ld);
    while (node) {
        rrkv *kv = (rrkv *)listNodeValue(node);
        nextnode = listNextNode(node); 
        if(strcmp(field, kv->key)==0){
           ret=1;
           listDelNode(ld,node);
           zfree(kv);
           break;
        }
        node = nextnode;
    }
    sdsfree(skey);

    return ret;
}

const char* RRhgetByrrmemBuf(int id,char *key,char *field){
    char *retStr=NULL;
    dict *rb=rrserver.rrmembuf[ rrserver.rrTobatchmembuf];
    sds skey=sdsnew(key);
    dictEntry *de=  dictFind(rb, skey);
    if(de == NULL){sdsfree(skey);  return NULL;} 
    list *ld= dictGetVal(de);
    listNode *node, *nextnode;
    if(listLength(ld)==0) return NULL;
    
    node = listFirst(ld);
    while (node) {
        rrkv *kv = (rrkv *)listNodeValue(node);
        nextnode = listNextNode(node); 
        if(strcmp(field, kv->key)==0){
//           printf("buffetch\r\n");
           retStr= kv->val;
           break;
        }
        node = nextnode;
    }
    sdsfree(skey);
    return retStr;
}

int RRcoverrrmemBufKey(const char * field,const char * val,const list * ld){
        int ret=0;
        listNode *node, *nextnode;

//        printf("listnode:%d,field:%s,val:%s\r\n",listLength(ld),field,val);
        
        if(listLength(ld)==0) return 0;
        node = listFirst(ld);
        while (node) {
            rrkv *kv = (rrkv *)listNodeValue(node);
//            printf("rrmembufkey:%s,key:%s\r\n",kv->key,field);
            nextnode = listNextNode(node); 
            if(strcmp(field,kv->key)==0){
                
                kv->val=sdsnew(val);
                ret=1;
                break;
            }
            node = nextnode;
        }
        return  ret;
}
/*
*hash setnx batch command 
*
*/
int RRhsetnxBatchCommand(client *c){
    int id=c->db->id;
    char *key=c->argv[1]->ptr;
    char *field=c->argv[2]->ptr;
    char *val=c->argv[3]->ptr;

    dict *rb=rrserver.rrmembuf[ rrserver.rrTobatchmembuf];

    sds skey=sdsnew(key);
    dictEntry *de=  dictFind(rb, skey);

    char gpath[256];
    memset(gpath,0,sizeof(gpath));
    getDBKeyStrpath(id,key,gpath);
    dictEntry *de_rrdb=  dictFind(rrserver.rrdbs, sdsnew(gpath));
    rrdbObj *rrdb;
    if(de_rrdb != NULL) {
         rrdb=dictGetVal(de_rrdb);
    }

    if(rrdb ==  NULL){
        return 0;
    } 
    
    if(de == NULL) {
        list *ll=listCreate();
        rrkv *kv=(rrkv*)zmalloc(sizeof(rrkv));
        kv->key=sdsnew(field);
        kv->val=sdsnew(val);
        listAddNodeTail(ll,kv);
        dictAdd(rb, skey, ll);

        rrdb->countForexpire++;
        rrdb->lastsave=server.unixtime;
    }else{//得加一个key的内存去重
        list *ld= dictGetVal(de);
        
        if(RRcoverrrmemBufKey(field, val,ld)) return 1;
        
        rrkv *kv=(rrkv*)zmalloc(sizeof(rrkv));
        kv->key=sdsnew(field);
        kv->val=sdsnew(val);
        
        listAddNodeTail(ld,kv);
        int len=listLength(ld);
        rrdb->countForexpire++;
        rrdb->lastsave=server.unixtime;

//        printf("hset,dbkey:%s,count:%d\r\n",dictGetKey(de_rrdb),rrdb->countForexpire);
        if(len >= rsmagnet.rocks_write_batch_size){
            RRWriteBatch *wb=zmalloc(sizeof(RRWriteBatch));
            wb->id=c->db->id;
            wb->type=OBJ_HASH;
            memset(wb->key,0,sizeof(wb->key));
            strcat(wb->key,c->argv[1]->ptr);
            wb->ld=listDup(ld);//ld;
            dictDelete(rb, skey);
            listRelease(ld);
            rrdb->lastsave=server.unixtime;
            pool_add_worker(&RRhsetnxWriteBatchThread, (void *)wb);
            rrdb->countForexpire=0;
        }
         sdsfree(skey);
        
    }

    return 1;
}

/*
*hash setnx commad
*/
int RRhsetnxCommand(client *c){
    int ret;
    ret=RRhsetnxBatchCommand(c);
//    ret=RRhsetnx(c->db->id,OBJ_HASH,c->argv[1]->ptr,c->argv[2]->ptr,c->argv[3]->ptr);
    addReply(c, shared.cone);
    return ret;
}


/*
*hash set command
*/
int RRhsetCommand(client *c){
    int i=0,created = 0;
    int ret=0;
    for (i = 2; i < c->argc; i += 2){
        created += RRhsetnxBatchCommand(c);
//        if(ret){
//            created++;
//        }else{
//            created += RRhsetnx(c->db->id,OBJ_HASH,c->argv[1]->ptr,c->argv[2]->ptr,c->argv[3]->ptr);
//        }
    }

    char *cmdname = c->argv[0]->ptr;
    if (cmdname[1] == 's' || cmdname[1] == 'S') {
        /* HSET */
        addReplyLongLong(c, created);
    } else {
        /* HMSET */
        addReply(c, shared.ok);
    }
    return 1;
}
/*
hash hstrlen command
*/
int RRhstrlenCommand(client *c){
    const char * s;
    s = RRhgetByrrmemBuf(c->db->id,c->argv[1]->ptr,c->argv[2]->ptr);
    if(s == NULL){
       s = RRhget(c->db->id,OBJ_HASH,c->argv[1]->ptr,c->argv[2]->ptr);
       if(strcasecmp(CkInvalidArgument,s)==0){//db not exist
           s="";
        
       }else if(strcasecmp(CkIOError,s)==0 ){//db open err           
            while(strcasecmp(CkIOError,s)==0){
                 s = RRhget(c->db->id,OBJ_HASH,c->argv[1]->ptr,c->argv[2]->ptr);
                 msleep(300);
            }
       }
    }

    if(s==NULL || (strlen(s)==0)){
        addReply(c,shared.czero);
    }else{
        long len=strlen(s);
        addReplyLongLong(c,len);
    } 

    return 1;
   
}


/*
*hash get command
*/
int RRhgetCommand(client *c){
    const char * s;
    s = RRhgetByrrmemBuf(c->db->id,c->argv[1]->ptr,c->argv[2]->ptr);
    if(s == NULL){
       s = RRhget(c->db->id,OBJ_HASH,c->argv[1]->ptr,c->argv[2]->ptr);
       if(strcasecmp(CkInvalidArgument,s)==0){//db not exist
           s="";
       }else if(strcasecmp(CkIOError,s)==0 ){//db open err           
            while(strcasecmp(CkIOError,s)==0){
                 s = RRhget(c->db->id,OBJ_HASH,c->argv[1]->ptr,c->argv[2]->ptr);
                 msleep(300);
            }
       }
   } 

    int len=strlen(s)+1;
    char  val[len];
    memset(val,0,len);
    strcpy(val,s);
    sds res=sdsnew(val);
    if(s==NULL || strlen(s)==0){
        addReply(c,shared.null[c->resp]);
    }else{
        addReplyBulkCBuffer(c, res, sdslen(res));
    }

    sdsfree(res);
    
    return 1;
}

/*
*hash scan command
*/
int RRhscanCommand(client *c){
    int ret=1;
    long int size;
    long int cursor;
    long int count;
    char  dbcustom_key[512];
    memset(dbcustom_key,0,sizeof(dbcustom_key));
    getDBKeyStrpath(c->db->id,c->argv[1]->ptr,dbcustom_key);
    
    sds dbcustom_key_sds=sdsnew(dbcustom_key);
    dictEntry *de=  dictFind(rrserver.rrdbs, dbcustom_key_sds);
    if(de != NULL){
        rrdbObj *rrobj=dictGetVal(de);
        size=rrobj->fieldcount;
        if(!strcasecmp(c->argv[0]->ptr,"hgetall")){
            while(ret){
                 ret=RRhscan(c,c->db->id,OBJ_HASH, c->argv[1]->ptr, 0, size,0,"hgetall");
            }
        }else if(!strcasecmp(c->argv[0]->ptr,"hkeys")){
            while(ret){
                ret = RRhscan(c,c->db->id,OBJ_HASH, c->argv[1]->ptr, 0, size,0,"hkeys");
            }
        }else if(!strcasecmp(c->argv[0]->ptr,"hvals")){
            while(ret){
                ret = RRhscan(c,c->db->id,OBJ_HASH, c->argv[1]->ptr, 0, size,0,"hvals");;
            }
        }else if(!strcasecmp(c->argv[0]->ptr,"hscan")){
            cursor=atoi(c->argv[2]->ptr);
            count=atoi(c->argv[4]->ptr);
            if(cursor > size) count=0;
            else if((count+cursor)>size) count = size-cursor;
            
            while(ret){
                ret=RRhscan(c,c->db->id,OBJ_HASH, c->argv[1]->ptr, cursor, count,0,"hscan");
            }
        }
    }else{
        addReply(c,shared.emptyscan);
    }
   sdsfree(dbcustom_key_sds);
    return 1;
}


/*
*hash hlen command
*/
int RRhlenCommand(client *c){
    char  dbcustom_key[512];
    memset(dbcustom_key,0,sizeof(dbcustom_key));
    getDBKeyStrpath(c->db->id,c->argv[1]->ptr,dbcustom_key);
    sds  dbcustom_key_sds=  sdsnew(dbcustom_key);
    dictEntry *de=  dictFind(rrserver.rrdbs, dbcustom_key_sds);
    if(de != NULL){
        rrdbObj *rrobj=dictGetVal(de);
        addReplyLongLong(c,rrobj->fieldcount);
    }else{
        addReply(c,shared.czero);
    }

    sdsfree(dbcustom_key_sds);
    return 1;
}
/*
*hash del 
*key[field]
*/
int RRhdelCommand(client *c){
    int j, deleted = 0, keyremoved = 0;
    for (j = 2; j < c->argc; j++) {
        RRhdelByrrmemBuf(c->db->id, c->argv[1]->ptr,c->argv[j]->ptr);
        if (RRhdel(c->db->id,OBJ_HASH,c->argv[1]->ptr,c->argv[j]->ptr)) {
            deleted++;
            int ret=RRdelRrobjFieldCount(c);
            if(ret){
               keyremoved=1;
               break;
            } 
        }
    }
    addReplyLongLong(c,deleted);
    return 1;
}


/*
*hash kexist command
*/
int RRhexistsCommand(client *c){
    if(RRhgetByrrmemBuf(c->db->id,c->argv[1]->ptr,c->argv[2]->ptr) != NULL) addReply(c,  shared.cone);
    else{
        int ret = -1;
        while((ret == -1)){
           ret= RRhexist(c->db->id,OBJ_HASH,c->argv[1]->ptr,c->argv[2]->ptr);
        } 
        addReply(c, ret ? shared.cone : shared.czero);
    }
    return 1;
}


/*
*hash getall command
*/
int RRhgetallCommand(client *c){
    RRhscanCommand(c);
    return 1;
}

/*
*hash hvals  command
*/
int RRhvalsCommand(client *c){
    RRhscanCommand(c);
    return 1;
}

/*
*hash keys  command
*/
int RRhkeysCommand(client *c){
    RRhscanCommand(c);
    return 1;
}

/*
*hash mget  command
*/
int RRhmgetCommand(client *c){
    addReplyArrayLen(c, c->argc-2);
    for (int i = 2; i < c->argc; i++) {
        const char * s;
        s = RRhgetByrrmemBuf(c->db->id,c->argv[1]->ptr,c->argv[2]->ptr);
        if(s==NULL)  s=RRhget(c->db->id,OBJ_HASH,c->argv[1]->ptr,c->argv[i]->ptr);
        
        int len=strlen(s)+1;
        char  val[len];
        memset(val,0,len);
        strcpy(val,s);
        sds res=sdsnew(val);
         if(s==NULL || strlen(s)==0){
           addReply(c,shared.null[c->resp]);
        }else{
            addReplyBulkCBuffer(c, res, sdslen(res));
        }
    }
    return 1;
}


