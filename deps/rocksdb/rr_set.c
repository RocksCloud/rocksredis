#include "../../src/server.h"
#include "rr.h"
#include "rocksredis.h"
#include <stdio.h>
#include "stdlib.h"
#include "magnet.h"

int RRsremCommand(client *c){
    int j, deleted = 0, keyremoved = 0;
    for (j = 2; j < c->argc; j++) {
        if (RRsrem(c->db->id,OBJ_SET,c->argv[1]->ptr,c->argv[j]->ptr)) {
            deleted++;
            if (RRdelRrobjFieldCount(c)) {
                keyremoved = 1;
                break;
            }
        }
    }
    addReplyLongLong(c,deleted);
    return 1;
}

int RRsscanCommand(client *c){
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
       cursor=atoi(c->argv[2]->ptr);
       count=atoi(c->argv[4]->ptr);
       if(cursor > size) count=0;
       else if((count+cursor)>size) count = size-cursor;

       RRsscan(c,c->db->id,rrobj->type, c->argv[1]->ptr, cursor, count,0,"sscan");
    }else{
       addReply(c,shared.emptyscan);
    }
    sdsfree(dbcustom_key_sds);
    return 1;
}

int RRsinterCommand(client *c){
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
        if(size>0){
            addReplyArrayLen(c, size);
            int ret=1;
            while(ret){ ret= RRsmembers(c,c->db->id,OBJ_SET,c->argv[1]->ptr);}
           
        }else addReply(c,shared.emptyset[c->resp]);
    }
    sdsfree(dbcustom_key_sds);
    return 1;
}

int RRspopCommand(client *c){
    unsigned long count, size;
    char  dbcustom_key[512];
    memset(dbcustom_key,0,sizeof(dbcustom_key));
    getDBKeyStrpath(c->db->id,c->argv[1]->ptr,dbcustom_key);
    sds dbcustom_key_sds=sdsnew(dbcustom_key);
    dictEntry *de=  dictFind(rrserver.rrdbs, dbcustom_key_sds);
    if (c->argc == 3) {//pop count
        if(de != NULL){
            rrdbObj *rrobj=dictGetVal(de);
            size=rrobj->fieldcount;
            if(size>0){
                count = atoi(c->argv[2]->ptr);
                if(count > size){count = size;}
                addReplyArrayLen(c, count);
                RRspop(c,c->db->id,OBJ_SET,c->argv[1]->ptr,count);
                rrobj->fieldcount=rrobj->fieldcount-count;
                if(rrobj->fieldcount==0){
                    RRUpdateCustom(dbcustom_key,RR_OP_KEY_DEL,rrobj->type);
                    dictDelete(rrserver.rrdbs, dictGetKey(de));
                    RRdeleteDB(c->db->id,rrobj->type,c->argv[1]->ptr);
                    zfree(rrobj);
                    rrobj=NULL;
                } 
            }else addReply(c,shared.emptyset[c->resp]);
        }else addReply(c,shared.emptyset[c->resp]);
    } else if (c->argc > 3) {
        addReply(c,shared.syntaxerr);
    }else{//pop one
        if(de != NULL){
            rrdbObj *rrobj=dictGetVal(de);
            size=rrobj->fieldcount;
            if(size==0) addReply(c,shared.emptyset[c->resp]);
            rrobj->fieldcount--;
            addReplyArrayLen(c, 1);
            RRspop(c,c->db->id,OBJ_SET,c->argv[1]->ptr,1);
            if(rrobj->fieldcount==0){
                RRUpdateCustom(dbcustom_key,RR_OP_KEY_DEL,rrobj->type);
                dictDelete(rrserver.rrdbs, dictGetKey(de));
                RRdeleteDB(c->db->id,rrobj->type,c->argv[1]->ptr);
                zfree(rrobj);
                rrobj=NULL;
            } 
        }else addReply(c,shared.emptyset[c->resp]);
    }
    sdsfree(dbcustom_key_sds);
    return 1;
}

int RRsismemberCommand(client *c){
    const char *s;
    s=RRsismember(c->db->id,OBJ_SET,c->argv[1]->ptr,c->argv[2]->ptr) ;
    if(strcasecmp(CkInvalidArgument,s)==0){//db not exist
       s="";
    }else if(strcasecmp(CkIOError,s)==0 ){//db open err           
        while(strcasecmp(CkIOError,s)==0){
             s = RRsismember(c->db->id,OBJ_SET,c->argv[1]->ptr,c->argv[2]->ptr);
             msleep(300);
        }
    }
    int key;
    key=atoi(s);
    addReply(c, key ? shared.cone : shared.czero);
    return 1;
}

int RRscardCommand(client *c){
    char  dbcustom_key[512];
    memset(dbcustom_key,0,sizeof(dbcustom_key));
    getDBKeyStrpath(c->db->id,c->argv[1]->ptr,dbcustom_key);
    sds dbcustom_key_sds=sdsnew(dbcustom_key);
    dictEntry *de=  dictFind(rrserver.rrdbs,dbcustom_key_sds);
    if(de != NULL){
        rrdbObj *rrobj=dictGetVal(de);
        addReplyLongLong(c,rrobj->fieldcount);
    }else{
        addReply(c,shared.czero);
    }
    sdsfree(dbcustom_key_sds);
    return 1;
}

int RRsaddCommand(client *c){
    int j, added = 0;
    for (j = 2; j < c->argc; j++) {
        int ret=1;
        while(ret){
            ret=RRsadd(c->db->id,DBSET,c->argv[1]->ptr,c->argv[j]->ptr);
        }
        added++;
    }
    addReplyLongLong(c,added);
    return 1;
}


