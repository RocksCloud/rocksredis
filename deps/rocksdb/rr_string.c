#include "../../src/server.h"
#include "rr.h"
#include "rocksredis.h"
#include <stdio.h>
#include "stdlib.h"
#include "magnet.h"


int RRstrlenCommand(client *c){

    const char * s=RRget(c->db->id,DBSTRING,c->argv[1]->ptr);
    if(strcasecmp(CkInvalidArgument,s)==0){//db not exist
      s="";
    }else if(strcasecmp(CkIOError,s)==0 ){//db open err           
       while(strcasecmp(CkIOError,s)==0){
            s = RRget(c->db->id,DBSTRING,c->argv[1]->ptr);
            msleep(300);
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

int RRsetCommand(client *c){
    int ret=1;
    while(ret){
       ret = RRset(c->db->id,DBSTRING,c->argv[1]->ptr,c->argv[2]->ptr);
    } 
    addReply(c, ret ? shared.ok : shared.err);
    return 1;
}

int RRgetCommand(client *c){
    const char * s=RRget(c->db->id,DBSTRING,c->argv[1]->ptr);
    if(strcasecmp(CkInvalidArgument,s)==0){//db not exist
       s="";
    }else if(strcasecmp(CkIOError,s)==0 ){//db open err           
        while(strcasecmp(CkIOError,s)==0){
             s = RRget(c->db->id,DBSTRING,c->argv[1]->ptr);
             msleep(300);
        }
    }

    int len=strlen(s)+1;
    char  val[len];
    memset(val,0,len);
    strcpy(val,s);
    robj cf_kobj;
    sds val_sds=sdsnew(val);
    initStaticStringObject(cf_kobj, val_sds);
    if(s==NULL || strlen(s)==0){
        addReply(c,shared.null[c->resp]);
    }else{
        addReplyBulk(c, &cf_kobj);
    }
    sdsfree(val_sds);
    return 1;
}

