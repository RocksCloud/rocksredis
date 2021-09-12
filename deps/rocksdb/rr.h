#ifndef __RR_H
#define __RR_H

#include <stdio.h>

#define printf PR_DEBUG 
#define PR_DEBUG(fmt,args...) /*do nothing */

#define DBSTRING 0
#define DBLIST 1
#define DBSET 2
#define DBZSET 3      
#define DBHASH 4
#define DBSTREAM 5
#define DBGEO 6
#define DB_KEY  7

typedef int rocksredisCommandProc(client *c);
struct rocksredisCommand {
    char *name;
    rocksredisCommandProc *proc;
    int id;
    int type;
};

struct RRServer{
    dict * commands;
    struct rocksredisCommand *RRhsetnxCommand,*RRhsetCommand,*RRhgetCommand,*RRhscanCommand,*RRdelCommand,*RRhdelCommand,*RRhlenCommand
        ,*RRhexistsCommand,*RRhgetallCommand,*RRhvalsCommand,*RRhkeysCommand,*RRhmgetCommand,*RRscanCommand;

    list * rrdbkeys;
    dict * rrdbs;
    dict * customKeyCount;
    dict *rrmembuf[3];//max_rrmem_num
    int rrTobatchmembuf;// batch cache membuf num
    int rrToWritemembuf;// wirte batch membuf num
    
};


struct RRServer rrserver;

/*rr commands*/
int RRhsetnxCommand(client *c);
int RRhsetCommand(client *c);
int RRhgetCommand(client *c);
int RRhscanCommand(client *c);
int RRdelCommand(client *c);
int RRhdelCommand(client *c);
int RRhlenCommand(client *c);
int RRhstrlenCommand(client *c);
int RRhexistsCommand(client *c);
int RRhgetallCommand(client *c);
int RRhvalsCommand(client *c);
int RRhkeysCommand(client *c);
int RRhmgetCommand(client *c);
int RRscanCommand(client *c);
int RRtypeCommand(client *c);
int RRsaddCommand(client *c);
int RRscardCommand(client *c);
int RRsismemberCommand(client *c);
int RRspopCommand(client *c);
int RRsinterCommand(client *c);
int RRsscanCommand(client *c);
int RRsremCommand(client *c);
int RRstrlenCommand(client *c);
int RRsetCommand(client *c);
int RRgetCommand(client *c);

int RRhsetnxBatchFlush(int id,char *skey);


/*redis fn*/
unsigned long ACLGetCommandID(const char *cmdname);
void addReplyError(client *c, const char *err);
void *dictFetchValue(dict *d, const void *key) ;
dictType commandTableDictType;
void populateRRCommandTable();


/*rocksredis*/
struct rocksredisCommand *rocksredislookupCommand(sds name);
struct rocksredisCommand *rocksredislookupCommandByCString(char *s);
void RRinit();
int rocksredisReg(client *c,int id,int type,char * key);

char *strrpc(char *str,char *oldstr,char *newstr);


#endif

