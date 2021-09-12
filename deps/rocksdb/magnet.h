#ifndef __MAGNET_H
#define __MAGNET_H

#include <stdio.h>

//#include "magnet_common.h"

#define printf PR_DEBUG 
#define PR_DEBUG(fmt,args...) /*do nothing */

#define msleep(x) usleep(x*1000)

typedef struct magnet{
  

    unsigned char create_if_missing;
    char * rocksredis_db_dir;

    int write_buffer_size ; 
    int max_write_buffer_number ;
    unsigned long long target_file_size_base ; 
    int level0_file_num_compaction_trigger ;
    int level0_slowdown_writes_trigger;
    int level0_stop_writes_trigger;
    int num_levels ;
    unsigned long long max_bytes_for_level_base ; 
    double max_bytes_for_level_multiplier ;
    int max_open_files ;
    int max_background_flushes;
    int max_background_compactions;
    int BackgroundThreads;
    unsigned char compaction_style;
    unsigned char compression ;
    int rocksredis_admin_port;
    int rocks_write_batch_size;
   
    int  cf_val_max_len; //默认1MB
    int  cf_name_max_len;//默认256
    int  cf_max; //默认 1024 一个DB最大的cf

    int rocksredis_select;

    int batch_write_background_threadpools;
   
}magnet;

#define RR_OP_INCREASE 0
#define RR_OP_REDUCE 1
#define RR_OP_DEF 2

#define RR_OP_KEY_ADD 0
#define RR_OP_KEY_DEL 1
#define RR_OP_KEY_UPDATE 2


extern magnet rsmagnet;

//key:db+type+db_key
//val:rrdbObj
typedef struct rrdb{
    long long fieldcount;
    unsigned char IscustomLoad;
    char backcursor[512];
    long long backcount;
    int time;
    unsigned char flag;
    int expires;
    int type;
    char key[256];
    time_t lastsave;
    int dirty;

    int countForexpire;
    
}rrdbObj;

typedef struct RRkv{
    sds key;
    sds val;
}rrkv;

typedef struct RRWriteBatch{
    int id;
    int type;
    char key[256];
    list *ld;
   
}RRWriteBatch;


#define CkOk                                 "code:kOk"
#define CkNotFound                           "code:kNotFound"
#define CkCorruption                         "code:kCorruption"
#define CkNotSupported                       "code:kNotSupported"
#define CkInvalidArgument                    "code:kInvalidArgument"
#define CkIOError                            "code:kIOError"
#define CkMergeInProgress                    "code:kMergeInProgress"
#define CkIncomplete                         "code:kIncomplete"
#define CkShutdownInProgress                 "code:kShutdownInProgress"
#define CkTimedOut                           "code:kTimedOut"
#define CkAborted                            "code:kAborted"
#define CkBusy                               "code:kBusy"
#define CkExpired                            "code:kExpired"
#define CkTryAgain                           "code:kTryAgain"
#define CkCompactionTooLarge                 "code:kCompactionTooLarge"
#define CkColumnFamilyDropped                "code:kColumnFamilyDropped"
#define CkMaxCode                            "code:kMaxCode"

#endif

