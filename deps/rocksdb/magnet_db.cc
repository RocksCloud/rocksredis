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
/*============================redis reference========================*/
extern "C" {
    #include "../../src/adlist.h"
    #include "../../src/dict.h"
    typedef char *sds;
  
}
#include "magnet.h"
 magnet rsmagnet;

 extern "C"   listNode *listSearchKey(list *list, void *key);
 extern "C"   list *listAddNodeTail(list *list, void *value);
 extern "C"   sds sdsnew(const char *init);
 extern "C"   void *zmalloc(size_t size) ;
/*============================magnet db reference========================*/
 extern "C" const char*  getDBTypeStrpath(int id,int type);
 extern "C" std::string getDBTypepath(int id,int type);
 extern "C" void getDBKeyStrpath(int id,char * key,char *gpath);
 extern "C" std::string getDBKeypath(int id,char * key);
 extern "C" const char* getDBStrpath(int id);
 extern "C" std::string getDBpath(int id);
 extern "C" int RRInitDB();
 extern "C" int RRdbNum(void);
 extern "C" void RRgetOptions(struct Options *op);
 extern "C" int RRGetrrdbObj(char *dbcustom_key,int keytype,rrdbObj *rrobj);
 extern "C" int RRInitrrdbs(list  * ldbs);
 extern "C" const char * RRDBcustomVal(char *dbcustom_key);
 extern "C" int RRUpdateCustom(const char *dbcustom_key,int op,int type);
 extern "C" int RRcreateDB(int id,int type,char *key);
 extern "C" const char * getDBTypeKeyStrPath(int id,int type,char *key);
 extern "C" std::string getDBTypeKeyPath(int id,int type,char *key);
 extern "C" void RRdeleteDB(int id,int type,char *key);
 extern "C" void RRcreateStringDB(int id,int type,char *key);
 extern "C" int RRGetrrStringdbObj(char *dbcustom_key,int keytype,rrdbObj *rrobj);
 extern "C" int RRsyncKeyCountToCustom(const char *dbcustom_key,int c,int op);
 extern "C" void RROptions(struct Options *op);
 extern "C" void RRwriteOptions(struct WriteOptions *op);
 extern "C" void RRreadOptions(struct ReadOptions *op);
 extern "C" int RRinitCustomKeyCount(dict *cKeycounts);
 extern "C" int RRcheckIsdigitStr(std::string str);
 extern "C" const char * RRFetchBeginLastKey( char *src,char c,char *out);
 extern "C" void Test();
 /*==magenet string==*/
 extern "C" int RRstringDel(int id,int type,char *key);
 
 //DB0
std::string DB0STRING = "/rocksredis/db0/string/";
std::string DB0SET =    "/rocksredis/db0/set/";
std::string DB0ZSET =   "/rocksredis/db0/zset/";
std::string DB0LIST =   "/rocksredis/db0/list/";
std::string DB0HASH =   "/rocksredis/db0/hash/";
std::string DB0STREAM=  "/rocksredis/db0/stream/";
std::string DB0GEO=     "/rocksredis/db0/geo/";
std::string DB0DEFAULT= "/rocksredis/db0/default/";


//DB1
std::string DB1STRING = "/rocksredis/db1/string/";
std::string DB1SET =    "/rocksredis/db1/set/";
std::string DB1ZSET =   "/rocksredis/db1/zset/";
std::string DB1LIST =   "/rocksredis/db1/list/";
std::string DB1HASH =   "/rocksredis/db1/hash/";
std::string DB1STREAM = "/rocksredis/db1/stream/";
std::string DB1GEO =    "/rocksredis/db1/geo/";
std::string DB1DEFAULT= "/rocksredis/db1/default/";

//DB2
std::string DB2STRING = "/rocksredis/db2/string/";
std::string DB2SET =    "/rocksredis/db2/set/";
std::string DB2ZSET =   "/rocksredis/db2/zset/";
std::string DB2LIST =   "/rocksredis/db2/list/";
std::string DB2HASH =   "/rocksredis/db2/hash/";
std::string DB2STREAM = "/rocksredis/db2/stream/";
std::string DB2GEO =    "/rocksredis/db2/geo/";
std::string DB2DEFAULT= "/rocksredis/db2/default/";


//DB3
std::string DB3STRING = "/rocksredis/db3/string/";
std::string DB3SET =    "/rocksredis/db3/set/";
std::string DB3ZSET =   "/rocksredis/db3/zset/";
std::string DB3LIST =   "/rocksredis/db3/list/";
std::string DB3HASH =   "/rocksredis/db3/hash/";
std::string DB3STREAM = "/rocksredis/db3/stream/";
std::string DB3GEO =    "/rocksredis/db3/geo/";
std::string DB3DEFAULT= "/rocksredis/db3/default/";
    

//DB4
std::string DB4STRING = "/rocksredis/db4/string/";
std::string DB4SET =    "/rocksredis/db4/set/";
std::string DB4ZSET =   "/rocksredis/db4/zset/";
std::string DB4LIST =   "/rocksredis/db4/list/";
std::string DB4HASH =   "/rocksredis/db4/hash/";
std::string DB4STREAM = "/rocksredis/db4/stream/";
std::string DB4GEO =    "/rocksredis/db4/geo/";
std::string DB4DEFAULT= "/rocksredis/db4/default/";


//DB5
std::string DB5STRING = "/rocksredis/db5/string/";
std::string DB5SET =    "/rocksredis/db5/set/";
std::string DB5ZSET =   "/rocksredis/db5/zset/";
std::string DB5LIST =   "/rocksredis/db5/list/";
std::string DB5HASH =   "/rocksredis/db5/hash/";
std::string DB5STREAM = "/rocksredis/db5/stream/";
std::string DB5GEO =    "/rocksredis/db5/geo/";
std::string DB5DEFAULT= "/rocksredis/db5/default/";


//DB6
std::string DB6STRING = "/rocksredis/db6/string/";
std::string DB6SET =    "/rocksredis/db6/set/";
std::string DB6ZSET =   "/rocksredis/db6/zset/";
std::string DB6LIST =   "/rocksredis/db6/list/";
std::string DB6HASH =   "/rocksredis/db6/hash/";
std::string DB6STREAM = "/rocksredis/db6/stream/";
std::string DB6GEO =    "/rocksredis/db6/geo/";
std::string DB6DEFAULT= "/rocksredis/db6/default/";


//DB7
std::string DB7STRING = "/rocksredis/db7/string/";
std::string DB7SET =    "/rocksredis/db7/set/";
std::string DB7ZSET =   "/rocksredis/db7/zset/";
std::string DB7LIST =   "/rocksredis/db7/list/";
std::string DB7HASH =   "/rocksredis/db7/hash/";
std::string DB7STREAM = "/rocksredis/db7/stream/";
std::string DB7GEO =    "/rocksredis/db7/geo/";
std::string DB7DEFAULT= "/rocksredis/db7/default/";


//DB8
std::string DB8STRING = "/rocksredis/db8/string/";
std::string DB8SET =    "/rocksredis/db8/set/";
std::string DB8ZSET =   "/rocksredis/db8/zset/";
std::string DB8LIST =   "/rocksredis/db8/list/";
std::string DB8HASH =   "/rocksredis/db8/hash/";
std::string DB8STREAM = "/rocksredis/db8/stream/";
std::string DB8GEO =    "/rocksredis/db8/geo/";
std::string DB8DEFAULT= "/rocksredis/db8/default/";


//DB9
std::string DB9STRING = "/rocksredis/db9/string/";
std::string DB9SET =    "/rocksredis/db9/set/";
std::string DB9ZSET =   "/rocksredis/db9/zset/";
std::string DB9LIST =   "/rocksredis/db9/list/";
std::string DB9HASH =   "/rocksredis/db9/hash/";
std::string DB9STREAM = "/rocksredis/db9/stream/";
std::string DB9GEO =    "/rocksredis/db9/geo/";
std::string DB9DEFAULT= "/rocksredis/db9/default/";

//DB10
std::string DB10STRING = "/rocksredis/db10/string/";
std::string DB10SET =    "/rocksredis/db10/set/";
std::string DB10ZSET =   "/rocksredis/db10/zset/";
std::string DB10LIST =   "/rocksredis/db10/list/";
std::string DB10HASH =   "/rocksredis/db10/hash/";
std::string DB10STREAM = "/rocksredis/db10/stream/";
std::string DB10GEO =    "/rocksredis/db10/geo/";
std::string DB10DEFAULT= "/rocksredis/db10/default/";


//DB11
std::string DB11STRING = "/rocksredis/db11/string/";
std::string DB11SET =    "/rocksredis/db11/set/";
std::string DB11ZSET =   "/rocksredis/db11/zset/";
std::string DB11LIST =   "/rocksredis/db11/list/";
std::string DB11HASH =   "/rocksredis/db11/hash/";
std::string DB11STREAM = "/rocksredis/db11/stream/";
std::string DB11GEO =    "/rocksredis/db11/geo/";
std::string DB11DEFAULT= "/rocksredis/db11/default/";


//DB12
std::string DB12STRING = "/rocksredis/db12/string/";
std::string DB12SET =    "/rocksredis/db12/set/";
std::string DB12ZSET =   "/rocksredis/db12/zset/";
std::string DB12LIST =   "/rocksredis/db12/list/";
std::string DB12HASH =   "/rocksredis/db12/stream/";
std::string DB12STREAM = "/rocksredis/db12/geo/";
std::string DB12GEO =    "/rocksredis/db12/hash/";
std::string DB12DEFAULT= "/rocksredis/db12/default/";


//DB13
std::string DB13STRING = "/rocksredis/db13/string/";
std::string DB13SET =    "/rocksredis/db13/set/";
std::string DB13ZSET =   "/rocksredis/db13/zset/";
std::string DB13LIST =   "/rocksredis/db13/list/";
std::string DB13HASH =   "/rocksredis/db13/hash/";
std::string DB13STREAM = "/rocksredis/db13/stream/";
std::string DB13GEO =    "/rocksredis/db13/geo/";
std::string DB13DEFAULT= "/rocksredis/db13/default/";


//DB14
std::string DB14STRING = "/rocksredis/db14/string/";
std::string DB14SET =    "/rocksredis/db14/set/";
std::string DB14ZSET =   "/rocksredis/db14/zset/";
std::string DB14LIST =   "/rocksredis/db14/list/";
std::string DB14HASH =   "/rocksredis/db14/hash/";
std::string DB14STREAM = "/rocksredis/db14/stream/";
std::string DB14GEO =    "/rocksredis/db14/geo/";
std::string DB14DEFAULT= "/rocksredis/db14/default/";


//DB15
std::string DB15STRING = "/rocksredis/db15/string/";
std::string DB15SET =    "/rocksredis/db15/set/";
std::string DB15ZSET =   "/rocksredis/db15/zset/";
std::string DB15LIST =   "/rocksredis/db15/list/";
std::string DB15HASH =   "/rocksredis/db15/hash/";
std::string DB15STREAM = "/rocksredis/db15/stream/";
std::string DB15GEO =    "/rocksredis/db15/geo/";
std::string DB15DEFAULT= "/rocksredis/db15/default/";

//custom
std::string DBCUSTOM = "/rocksredis/custom/";

std::string dbdefaultlist[]={
    DB0DEFAULT,DB1DEFAULT,DB2DEFAULT,DB3DEFAULT,DB4DEFAULT,DB5DEFAULT,DB6DEFAULT,DB7DEFAULT,DB8DEFAULT,DB9DEFAULT,DB10DEFAULT,DB11DEFAULT,
    DB12DEFAULT,DB13DEFAULT,DB14DEFAULT,DB15DEFAULT
};


std::string DB0=    "/rocksredis/db0/";
std::string DB1=    "/rocksredis/db1/";
std::string DB2=    "/rocksredis/db2/";
std::string DB3=    "/rocksredis/db3/";
std::string DB4=    "/rocksredis/db4/";
std::string DB5=    "/rocksredis/db5/";
std::string DB6=    "/rocksredis/db6/";
std::string DB7=    "/rocksredis/db7/";
std::string DB8=    "/rocksredis/db8/";
std::string DB9=    "/rocksredis/db9/";
std::string DB10=    "/rocksredis/db10/";
std::string DB11=    "/rocksredis/db11/";
std::string DB12=    "/rocksredis/db12/";
std::string DB13=    "/rocksredis/db13/";
std::string DB14=    "/rocksredis/db14/";
std::string DB15=    "/rocksredis/db15/";


std::string dblist[]={
    DB0,DB1,DB2,DB3,DB4,DB5,DB6,DB7,DB8,DB9,DB10,DB11,DB12,DB13,DB14,DB15
};

std::string _DBSTRING = "/string/";
std::string _DBSET =    "/set/";
std::string _DBZSET =   "/zset/";
std::string _DBLIST =   "/list/";
std::string _DBHASH =   "/hash/";
std::string _DBSTREAM = "/stream/";
std::string _DBGEO =    "/geo/";


std::string typelist[]={
    _DBSTRING,_DBLIST,_DBSET,_DBZSET,_DBHASH,_DBSTREAM,_DBGEO
};
    
//rocksdblist
std::string dbtypelist[]={
    DB0STRING,
	DB0LIST,
    DB0SET,  
    DB0ZSET,      
    DB0HASH,
    DB0STREAM,
    DB0GEO,
    DB0DEFAULT,
    
	DB1STRING,
	DB1LIST,
	DB1SET,   
	DB1ZSET,  	  
	DB1HASH,  
	DB1STREAM,
	DB1GEO,
	DB1DEFAULT,
	
	DB2STRING,
	DB2LIST,	
	DB2SET,   
	DB2ZSET,  
	DB2HASH,  
	DB2STREAM,
	DB2GEO,
	DB2DEFAULT,
	
	DB3STRING,
	DB3LIST,
	DB3SET,   
	DB3ZSET,  
	DB3HASH,  
	DB3STREAM,
	DB3GEO,
	DB3DEFAULT,
	
	DB4STRING,
	DB4LIST,
	DB4SET,   
	DB4ZSET,  
	DB4HASH,
	DB4STREAM,
	DB4GEO,
	DB4DEFAULT,
	
	DB5STRING,
	DB5LIST,
	DB5SET,   
	DB5ZSET,  
	DB5HASH,
	DB5STREAM,
	DB5GEO,
	DB5DEFAULT,
	
	DB6STRING,
	DB6LIST,
	DB6SET,   
	DB6ZSET,  
	DB6HASH, 
	DB6STREAM,
	DB6GEO,
	DB6DEFAULT,
	
	DB7STRING,
	DB7LIST,
	DB7SET,
	DB7ZSET,
	DB7HASH,
	DB7STREAM,
	DB7GEO,
	DB7DEFAULT,
	
    DB8STRING,
	DB8LIST,
    DB8SET,   
    DB8ZSET,  
    DB8HASH, 
    DB8STREAM,
    DB8GEO,
    DB8DEFAULT,
    
	DB9STRING,
	DB9LIST,
	DB9SET,   
	DB9ZSET,  	  
	DB9HASH, 
	DB9STREAM,
	DB9GEO,
	DB9DEFAULT,
	
	DB10STRING,
	DB10LIST,
	DB10SET,   
	DB10ZSET,  	  
	DB10HASH, 
	DB10STREAM,
	DB10GEO,
	DB10DEFAULT,
	
	DB11STRING,
	DB11LIST,
	DB11SET,   
	DB11ZSET,  	  
	DB11HASH,  
	DB11STREAM,
	DB11GEO,
	DB11DEFAULT,
	
	DB12STRING,
	DB12LIST,
	DB12SET,   
	DB12ZSET,  	  
	DB12HASH,  
	DB12STREAM,
	DB12GEO,
	DB12DEFAULT,
	
	DB13STRING,
	DB13LIST,
	DB13SET,   
	DB13ZSET,  	  
	DB13HASH, 
	DB13STREAM,
	DB13GEO,
	DB13DEFAULT,
	
	DB14STRING,
	DB14LIST,
	DB14SET,   
	DB14ZSET,  	  
	DB14HASH,
	DB14STREAM,
	DB14GEO,
	DB14DEFAULT,
	
	DB15STRING,
	DB15LIST,
	DB15SET,
	DB15ZSET,	
	DB15HASH,
	DB15STREAM,
	DB15GEO,
	DB15DEFAULT,
	DBCUSTOM
};

int RRcheckIsdigitStr(std::string str){
    int count=0;
    int len = str.length();
    for(int j = 0;j < len;j++){
         if(isdigit(str[j])){ 
             count++;
         }
    }

    if(count==len) return 1;
    else return 0;

}

void RRcreateStringDB(int id,int type,char *key){
    const char *dbtypepath;
    char dbbuf[512];
    memset(dbbuf,0,sizeof(dbbuf));
    dbtypepath=getDBTypeStrpath(id, type);
    printf("createStringdb=%s\r\n",dbtypepath);
    strcat(dbbuf,rsmagnet.rocksredis_db_dir);
    strcat(dbbuf,dbtypepath);
    strcat(dbbuf,"dbsting");
    
    if (access(dbbuf, F_OK) == -1){
       memset(dbbuf,0,sizeof(dbbuf));
       strcat(dbbuf,"mkdir  ");
       strcat(dbbuf,rsmagnet.rocksredis_db_dir);
       strcat(dbbuf,dbtypepath);
       strcat(dbbuf,"dbsting");
       strcat(dbbuf,"  -p");   
       system(dbbuf);
       std::string DBcustomKey=getDBpath(id)+key;
       RRUpdateCustom(DBcustomKey.c_str(),RR_OP_KEY_ADD,type);
    }

}

/* create db
1、DB+TYPE+DB_key-key
2、DB+TYPE+DB_key-custom
*/
int RRcreateDB(int id,int type,char *key){
    const char *dbtypekeypath;
    Status s;
    dbtypekeypath=getDBTypeKeyStrPath(id, type, key);
    if (access(dbtypekeypath, F_OK) == -1){
      
        char dbbuf[512];
        char custom[512];
        char dbkey[512];
        char customkey[512];
        
        memset(dbbuf,0,sizeof(dbbuf));
        memset(custom,0,sizeof(custom));
        memset(dbkey,0,sizeof(dbkey));
        memset(customkey,0,sizeof(customkey));
        strcat(dbkey,rsmagnet.rocksredis_db_dir);
        strcat(dbkey,dbtypekeypath);
        strcat(dbkey,"/");
        strcat(dbkey,key);
        printf("create db =%s\r\n",dbkey);
        
        strcat(customkey,rsmagnet.rocksredis_db_dir);
        strcat(customkey,dbtypekeypath);
        strcat(customkey,"/");
        strcat(customkey,"custom");
          
        strcat(dbbuf,"mkdir  ");
        strcat(dbbuf,rsmagnet.rocksredis_db_dir);
        strcat(dbbuf,dbtypekeypath);
        strcat(dbbuf,"/");
        strcat(custom,dbbuf);
        strcat(dbbuf,key);
        strcat(custom,"custom");
//        printf("createdb custom=%s\r\n",custom);
        strcat(dbbuf,"  -p");   
        strcat(custom,"  -p");
        system(dbbuf);
        system(custom);

        //注册DB
        std::string DBcustomKey=getDBpath(id)+key;
        RRUpdateCustom(DBcustomKey.c_str(),RR_OP_KEY_ADD,type);
        //创建并初始化DB

        Options options;
        
        RRgetOptions(&options);
        DB* db;
        options.create_if_missing = true;
        s = DB::Open(options, dbkey, &db);
        if(!s.ok()){
            delete db;
            printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.ToString().c_str()); 
            return s.code();
        }

        delete db;

        s = DB::Open(options, customkey, &db);
        if(!s.ok()){
            delete db;
            printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.ToString().c_str()); 
            return  s.code();
        }

        delete db;

        return s.Code::kOk;
    }

     return s.Code::kOk;
}
void RRdeleteDB(int id,int type,char *key){
    const char *dbtypekeypath;
    char dbbuf[512];
    if(type==0){//DBSTRING
        int ret = 1;
        while(ret){ 
            ret = RRstringDel(id,type,key);
        }
        return ;
    }
    dbtypekeypath=getDBTypeKeyStrPath(id, type, key);
    memset(dbbuf,0,sizeof(dbbuf));
    strcat(dbbuf,"rm -rf  ");
    strcat(dbbuf,rsmagnet.rocksredis_db_dir);
    strcat(dbbuf,dbtypekeypath);
    strcat(dbbuf,"/*");
    system(dbbuf);
}

/*=====================DB+TYPE+DB_KEY char===============*/
const char * getDBTypeKeyStrPath(int id,int type,char *key){
    std::string path=getDBTypeKeyPath(id,type,key);
    return path.c_str();

}

/*=====================DB+TYPE+DB_KEY string=============*/
std::string getDBTypeKeyPath(int id,int type,char *key){
    char fullkeypath[256];
    memset(fullkeypath,0,sizeof(fullkeypath));
        
    std::string path=getDBTypepath(id,type);
    strcat(fullkeypath,path.c_str());
    strcat(fullkeypath,"db_");
    strcat(fullkeypath,key);
//    std::string keypath=path+"db_"+key;
    return fullkeypath;
}

/*=====================DB+TYPE char=======================*/
const char*  getDBTypeStrpath(int id,int type){
    std::string path=getDBTypepath(id,type);
    return path.c_str();
}


/*=====================DB+TYPE string=====================*/
std::string getDBTypepath(int id,int type){
    if(id>16) return "none";
    return dbtypelist[id*8+type];
}


/*=====================DB+KEY char========================*/
void getDBKeyStrpath(int id,char * key,char * gpath){
    std::string path=getDBKeypath(id,key);
    strcat(gpath,path.c_str());
}

/*=====================DB+KEY string======================*/
std::string getDBKeypath(int id,char * key){
    std::string path = getDBpath(id)+key;
    return path;

}

/*=====================DB char===========================*/
const char* getDBStrpath(int id){
    std::string path=getDBpath(id);
    return path.c_str();
}


/*=====================DB string=========================*/
std::string getDBpath(int id){
     if(id>16) return "none";
     return dblist[id];
}

//做分片管理策略，当一个目录下的文件数量/存储空间超过某个值是采用分片策略，从master中clone出一个副本进行接下来的存储
//非常重要的负载策略,可以通过rocksdb自带的资源使用函数来查询，并做副本存储
//{..........}


/*=Init rocksredis DB Path and profiles=*/
int RRInitDB(){
    Status s;
    int  ret;
    char dbbuf[512];
    for(int i=0;i<16*8+1;i++){
        if (access(dbtypelist[i].c_str(), F_OK) == -1)	//创建文件目录
        {
            memset(dbbuf,0,sizeof(dbbuf));
            strcat(dbbuf,"mkdir  ");
            strcat(dbbuf,rsmagnet.rocksredis_db_dir);
            strcat(dbbuf,dbtypelist[i].c_str());
            strcat(dbbuf,"  -p");                
        	system(dbbuf);
        }
    }
    return 0;
} 


/*= DB write options =*/
void RRwriteOptions(struct WriteOptions *op){
    op->disableWAL=true;//如果不在乎数据安全性, 设置为 true，加快写吞吐率
    op->sync=false;//异步写
    

}
/*= DB read options ,config.c=*/
void RRreadOptions(struct ReadOptions *op){
    op->verify_checksums = false;//

}

/*= DB Open options =*/
void RROptions(struct Options *op){
//    //rsmagnet.max_open_files;//官方建议把这个选项设置为 -1，以方便 RocksDB 加载所有的 index 和 filter 文件，最大化程序性能
    op->max_open_files = -1;

    //加快DB打开时间
//    op->skip_stats_update_on_db_open=true;
    op->compaction_style =rocksdb::CompactionStyle::kCompactionStyleLevel;
    //这个值默认是 2，当一个 buffer 的数据被 flush 到磁盘上的时候，RocksDB 就用另一个 buffer 作为数据读写缓冲区。
    op->max_write_buffer_number = 5;
    //落盘前需要合并的memtable的最小数量
    op->min_write_buffer_number_to_merge =2;
    
     //指定了一个写内存 buffer 的大小，当这个 buffer 写满之后数据会被固化到磁盘上。这个值越大批量写入的性能越好
    op->write_buffer_size = 256 << 20;//512MB
    op->target_file_size_base =256 << 20;//512MB
    op->max_bytes_for_level_base = 512 << 20;//512MB

    
    op->level0_file_num_compaction_trigger = 10;
    op->level0_slowdown_writes_trigger = 20;
    op->level0_stop_writes_trigger =40;
    op->max_bytes_for_level_multiplier = 10;

    op->env->SetBackgroundThreads(2,op->env->LOW);//2 low
    op->env->SetBackgroundThreads(1,op->env->HIGH);//1
    op->compression = rocksdb::CompressionType::kNoCompression;//关闭压缩//rocksdb::kLZ4Compression;
    //为落盘并发数。默认1
    op->max_background_flushes = 2;
    //max_background_compactions为后台压缩的最大线程数。默认为1，但是为了完全利用CPU和存储，你可能会希望增加这个到接近系统的核的数量
    op->max_background_compactions = 4;

    //sst block size 
    BlockBasedTableOptions table_options;
    table_options.block_size = 16 * 1024;
    op->table_factory.reset(NewBlockBasedTableFactory(table_options));
   
//    //基于level的压缩
    op->level_compaction_dynamic_level_bytes = true;
    op->compaction_readahead_size= 256<<20;//256MB
    //打开RocksDB层的预读取
    op->new_table_reader_for_compaction_inputs = true ;

}
void RRgetOptions(struct Options *op){
//    RROptions(op);
#if 0
    op->compaction_style = (rocksdb::CompactionStyle)rsmagnet.compaction_style;
    op->write_buffer_size = (size_t)rsmagnet.write_buffer_size;
    op->max_write_buffer_number = rsmagnet.max_write_buffer_number;
    op->target_file_size_base = rsmagnet.target_file_size_base;
    op->level0_file_num_compaction_trigger = rsmagnet.level0_file_num_compaction_trigger;
    op->level0_slowdown_writes_trigger = rsmagnet.level0_slowdown_writes_trigger;
    op->level0_stop_writes_trigger = rsmagnet.level0_stop_writes_trigger;
    op->num_levels = rsmagnet.num_levels;
    op->max_bytes_for_level_base = rsmagnet.max_bytes_for_level_base;
    op->max_bytes_for_level_multiplier = rsmagnet.max_bytes_for_level_multiplier;
    op->max_open_files =    rsmagnet.max_open_files;
    op->max_background_flushes=rsmagnet.max_background_flushes;
    op->env->SetBackgroundThreads(rsmagnet.BackgroundThreads);
    op->compression = (rocksdb::CompressionType)rsmagnet.compression;
#endif
    

#if 0
    //-扩展优化配置-//
     op->env->SetBackgroundThreads(2,op->env->LOW);//2 low
     op->env->SetBackgroundThreads(1,op->env->HIGH);//1
     op->level0_file_num_compaction_trigger = 10;
     op->level0_slowdown_writes_trigger = 20;
     op->level0_stop_writes_trigger =40;
     op->max_bytes_for_level_multiplier = 10;
     
     //为落盘并发数。默认1
     op->max_background_flushes = 2;
     //max_background_compactions为后台压缩的最大线程数。默认为1，但是为了完全利用CPU和存储，你可能会希望增加这个到接近系统的核的数量
     op->max_background_compactions = 4;
    
      BlockBasedTableOptions table_options;
     table_options.block_size = 16 * 1024;
     op->table_factory.reset(NewBlockBasedTableFactory(table_options));
     op->target_file_size_base =256 << 20;//512MB
     op->level_compaction_dynamic_level_bytes = true;
     op->compaction_readahead_size= 256<<20;//256MB
     //打开RocksDB层的预读取
     op->new_table_reader_for_compaction_inputs = true ;
     //加快DB打开时间
     op->skip_stats_update_on_db_open=true;
    
     op->compaction_style =rocksdb::CompactionStyle::kCompactionStyleLevel;
     op->compression = rocksdb::CompressionType::kNoCompression;//关闭压缩//rocksdb::kLZ4Compression;
#endif

#if 1
    //--扩展优化2--//
    //default
    op->max_open_files = -1;
    op->skip_stats_update_on_db_open=true;
    op->compression = rocksdb::kLZ4Compression;
    op->num_levels=4;
    op->env->SetBackgroundThreads(4,op->env->LOW);
    op->env->SetBackgroundThreads(2,op->env->HIGH);//1
    op->min_write_buffer_number_to_merge =2;
    op->level0_file_num_compaction_trigger=4;
    //redis.conf
    op->write_buffer_size = rsmagnet.write_buffer_size;//512MB
    op->max_write_buffer_number = rsmagnet.max_write_buffer_number;
    op->max_background_flushes = rsmagnet.max_background_flushes;
    op->max_background_compactions = rsmagnet.max_background_compactions;
#endif

}

const char * RRFetchBeginLastKey( char *src,char c,char *out){
    const char *key=strrchr(src,c);
    memcpy(out,src,strlen(src)-strlen(key));
    return key;
}

int RRGetrrStringdbObj(char *dbcustom_key,int keytype,rrdbObj *rrobj){
    Options options;
    options.create_if_missing=true;
    DB* db;
    Status s;
    char * pEnd;
    char dbbuf[512];
    char dbpath[512];
    memset(dbpath,0,sizeof(dbpath));
    const char *key=RRFetchBeginLastKey(dbcustom_key,'/',dbpath);
    const char *pkey=key+1;

    memset(dbbuf,0,sizeof(dbbuf));
    strcat(dbbuf,rsmagnet.rocksredis_db_dir);
    strcat(dbbuf,dbpath);
    strcat(dbbuf,typelist[keytype].c_str());
    strcat(dbbuf,"dbsting");

    options.create_if_missing=true;
    s= DB::Open(options, dbbuf, &db);
    if(!s.ok()){
        delete db;
        printf("code:%d, %s:%d: ERR:%s\n",__FUNCTION__, __LINE__,s.code(),s.ToString().c_str()); 
        return s.code();
    }  
    ReadOptions rp=ReadOptions();
    RRreadOptions(&rp);
    std::string value;
    db->Get(rp, Slice(pkey), &value);
    if(value=="") rrobj->fieldcount=0;
    else {
        unsigned long int count= strtoul(value.c_str(),&pEnd,10);
        rrobj->fieldcount=count;
    }
    memset(rrobj->key,0,sizeof(rrobj->key));
    strcpy(rrobj->key,pkey);
    rrobj->type=keytype;
    rrobj->IscustomLoad=1;

    delete db;

    return s.Code::kOk;

}

int RRGetrrdbObj(char *dbcustom_key,int keytype,rrdbObj *rrobj){
    Options options;
    DB* db;
    Status s;
    char * pEnd;
    char dbbuf[256];
    char dbpath[256];
    memset(dbpath,0,sizeof(dbpath));
    const char *key=RRFetchBeginLastKey(dbcustom_key,'/',dbpath);
    const char *pkey=key+1;
    
    memset(dbbuf,0,sizeof(dbbuf));
    strcat(dbbuf,rsmagnet.rocksredis_db_dir);
    strcat(dbbuf,dbpath);
    strcat(dbbuf,typelist[keytype].c_str());
    strcat(dbbuf,"db_");
    strcat(dbbuf,pkey);
    
    strcat(dbbuf,"/");
    strcat(dbbuf,"custom");
    options.create_if_missing=true;
    s= DB::Open(options, dbbuf, &db);
    if(!s.ok()){
        delete db;
        printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.ToString().c_str()); 
        return s.code();
    }  
    ReadOptions rp=ReadOptions();
    RRreadOptions(&rp);
    std::string value;
    s=db->Get(rp, Slice(pkey), &value);
    if(!s.ok()) {
//        printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.ToString().c_str()); 
    }
    unsigned long int count= strtoul(value.c_str(),&pEnd,10);
    rrobj->fieldcount=count;
    memset(rrobj->key,0,sizeof(rrobj->key));
    strcpy(rrobj->key,pkey);
    rrobj->type=keytype;
    rrobj->IscustomLoad=1;
    delete db;
    return s.Code::kOk;
}


/*load all custom key count*/
int RRinitCustomKeyCount(dict *cKeycounts){
    Options options;
    Iterator* iterator;
    RRgetOptions(&options);
    Status s;
    int ret=s.Code::kOk;
    DB* db;
    options.create_if_missing=true;
    char dbbuf[512];
    memset(dbbuf,0,sizeof(dbbuf));
    strcat(dbbuf,rsmagnet.rocksredis_db_dir);
    strcat(dbbuf,DBCUSTOM.c_str());
    strcat(dbbuf,"DBCUSTOM_KEY_COUNT");
    s= DB::Open(options, dbbuf, &db);

    if(!s.ok()){
        delete db;
        printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.ToString().c_str());  
        ret=s.code();
    }
     
    ReadOptions rp=ReadOptions();
    RRreadOptions(&rp);
    rp.total_order_seek=true;
    iterator=db->NewIterator(ReadOptions());     
    std::string row, val;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
      row = iterator->key().ToString();
      val = iterator->value().ToString();
       if(RRcheckIsdigitStr(val))
           dictAdd(cKeycounts, sdsnew(row.c_str()),sdsnew(val.c_str()));
    }

    delete db;
    return ret;
}


/*load all keys*/
int RRInitrrdbs(list  * ldbs){
    Options options;
    Iterator* iterator;
    DB* db;
    Status s;
    int ret=s.Code::kOk;
    options.create_if_missing=true;
    char dbbuf[512];
    memset(dbbuf,0,sizeof(dbbuf));
    strcat(dbbuf,rsmagnet.rocksredis_db_dir);
    strcat(dbbuf,DBCUSTOM.c_str());
    strcat(dbbuf,"DBCUSTOM");
    s= DB::Open(options, dbbuf, &db);
    if(!s.ok()){
       delete db;
       printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.ToString().c_str());   
       ret = s.code();
       return ret;
   }  
    ReadOptions rp=ReadOptions();
    RRreadOptions(&rp);
    rp.total_order_seek=true;
    iterator=db->NewIterator(ReadOptions());     
    std::string row, val;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
      row = iterator->key().ToString();
      val = iterator->value().ToString();
      rrkv *kv = (rrkv*)zmalloc(sizeof(rrkv));
      kv->key = sdsnew(row.c_str());
      kv->val = sdsnew(val.c_str());
      listAddNodeTail(ldbs,kv);
    }
    
    delete db;
    return ret;
}

/*load key style*/
const char * RRDBcustomVal(char *dbcustom_key){
    Options options;
    RRgetOptions(&options);
    DB* db;
    Status s;
    int type;
    options.create_if_missing=true;
    char dbbuf[512];
    memset(dbbuf,0,sizeof(dbbuf));
    strcat(dbbuf,rsmagnet.rocksredis_db_dir);
    strcat(dbbuf,DBCUSTOM.c_str());
    strcat(dbbuf,"DBCUSTOM");
    s= DB::Open(options, dbbuf, &db);
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
    db->Get(rp, Slice(dbcustom_key),  &value);
    delete db;
 
  
    return value.c_str();

}

/*sync per dbcustom  keycount to custom CUSTOM_KEY_COUNT*/
int RRsyncKeyCountToCustom(const char *dbcustom_key,int c,int op){
    Options options;
    RRgetOptions(&options);
    DB* db;
    Status s;
    options.create_if_missing=true;
    char dbbuf[512];
    memset(dbbuf,0,sizeof(dbbuf));
    strcat(dbbuf,rsmagnet.rocksredis_db_dir);
    strcat(dbbuf,DBCUSTOM.c_str());
    strcat(dbbuf,"DBCUSTOM_KEY_COUNT");
    s= DB::Open(options, dbbuf, &db);
    
    if(!s.ok()){
         delete db;
         printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.ToString().c_str());  
         return s.code();
    }
     
    ReadOptions rp=ReadOptions();
    RRreadOptions(&rp);
    std::string value;
    s=db->Get(rp ,Slice(dbcustom_key) ,&value);
    if(!s.ok()){
//        printf(" %s:%d: ERR:%s\n",__FUNCTION__, __LINE__,s.ToString().c_str()); 
    }
    char count[128];
    char * pEnd;
    memset(count,0,sizeof(count));
    unsigned long int opcount= strtoul(value.c_str(),&pEnd,10);//10  十进制
        
    if(op==RR_OP_INCREASE){
       opcount += c;
    }
    else if(op==RR_OP_REDUCE && opcount>0){
         opcount =opcount-c;
         if(opcount<0) opcount=0;
    }

    WriteOptions wp=WriteOptions();
    RRwriteOptions(&wp);
    memset(count,0,sizeof(count));
    sprintf(count,"%ld",opcount);
    s = db->Put(wp, Slice(dbcustom_key), Slice(count));
    if(!s.ok()){  printf(" %s:%d: ERR:%s\n",__FUNCTION__, __LINE__,s.ToString().c_str());  return 0;}  

    delete db;
    return 1;
}

/*add a  key and  key style*/
int RRUpdateCustom(const char *dbcustom_key,int op,int type){
    Options options;
    RRgetOptions(&options);
    DB* db;
    Status s;
    options.create_if_missing=true;
    char dbbuf[512];
    memset(dbbuf,0,sizeof(dbbuf));
    strcat(dbbuf,rsmagnet.rocksredis_db_dir);
    strcat(dbbuf,DBCUSTOM.c_str());
    strcat(dbbuf,"DBCUSTOM");
    s= DB::Open(options, dbbuf, &db);
    if(!s.ok()){
          delete db;
          printf("code:%d, %s:%d: ERR:%s\n",s.code(),__FUNCTION__, __LINE__,s.ToString().c_str());  
          return s.code();
    }
    if(op==RR_OP_KEY_ADD || op==RR_OP_KEY_UPDATE){
        WriteOptions wp=WriteOptions();
        RRwriteOptions(&wp);
        char typestr[10];
        memset(typestr,0,sizeof(typestr));
        sprintf(typestr,"%d",type);
        s = db->Put(wp, Slice(dbcustom_key), Slice(typestr));
    }else if(op==RR_OP_KEY_DEL){
        s=db->Delete(WriteOptions(), Slice(dbcustom_key));
    }

    delete db;

    return s.Code::kOk;
}

int RRdbNum(void){
    return rsmagnet.rocksredis_select;
}


void Test(){
    std::string value="123a";
    int ret=RRcheckIsdigitStr(value);
    printf("ret:%d\r\n",ret);
    value="24";
    ret=RRcheckIsdigitStr(value);
    printf("ret:%d\r\n",ret);

}

