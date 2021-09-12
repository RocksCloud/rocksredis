#ifndef __ROCKSREDIS_H
#define __ROCKSREDIS_H
int rocksredisdb(void);
void rocksredisinit(int cluster_enabled);
int rocksrediscommand(client *c);

#endif

