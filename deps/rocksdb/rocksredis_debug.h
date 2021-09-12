#ifndef _DEBUG_H_
#define _DEBUG_H_
#include <stdio.h>
//#define DEBUG_INFO(fmt, args...) printf("\033[33m[%s:%d]\033[0m "#fmt"\r\n", __FUNCTION__, __LINE__, ##args)

#define RSPRING_WARN(fmt, args...) printf("\033[33m"#fmt"\033[0m\r\n", ##args)
#define RSPRING_ERR(fmt, args...) printf("\033[31m"#fmt"\033[0m\r\n",  ##args)
#define RSPRING_INFO(fmt, args...) printf("\033[30m"#fmt"\033[0m\r\n", __FUNCTION__, __LINE__, ##args)
#define RSPRING_DEBUG(fmt, args...) printf("\033[33m[%s:%d]\033[0m "#fmt"\r\n", ##args)
#define RSPRING(fmt, args...) printf("\033[0m#fmt\r\n",##args)





//none         = "\033[0m"
//black        = "\033[0;30m"
//dark_gray    = "\033[1;30m"
//blue         = "\033[0;34m"
//light_blue   = "\033[1;34m"
//green        = "\033[0;32m"
//light_green -= "\033[1;32m"
//cyan         = "\033[0;36m"
//light_cyan   = "\033[1;36m"
//red          = "\033[0;31m"
//light_red    = "\033[1;31m"
//purple       = "\033[0;35m"
//light_purple = "\033[1;35m"
//brown        = "\033[0;33m"
//yellow       = "\033[1;33m"
//light_gray   = "\033[0;37m"
//white        = "\033[1;37m"


//\033[0m  关闭所有属性  
//\033[1m   设置高亮度  
//\03[4m   下划线  
//\033[5m   闪烁  
//\033[7m   反显  
//\033[8m   消隐  
//\033[30m   --   \033[37m   设置前景色  
//\033[40m   --   \033[47m   设置背景色

#endif
