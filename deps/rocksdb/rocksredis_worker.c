#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <net/if.h>
#include <linux/sockios.h>
#include <pthread.h>
#include <signal.h>
#include <semaphore.h>

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>


/* 
*All the running and waiting tasks in the thread pool are a ctthread_ worker
*Since all tasks are in a linked list, it is a linked list structure
*/
typedef struct worker
{
    /*Callback function, which will be called when the task is running. Note that it can also be declared in other forms*/
    void *(*process)(void *arg);
    void *arg; /*The parameters of the callback function*/
    struct worker *next;

} CThread_worker;


/*Thread pool structure*/
typedef struct
{
    pthread_mutex_t queue_lock;
    pthread_cond_t queue_ready;

    /*Linked list structure, all waiting tasks in thread pool*/
    CThread_worker *queue_head;

    /*Destroy thread pool*/
    int shutdown;
    pthread_t *threadid;
    /*The number of active threads allowed in the thread pool*/
    int max_thread_num;
    /*The number of tasks currently waiting on the queue*/
    int cur_queue_size;

} CThread_pool;

static CThread_pool *pool = NULL;


/***************************************/
//thread pool 
/****************************************/
int pool_add_worker(void *(*process)(void *arg), void *arg);
void return_code(int code, char *send_msg);
void * thread_routine(void *arg);
void pool_init(int max_thread_num);
int pool_destroy();


/***************************************/
//thread init
/****************************************/
void pool_init(int max_thread_num){
    pool = (CThread_pool *)malloc(sizeof(CThread_pool));
    pthread_mutex_init(&(pool->queue_lock), NULL);
    pthread_cond_init(&(pool->queue_ready), NULL);
    pool->queue_head = NULL;
    pool->max_thread_num = max_thread_num;
    pool->cur_queue_size = 0;
    pool->shutdown = 0;
    pool->threadid = (pthread_t *)malloc(max_thread_num * sizeof(pthread_t));
    int i = 0;
    for (i = 0; i < max_thread_num; i++)
    {
        pthread_create(&(pool->threadid[i]), NULL, thread_routine, NULL);
    }
}


/***************************************/
/*Add tasks to thread pool*/
/****************************************/

int pool_add_worker(void *(*process)(void *arg), void *arg)
{
    /*Construct a new task*/
    CThread_worker *newworker = (CThread_worker *)malloc(sizeof(CThread_worker));
    newworker->process = process;
    newworker->arg = arg;
    newworker->next = NULL; /*Don't forget to leave it empty*/

    pthread_mutex_lock(&(pool->queue_lock));
    /*Add the task to the waiting queue*/
    CThread_worker *member = pool->queue_head;
    if (member != NULL)
    {
        while (member->next != NULL)
            member = member->next;
        member->next = newworker;
    }
    else
    {
        pool->queue_head = newworker;
    }

    assert(pool->queue_head != NULL);

    pool->cur_queue_size++;
    pthread_mutex_unlock(&(pool->queue_lock));
    /*There is a task in the waiting queue. Wake up a waiting thread*/
    pthread_cond_signal(&(pool->queue_ready));
    return 0;
}

/***************************************/
/*Destroy the thread pool. The tasks in the waiting queue will not be executed, but the running threads will be executed all the time
Run the task and then exit*/
/****************************************/

int pool_destroy()
{
    if (pool->shutdown)
        return -1; /*Prevent two calls*/
    pool->shutdown = 1;

    /*Wake up all waiting threads. The thread pool will be destroyed*/
    pthread_cond_broadcast(&(pool->queue_ready));

   /*Block and wait for the thread to exit, otherwise it will become a zombie*/
    int i;
    for (i = 0; i < pool->max_thread_num; i++)
        pthread_join(pool->threadid[i], NULL);
    free(pool->threadid);

    /*Destroy waiting queue*/
    CThread_worker *head = NULL;
    while (pool->queue_head != NULL)
    {
        head = pool->queue_head;
        pool->queue_head = pool->queue_head->next;
        free(head);
    }
    /*Don't forget to destroy conditional variables and mutexes*/
    pthread_mutex_destroy(&(pool->queue_lock));
    pthread_cond_destroy(&(pool->queue_ready));

    free(pool);
    /*It's a good habit to leave the pointer empty after destruction*/
    pool = NULL;
    return 0;
}


void * thread_routine(void *arg)
{
    while (1)
    {
        pthread_mutex_lock(&(pool->queue_lock));
        
        /*If the waiting queue is 0 and the thread pool is not destroyed, it is blocked; note that
         pthread_ cond_ Wait is an atomic operation that unlocks before waiting and locks after waking up*/
        while (pool->cur_queue_size == 0 && !pool->shutdown)
        {
            pthread_cond_wait(&(pool->queue_ready), &(pool->queue_lock));
        }

        /*The thread pool is about to be destroyed*/
        if (pool->shutdown)
        {
          /*When you encounter jump statements such as break, continue and return, don't forget to unlock them first*/
            pthread_mutex_unlock(&(pool->queue_lock));
            pthread_exit(NULL);
        }


        /*Assert is a good helper for debugging*/
        assert(pool->cur_queue_size != 0);
        assert(pool->queue_head != NULL);

        /*Wait for the queue length minus 1, and take out the header element in the list*/
        pool->cur_queue_size--;
        CThread_worker *worker = pool->queue_head;
        pool->queue_head = worker->next;
        pthread_mutex_unlock(&(pool->queue_lock));

        /*Call the callback function to perform the task*/
        (*(worker->process))(worker->arg);
        free(worker);
        worker = NULL;
    }
   
    pthread_exit(NULL);
}

void return_code(int code, char *send_msg)
{

    switch (code)
    {
    case 1:
    {
        send_msg = "0x01";
        break;
    }
    case 2:
    {
        send_msg = "0x02";
        break;
    }
    default:
        break;
    }
}

void init_pools(int pool_worker_num){
    pool_init(pool_worker_num); 

}



