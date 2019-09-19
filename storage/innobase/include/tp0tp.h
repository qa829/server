#include <tpool.h>
extern tpool::thread_pool *srv_thread_pool;
void srv_thread_pool_init();
void srv_thread_pool_end();