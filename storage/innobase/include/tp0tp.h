#include <tpool.h>
#include <memory>
extern tpool::thread_pool *srv_thread_pool;
extern std::unique_ptr<tpool::timer> srv_master_timer;
extern std::unique_ptr<tpool::timer> srv_error_monitor_timer;
void srv_thread_pool_init();
void srv_thread_pool_end();
