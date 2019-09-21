#include <tpool.h>
#include <memory>
extern tpool::thread_pool *srv_thread_pool;
extern std::unique_ptr<tpool::timer> srv_master_timer;
extern std::unique_ptr<tpool::timer> srv_error_monitor_timer;
extern std::unique_ptr<tpool::timer> srv_monitor_timer;

#define SRV_MONITOR_TIMER_PERIOD 5000
static inline void srv_monitor_timer_schedule_now()
{
	srv_monitor_timer->set_time(0, SRV_MONITOR_TIMER_PERIOD);
}
static void srv_start_periodic_timer(
	std::unique_ptr<tpool::timer>& timer, 
	void (*func)(void*),
	int period)
{
	static int cnt;
	timer.reset(srv_thread_pool->create_timer({ func,nullptr }));
	/*
	Make sure not all timers fire at the same time to reduce nr
	of pool threads; by introducing a tiny initial delay.
	*/
	timer->set_time((cnt++)%10, period);
}

void srv_thread_pool_init();
void srv_thread_pool_end();
