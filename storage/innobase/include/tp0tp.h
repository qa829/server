#include <tpool.h>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <queue>

#pragma once

extern tpool::thread_pool *srv_thread_pool;
extern std::unique_ptr<tpool::timer> srv_master_timer;
extern std::unique_ptr<tpool::timer> srv_error_monitor_timer;
extern std::unique_ptr<tpool::timer> srv_monitor_timer;

#define SRV_MONITOR_TIMER_PERIOD 5000
static inline void srv_monitor_timer_schedule_now()
{
	srv_monitor_timer->set_time(0, SRV_MONITOR_TIMER_PERIOD);
}
static inline void srv_start_periodic_timer(
	std::unique_ptr<tpool::timer>& timer, 
	void (*func)(void*),
	int period)
{
	timer.reset(srv_thread_pool->create_timer(func, 0, 0));
	timer->set_time(0, period);
}

void srv_thread_pool_init();
void srv_thread_pool_end();
