#include <tpool.h>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <queue>

extern tpool::thread_pool *srv_thread_pool;
extern std::unique_ptr<tpool::timer> srv_master_timer;
extern std::unique_ptr<tpool::timer> srv_error_monitor_timer;
extern std::unique_ptr<tpool::timer> srv_monitor_timer;
extern std::unique_ptr<tpool::execution_environment> srv_timers_env;

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
  timer.reset(srv_thread_pool->create_timer({ func, nullptr, srv_timers_env.get() }));
	timer->set_time(0, period);
}

/* 
Helper function for tasks that do not run concurrently
(buf_pool_resize, buf_dump_now etc)

Using tpool execution environment, with max_concurrency = 1
will do the trick to prevent concurrent execution, without
mutexes.
*/

static inline tpool::execution_environment* create_single_thread_execution_env()
{
  tpool::execution_environment* env = tpool::create_execution_environment();
  env->set_max_concurrency(1);
  return env;
}

/* A task that can be waited for. Multiple submit_task() will be ignored if there a pending ones.*/
struct waitable_task
{
  std::mutex m_mtx;
  std::condition_variable m_cv;
  enum {
    NONE,
    PENDING,
    RUNNING,
    CANCELED
  } m_state;
  tpool::task m_task;
  waitable_task(const tpool::task &t):m_mtx(), m_cv(), m_state(NONE),m_task(t)
  {
  }
  void run()
  {
    std::unique_lock<std::mutex> lk(m_mtx);
    if (m_state == PENDING)
    {
      m_state = RUNNING;
      lk.unlock();
      m_task.execute();
      lk.lock();
    }
    m_state = NONE;
    m_cv.notify_all();
  }
  static void execute(void* data)
  {
    ((waitable_task*)data)->run();
  }
  bool submit(tpool::thread_pool *pool)
  {
    std::unique_lock<std::mutex> lk(m_mtx);
    if (m_state == NONE)
    {
      pool->submit_task({ execute,this });
      m_state = PENDING;
      return true;
    }
    return false;
  }
  void wait(bool cancel_pending=true)
  {
    std::unique_lock<std::mutex> lk(m_mtx);
    if (cancel_pending && m_state == PENDING)
    {
      m_state = CANCELED;
      return;
    }
    while (m_state != NONE)
      m_cv.wait(lk);
  }
};

void srv_thread_pool_init();
void srv_thread_pool_end();
