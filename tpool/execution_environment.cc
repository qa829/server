#include <tpool.h>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <tpool_structs.h>
namespace tpool
{
  class rate_limiter : public execution_environment
  {
  private:
    circular_queue<tpool::task> m_queue;
    std::mutex m_mtx;
    std::condition_variable m_cv;
    unsigned int m_tasks_running;
    unsigned int m_max_concurrent_tasks;
    bool m_is_canceled;
    int m_waiter_count;
  public:
    rate_limiter(size_t queue_size = 32) :
      m_queue(),
      m_mtx(),
      m_cv(),
      m_tasks_running(),
      m_max_concurrent_tasks(1000),
      m_is_canceled(),
      m_waiter_count()
    {};
    void execute(task& t) override
    {
      std::unique_lock<std::mutex> lk(m_mtx);
      if (m_is_canceled)
        return;
      if (m_tasks_running == m_max_concurrent_tasks)
      {
        /* Queue for later execution by another thread.*/
        m_queue.push(t);
        return;
      }
      m_tasks_running++;
      for(;;)
      {
        lk.unlock();
        t.get_func()(t.get_arg());
        lk.lock();
        if (m_is_canceled || m_queue.empty())
          break;
        t = m_queue.front();
        m_queue.pop();
      }
      m_tasks_running--;
      if (!m_tasks_running && m_waiter_count)
        m_cv.notify_all();
    }

    void wait(bool cancel_pending)
    {
      std::unique_lock<std::mutex> lk(m_mtx);
      m_waiter_count++;
      if (cancel_pending)
        m_queue.clear();
      m_is_canceled = cancel_pending;
      while (m_tasks_running)
        m_cv.wait(lk);
      m_waiter_count--;
    }

    void set_max_concurrency(unsigned int max_concurrent_tasks)
    {
      std::unique_lock<std::mutex> lk(m_mtx);
      m_max_concurrent_tasks = max_concurrent_tasks;
    }
  };

  execution_environment * create_execution_environment()
  {
    return new rate_limiter();
  }
}
