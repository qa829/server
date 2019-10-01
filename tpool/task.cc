#include <tpool.h>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <tpool_structs.h>

namespace tpool
{
  task::task(callback_func func, void* arg, task_group* group) :
    m_func(func), m_arg(arg), m_group(group) {}

  void task::execute()
  {
    if (m_group)
    {
      /* Executing in a group (limiting concurrency).*/
      m_group->execute(this);
    }
    else
    {
      /* Execute directly. */
      m_func(m_arg);
      release();
    }
  }

  /* Task that provide wait() operation. */
  waitable_task::waitable_task(callback_func func, void* arg, task_group* group) :
    task(func,arg, group),m_mtx(),m_cv(),m_ref_count(),m_waiter_count(){}

  void waitable_task::add_ref()
  {
    std::unique_lock<std::mutex> lk(m_mtx);
    m_ref_count++;
  }

  void waitable_task::release()
  {
    std::unique_lock<std::mutex> lk(m_mtx);
    m_ref_count--;
    if (!m_ref_count && m_waiter_count)
      m_cv.notify_all();
  }
  void waitable_task::wait()
  {
    std::unique_lock<std::mutex> lk(m_mtx);
    m_waiter_count++;
    while (m_ref_count)
      m_cv.wait(lk);
    m_waiter_count--;
  }

}