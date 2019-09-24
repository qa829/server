/* Copyright(C) 2019 MariaDB

This program is free software; you can redistribute itand /or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02111 - 1301 USA*/

#pragma once
#include <memory> /* unique_ptr */
#ifdef LINUX_NATIVE_AIO
#include <libaio.h>
#endif
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
/**
  Windows-specific native file handle struct.
  Apart from the actual handle, contains PTP_IO
  used by the Windows threadpool.
*/
struct native_file_handle
{
  HANDLE m_handle;
  PTP_IO m_ptp_io;
  native_file_handle(){};
  native_file_handle(HANDLE h) : m_handle(h), m_ptp_io() {}
  operator HANDLE() const { return m_handle; }
};
#else
#include <unistd.h>
typedef int native_file_handle;
#endif

namespace tpool
{
/**
 Task callback function
 */
typedef void (*callback_func)(void *);
class task;

/* A class that can be used e.g for rate limiting,
or waiting for finished callbacks. */
class execution_environment
{
public:
  virtual void execute(task& t) = 0;
  virtual void wait(bool cancel_pending) = 0;
  virtual void set_max_concurrency(unsigned int) = 0;
  virtual ~execution_environment() {};
};

/* Task executor limiting concurrency.*/
extern execution_environment* create_execution_environment();
  /**
 Task, a void function with void *argument.
*/
class task
{
private:
  callback_func m_func;
  void *m_arg;
  execution_environment* m_env;
public:
  task() {};
  task(callback_func func, void* arg, execution_environment* env = nullptr) :
    m_func(func), m_arg(arg), m_env(env) {};
  void* get_arg() { return m_arg; }
  callback_func get_func() { return m_func; }
  inline void execute()
  {
    if (m_env)
      m_env->execute(*this);
    else
      m_func(m_arg);
  }
};

enum aio_opcode
{
  AIO_PREAD,
  AIO_PWRITE
};
const int MAX_AIO_USERDATA_LEN= 40;
struct aiocb;

/** IO control block, includes parameters for the IO, and the callback*/

struct aiocb
#ifdef _WIN32
  :OVERLAPPED
#elif defined LINUX_NATIVE_AIO
  :iocb
#endif
{
  native_file_handle m_fh;
  aio_opcode m_opcode;
  unsigned long long m_offset;
  void *m_buffer;
  unsigned int m_len;
  callback_func m_callback;
  execution_environment* m_env;
  /* Returned length and error code*/
  int m_ret_len;
  int m_err;
  void *m_internal;
  char m_userdata[MAX_AIO_USERDATA_LEN];

  void execute_callback()
  {
    task t(m_callback, this);
    t.execute();
  }
};


/**
 AIO interface
*/
class aio
{
public:
  /**
    Submit asyncronous IO.
    On completion, cb->m_callback is executed.
  */
  virtual int submit_io(aiocb *cb)= 0;
  /** "Bind" file to AIO handler (used on Windows only) */
  virtual int bind(native_file_handle &fd)= 0;
  /** "Unind" file to AIO handler (used on Windows only) */
  virtual int unbind(const native_file_handle &fd)= 0;
  virtual ~aio(){};
};

class timer
{
public:
  virtual void set_time(int initial_delay_ms, int period_ms) = 0;
  virtual void disarm() = 0;
  virtual ~timer(){}
};

class thread_pool;

extern aio *create_simulated_aio(thread_pool *tp);

class thread_pool
{
protected:
  /* AIO handler */
  std::unique_ptr<aio> m_aio;
  virtual aio *create_native_aio(int max_io)= 0;

  /**
    Functions to be called at worker thread start/end
    can be used for example to set some TLS variables
  */
  void (*m_worker_init_callback)(void);
  void (*m_worker_destroy_callback)(void);

public:
  thread_pool() : m_aio(), m_worker_init_callback(), m_worker_destroy_callback()
  {
  }
  virtual void submit_task(const task &t)= 0;
  virtual timer* create_timer(const task &t) = 0;
  void set_thread_callbacks(void (*init)(), void (*destroy)())
  {
    m_worker_init_callback= init;
    m_worker_destroy_callback= destroy;
  }
  int configure_aio(bool use_native_aio, int max_io)
  {
    if (use_native_aio)
      m_aio.reset(create_native_aio(max_io));
    if (!m_aio)
      m_aio.reset(create_simulated_aio(this));
    return !m_aio ? -1 : 0;
  }
  void disable_aio()
  {
    m_aio.reset();
  }
  int bind(native_file_handle &fd) { return m_aio->bind(fd); }
  void unbind(const native_file_handle &fd) { m_aio->unbind(fd); }
  int submit_io(aiocb *cb) { return m_aio->submit_io(cb); }
  virtual ~thread_pool() {}
};
const int DEFAULT_MIN_POOL_THREADS= 1;
const int DEFAULT_MAX_POOL_THREADS= 500;
extern thread_pool *
create_thread_pool_generic(int min_threads= DEFAULT_MIN_POOL_THREADS,
                           int max_threads= DEFAULT_MAX_POOL_THREADS);
#ifdef _WIN32
extern thread_pool *
create_thread_pool_win(int min_threads= DEFAULT_MIN_POOL_THREADS,
                       int max_threads= DEFAULT_MAX_POOL_THREADS);

/*
  Helper functions, to execute pread/pwrite even if file is
  opened with FILE_FLAG_OVERLAPPED, and bound to completion
  port.
*/
int pwrite(const native_file_handle &h, void *buf, size_t count,
           unsigned long long offset);
int pread(const native_file_handle &h, void *buf, size_t count,
          unsigned long long offset);
HANDLE win_get_syncio_event();
#endif
} // namespace tpool
