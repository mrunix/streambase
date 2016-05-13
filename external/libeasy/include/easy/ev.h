/*
 * libev native API header
 *
 * Copyright (c) 2007,2008,2009,2010 Marc Alexander Lehmann <libev@schmorp.de>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modifica-
 * tion, are permitted provided that the following conditions are met:
 *
 *   1.  Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *
 *   2.  Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MER-
 * CHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO
 * EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPE-
 * CIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTH-
 * ERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Alternatively, the contents of this file may be used under the terms of
 * the GNU General Public License ("GPL") version 2 or any later version,
 * in which case the provisions of the GPL are applicable instead of
 * the above. If you wish to allow the use of your version of this file
 * only under the terms of the GPL and not to allow others to use your
 * version of this file under the BSD license, indicate your decision
 * by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL. If you do not delete the
 * provisions above, a recipient may use your version of this file under
 * either the BSD or the GPL.
 */

#ifndef EV_H_
#define EV_H_

#ifdef __cplusplus
# define EV_CPP(x) x
#else
# define EV_CPP(x)
#endif

EV_CPP(extern "C" {
      )

/*****************************************************************************/
#define ev_any_watcher ez_any_watcher
#define ev_async ez_async
#define ev_async_fsend ez_async_fsend
#define ev_async_init ez_async_init
#define ev_async_pending ez_async_pending
#define ev_async_send ez_async_send
#define ev_async_set ez_async_set
#define ev_async_start ez_async_start
#define ev_async_stop ez_async_stop
#define ev_backend ez_backend
#define ev_break ez_break
#define ev_cb ez_cb
#define ev_check ez_check
#define ev_check_init ez_check_init
#define ev_check_set ez_check_set
#define ev_check_start ez_check_start
#define ev_check_stop ez_check_stop
#define ev_child ez_child
#define ev_child_init ez_child_init
#define ev_child_set ez_child_set
#define ev_child_start ez_child_start
#define ev_child_stop ez_child_stop
#define ev_cleanup ez_cleanup
#define ev_cleanup_init ez_cleanup_init
#define ev_cleanup_set ez_cleanup_set
#define ev_cleanup_start ez_cleanup_start
#define ev_cleanup_stop ez_cleanup_stop
#define ev_clear_pending ez_clear_pending
#define ev_default_destroy ez_default_destroy
#define ev_default_fork ez_default_fork
#define ev_default_loop ez_default_loop
#define ev_default_loop_ptr easy_default_loop_ptr
#define ev_default_loop_uc_ ez_default_loop_uc_
#define ev_depth ez_depth
#define ev_embed ez_embed
#define ev_embeddable_backends ez_embeddable_backends
#define ev_embed_init ez_embed_init
#define ev_embed_set ez_embed_set
#define ev_embed_start ez_embed_start
#define ev_embed_stop ez_embed_stop
#define ev_embed_sweep ez_embed_sweep
#define ev_feed_event ez_feed_event
#define ev_feed_fd_event ez_feed_fd_event
#define ev_feed_signal_event ez_feed_signal_event
#define ev_fork ez_fork
#define ev_fork_init ez_fork_init
#define ev_fork_set ez_fork_set
#define ev_fork_start ez_fork_start
#define ev_fork_stop ez_fork_stop
#define ev_idle ez_idle
#define ev_idle_init ez_idle_init
#define ev_idle_set ez_idle_set
#define ev_idle_start ez_idle_start
#define ev_idle_stop ez_idle_stop
#define ev_init ez_init
#define ev_invoke ez_invoke
#define ev_invoke_pending ez_invoke_pending
#define ev_io ez_io
#define ev_io_ctrl_del ez_io_ctrl_del
#define ev_io_init ez_io_init
#define ev_io_set ez_io_set
#define ev_io_start ez_io_start
#define ev_io_stop ez_io_stop
#define ev_io_stop_ctrl ez_io_stop_ctrl
#define ev_is_active ez_is_active
#define ev_is_default_loop ez_is_default_loop
#define ev_is_pending ez_is_pending
#define ev_iteration ez_iteration
#define ev_loop ez_loop
#define ev_loop_count ez_loop_count
#define ev_loop_depth ez_loop_depth
#define ev_loop_destroy ez_loop_destroy
#define ev_loop_fork ez_loop_fork
#define ev_loop_new ez_loop_new
#define ev_loop_verify ez_loop_verify
#define ev_now ez_now
#define ev_now_update ez_now_update
#define ev_once ez_once
#define ev_pending_count ez_pending_count
#define ev_periodic ez_periodic
#define ev_periodic_again ez_periodic_again
#define ev_periodic_at ez_periodic_at
#define ev_periodic_init ez_periodic_init
#define ev_periodic_set ez_periodic_set
#define ev_periodic_start ez_periodic_start
#define ev_periodic_stop ez_periodic_stop
#define ev_prepare ez_prepare
#define ev_prepare_init ez_prepare_init
#define ev_prepare_set ez_prepare_set
#define ev_prepare_start ez_prepare_start
#define ev_prepare_stop ez_prepare_stop
#define ev_priority ez_priority
#define ev_recommended_backends ez_recommended_backends
#define ev_ref ez_ref
#define ev_resume ez_resume
#define ev_rt_now ez_rt_now
#define ev_run ez_run
#define ev_set_allocator ez_set_allocator
#define ev_set_cb ez_set_cb
#define ev_set_invoke_pending_cb ez_set_invoke_pending_cb
#define ev_set_io_collect_interval ez_set_io_collect_interval
#define ev_set_loop_release_cb ez_set_loop_release_cb
#define ev_set_priority ez_set_priority
#define ev_set_syserr_cb ez_set_syserr_cb
#define ev_set_timeout_collect_interval ez_set_timeout_collect_interval
#define ev_set_userdata ez_set_userdata
#define ev_signal ez_signal
#define ev_signal_init ez_signal_init
#define ev_signal_set ez_signal_set
#define ev_signal_start ez_signal_start
#define ev_signal_stop ez_signal_stop
#define ev_sleep ez_sleep
#define ev_stat ez_stat
#define ev_statdata ez_statdata
#define ev_stat_init ez_stat_init
#define ev_stat_set ez_stat_set
#define ev_stat_start ez_stat_start
#define ev_stat_stat ez_stat_stat
#define ev_stat_stop ez_stat_stop
#define ev_supported_backends ez_supported_backends
#define ev_suspend ez_suspend
#define ev_time ez_time
#define ev_timer ez_timer
#define ev_timer_again ez_timer_again
#define ev_timer_init ez_timer_init
#define ev_timer_remaining ez_timer_remaining
#define ev_timer_set ez_timer_set
#define ev_timer_start ez_timer_start
#define ev_timer_stop ez_timer_stop
#define ev_tstamp ez_tstamp
#define ev_unloop ez_unloop
#define ev_unref ez_unref
#define ev_userdata ez_userdata
#define ev_verify ez_verify
#define ev_version_major ez_version_major
#define ev_version_minor ez_version_minor
#define ev_walk ez_walk
#define ev_watcher ez_watcher
#define ev_watcher_list ez_watcher_list
#define ev_watcher_time ez_watcher_time

/* pre-4.0 compatibility */
#ifndef EV_COMPAT3
# define EV_COMPAT3 1
#endif

#ifndef EV_FEATURES
# define EV_FEATURES 0x7f
#endif

#define EV_FEATURE_CODE     ((EV_FEATURES) &  1)
#define EV_FEATURE_DATA     ((EV_FEATURES) &  2)
#define EV_FEATURE_CONFIG   ((EV_FEATURES) &  4)
#define EV_FEATURE_API      ((EV_FEATURES) &  8)
#define EV_FEATURE_WATCHERS ((EV_FEATURES) & 16)
#define EV_FEATURE_BACKENDS ((EV_FEATURES) & 32)
#define EV_FEATURE_OS       ((EV_FEATURES) & 64)

/* these priorities are inclusive, higher priorities will be invoked earlier */
#ifndef EV_MINPRI
# define EV_MINPRI (EV_FEATURE_CONFIG ? -2 : 0)
#endif
#ifndef EV_MAXPRI
# define EV_MAXPRI (EV_FEATURE_CONFIG ? +2 : 0)
#endif

#ifndef EV_MULTIPLICITY
# define EV_MULTIPLICITY EV_FEATURE_CONFIG
#endif

#ifndef EV_PERIODIC_ENABLE
# define EV_PERIODIC_ENABLE EV_FEATURE_WATCHERS
#endif

#ifndef EV_STAT_ENABLE
# define EV_STAT_ENABLE EV_FEATURE_WATCHERS
#endif

#ifndef EV_PREPARE_ENABLE
# define EV_PREPARE_ENABLE EV_FEATURE_WATCHERS
#endif

#ifndef EV_CHECK_ENABLE
# define EV_CHECK_ENABLE EV_FEATURE_WATCHERS
#endif

#ifndef EV_IDLE_ENABLE
# define EV_IDLE_ENABLE EV_FEATURE_WATCHERS
#endif

#ifndef EV_FORK_ENABLE
# define EV_FORK_ENABLE EV_FEATURE_WATCHERS
#endif

#ifndef EV_CLEANUP_ENABLE
# define EV_CLEANUP_ENABLE EV_FEATURE_WATCHERS
#endif

#ifndef EV_SIGNAL_ENABLE
# define EV_SIGNAL_ENABLE EV_FEATURE_WATCHERS
#endif

#ifndef EV_CHILD_ENABLE
# ifdef _WIN32
#  define EV_CHILD_ENABLE 0
# else
#  define EV_CHILD_ENABLE EV_FEATURE_WATCHERS
#endif
#endif

#ifndef EV_ASYNC_ENABLE
# define EV_ASYNC_ENABLE EV_FEATURE_WATCHERS
#endif

#ifndef EV_EMBED_ENABLE
# define EV_EMBED_ENABLE EV_FEATURE_WATCHERS
#endif

#ifndef EV_WALK_ENABLE
# define EV_WALK_ENABLE 0 /* not yet */
#endif

/*****************************************************************************/

#if EV_CHILD_ENABLE && !EV_SIGNAL_ENABLE
# undef EV_SIGNAL_ENABLE
# define EV_SIGNAL_ENABLE 1
#endif

/*****************************************************************************/

typedef double          ez_tstamp;

#ifndef EV_ATOMIC_T
# include <signal.h>
# define EV_ATOMIC_T sig_atomic_t volatile
#endif

#if EV_STAT_ENABLE
# ifdef _WIN32
#  include <time.h>
#  include <sys/types.h>
# endif
# include <sys/stat.h>
#endif

/* support multiple event loops? */
#if EV_MULTIPLICITY
struct                  ez_loop;
# define EV_P  struct ez_loop *loop               /* a loop as sole parameter in a declaration */
# define EV_P_ EV_P,                              /* a loop as first of multiple parameters */
# define EV_A  loop                               /* a loop as sole argument to a function call */
# define EV_A_ EV_A,                              /* a loop as first of multiple arguments */
# define EV_DEFAULT_UC  ez_default_loop_uc_ ()    /* the default loop, if initialised, as sole arg */
# define EV_DEFAULT_UC_ EV_DEFAULT_UC,            /* the default loop as first of multiple arguments */
# define EV_DEFAULT  ez_default_loop (0)          /* the default loop as sole arg */
# define EV_DEFAULT_ EV_DEFAULT,                  /* the default loop as first of multiple arguments */
#else
# define EV_P void
# define EV_P_
# define EV_A
# define EV_A_
# define EV_DEFAULT
# define EV_DEFAULT_
# define EV_DEFAULT_UC
# define EV_DEFAULT_UC_
# undef EV_EMBED_ENABLE
#endif

/* EV_INLINE is used for functions in header files */
#if __STDC_VERSION__ >= 199901L || __GNUC__ >= 3
# define EV_INLINE static inline
#else
# define EV_INLINE static
#endif

/* EV_PROTOTYPES can be sued to switch of prototype declarations */
#ifndef EV_PROTOTYPES
# define EV_PROTOTYPES 1
#endif

/*****************************************************************************/

#define EV_VERSION_MAJOR 4
#define EV_VERSION_MINOR 1

/* eventmask, revents, events... */
  enum {
  EV_UNDEF    = 0xFFFFFFFF, /* guaranteed to be invalid */
  EV_NONE     =       0x00, /* no events */
  EV_READ     =       0x01, /* ez_io detected read will not block */
  EV_WRITE    =       0x02, /* ez_io detected write will not block */
  EV__IOFDSET =       0x80, /* internal use only */
  EV_IO       =    EV_READ, /* alias for type-detection */
  EV_TIMER    = 0x00000100, /* timer timed out */
#if EV_COMPAT3
  EV_TIMEOUT  =   EV_TIMER, /* pre 4.0 API compatibility */
#endif
  EV_PERIODIC = 0x00000200, /* periodic timer timed out */
  EV_SIGNAL   = 0x00000400, /* signal was received */
  EV_CHILD    = 0x00000800, /* child/pid had status change */
  EV_STAT     = 0x00001000, /* stat data changed */
  EV_IDLE     = 0x00002000, /* event loop is idling */
  EV_PREPARE  = 0x00004000, /* event loop about to poll */
  EV_CHECK    = 0x00008000, /* event loop finished poll */
  EV_EMBED    = 0x00010000, /* embedded event loop needs sweep */
  EV_FORK     = 0x00020000, /* event loop resumed in child */
  EV_CLEANUP  = 0x00040000, /* event loop resumed in child */
  EV_ASYNC    = 0x00080000, /* async intra-loop signal */
  EV_CUSTOM   = 0x01000000, /* for use by user code */
  EV_ERROR    = 0x80000000  /* sent when an error occurs */
};

/* can be used to add custom fields to all watchers, while losing binary compatibility */
#ifndef EV_COMMON
# define EV_COMMON void *data;
#endif

#ifndef EV_CB_DECLARE
# define EV_CB_DECLARE(type) void (*cb)(EV_P_ struct type *w, int revents);
#endif
#ifndef EV_CB_INVOKE
# define EV_CB_INVOKE(watcher,revents) (watcher)->cb (EV_A_ (watcher), (revents))
#endif

/* not official, do not use */
#define EV_CB(type,name) void name (EV_P_ struct ez_ ## type *w, int revents)

/*
 * struct member types:
 * private: you may look at them, but not change them,
 *          and they might not mean anything to you.
 * ro: can be read anytime, but only changed when the watcher isn't active.
 * rw: can be read and modified anytime, even when the watcher is active.
 *
 * some internal details that might be helpful for debugging:
 *
 * active is either 0, which means the watcher is not active,
 *           or the array index of the watcher (periodics, timers)
 *           or the array index + 1 (most other watchers)
 *           or simply 1 for watchers that aren't in some array.
 * pending is either 0, in which case the watcher isn't,
 *           or the array index + 1 in the pendings array.
 */

#if EV_MINPRI == EV_MAXPRI
# define EV_DECL_PRIORITY
#elif !defined (EV_DECL_PRIORITY)
# define EV_DECL_PRIORITY int priority;
#endif

/* shared by all watchers */
#define EV_WATCHER(type)            \
    int                     active; /* private */         \
    int                     pending; /* private */            \
    EV_DECL_PRIORITY /* private */        \
    EV_COMMON /* rw */                \
    EV_CB_DECLARE (type) /* private */

#define EV_WATCHER_LIST(type)           \
    EV_WATCHER (type)             \
    struct ez_watcher_list  *next; /* private */

#define EV_WATCHER_TIME(type)           \
    EV_WATCHER (type)             \
    ez_tstamp               at;     /* private */

/* base class, nothing to see here unless you subclass */
typedef struct ez_watcher {
  EV_WATCHER(ez_watcher)
} ez_watcher;

/* base class, nothing to see here unless you subclass */
typedef struct ez_watcher_list {
  EV_WATCHER_LIST(ez_watcher_list)
} ez_watcher_list;

/* base class, nothing to see here unless you subclass */
typedef struct ez_watcher_time {
  EV_WATCHER_TIME(ez_watcher_time)
} ez_watcher_time;

/* invoked when fd is either EV_READable or EV_WRITEable */
/* revent EV_READ, EV_WRITE */
typedef struct ez_io {
  EV_WATCHER_LIST(ez_io)

  int                     fd;     /* ro */
  int                     events; /* ro */
} ez_io;

/* invoked after a specific time, repeatable (based on monotonic clock) */
/* revent EV_TIMEOUT */
typedef struct ez_timer {
  EV_WATCHER_TIME(ez_timer)

  ez_tstamp               repeat; /* rw */
} ez_timer;

/* invoked at some specific time, possibly repeating at regular intervals (based on UTC) */
/* revent EV_PERIODIC */
typedef struct ez_periodic {
  EV_WATCHER_TIME(ez_periodic)

  ez_tstamp               offset; /* rw */
  ez_tstamp               interval; /* rw */
  ez_tstamp(*reschedule_cb)(struct ez_periodic* w, ez_tstamp now);    /* rw */
} ez_periodic;

/* invoked when the given signal has been received */
/* revent EV_SIGNAL */
typedef struct ez_signal {
  EV_WATCHER_LIST(ez_signal)

  int                     signum; /* ro */
} ez_signal;

/* invoked when sigchld is received and waitpid indicates the given pid */
/* revent EV_CHILD */
/* does not support priorities */
typedef struct ez_child {
  EV_WATCHER_LIST(ez_child)

  int                     flags;   /* private */
  int                     pid;     /* ro */
  int                     rpid;    /* rw, holds the received pid */
  int                     rstatus; /* rw, holds the exit status, use the macros from sys/wait.h */
} ez_child;

#if EV_STAT_ENABLE
/* st_nlink = 0 means missing file or other error */
# ifdef _WIN32
typedef struct _stati64 ez_statdata;
# else
typedef struct stat     ez_statdata;
# endif

/* invoked each time the stat data changes for a given path */
/* revent EV_STAT */
  typedef struct ez_stat {
    EV_WATCHER_LIST(ez_stat)

    ez_timer                timer;     /* private */
    ez_tstamp               interval; /* ro */
    const char*              path;   /* ro */
    ez_statdata             prev;   /* ro */
    ez_statdata             attr;   /* ro */

    int                     wd; /* wd for inotify, fd for kqueue */
  } ez_stat;
#endif

#if EV_IDLE_ENABLE
/* invoked when the nothing else needs to be done, keeps the process from blocking */
/* revent EV_IDLE */
typedef struct ez_idle {
  EV_WATCHER(ez_idle)
} ez_idle;
#endif

/* invoked for each run of the mainloop, just before the blocking call */
/* you can still change events in any way you like */
/* revent EV_PREPARE */
typedef struct ez_prepare {
  EV_WATCHER(ez_prepare)
} ez_prepare;

/* invoked for each run of the mainloop, just after the blocking call */
/* revent EV_CHECK */
typedef struct ez_check {
  EV_WATCHER(ez_check)
} ez_check;

#if EV_FORK_ENABLE
/* the callback gets invoked before check in the child process when a fork was detected */
/* revent EV_FORK */
typedef struct ez_fork {
  EV_WATCHER(ez_fork)
} ez_fork;
#endif

#if EV_CLEANUP_ENABLE
/* is invoked just before the loop gets destroyed */
/* revent EV_CLEANUP */
typedef struct ez_cleanup {
  EV_WATCHER(ez_cleanup)
} ez_cleanup;
#endif

#if EV_EMBED_ENABLE
/* used to embed an event loop inside another */
/* the callback gets invoked when the event loop has handled events, and can be 0 */
typedef struct ez_embed {
  EV_WATCHER(ez_embed)

  struct ez_loop*          other; /* ro */
  ez_io                   io;              /* private */
  ez_prepare              prepare;    /* private */
  ez_check                check;        /* unused */
  ez_timer                timer;        /* unused */
  ez_periodic             periodic;  /* unused */
  ez_idle                 idle;          /* unused */
  ez_fork                 fork;          /* private */
#if EV_CLEANUP_ENABLE
  ez_cleanup              cleanup;    /* unused */
#endif
} ez_embed;
#endif

#if EV_ASYNC_ENABLE
/* invoked when somebody calls ez_async_send on the watcher */
/* revent EV_ASYNC */
typedef struct ez_async {
  EV_WATCHER(ez_async)

  EV_ATOMIC_T             sent; /* private */
} ez_async;

# define ez_async_pending(w) (+(w)->sent)
#endif

/* the presence of this union forces similar struct layout */
union ez_any_watcher {
  struct ez_watcher       w;
  struct ez_watcher_list  wl;

  struct ez_io            io;
  struct ez_timer         timer;
  struct ez_periodic      periodic;
  struct ez_signal        signal;
  struct ez_child         child;
#if EV_STAT_ENABLE
  struct ez_stat          stat;
#endif
#if EV_IDLE_ENABLE
  struct ez_idle          idle;
#endif
  struct ez_prepare       prepare;
  struct ez_check         check;
#if EV_FORK_ENABLE
  struct ez_fork          fork;
#endif
#if EV_CLEANUP_ENABLE
  struct ez_cleanup       cleanup;
#endif
#if EV_EMBED_ENABLE
  struct ez_embed         embed;
#endif
#if EV_ASYNC_ENABLE
  struct ez_async         async;
#endif
};

/* flag bits for ez_default_loop and ez_loop_new */
enum {
  /* the default */
  EVFLAG_AUTO      = 0x00000000U, /* not quite a mask */
  /* flag bits */
  EVFLAG_NOENV     = 0x01000000U, /* do NOT consult environment */
  EVFLAG_FORKCHECK = 0x02000000U, /* check for a fork in each iteration */
  /* debugging/feature disable */
  EVFLAG_NOINOTIFY = 0x00100000U, /* do not attempt to use inotify */
#if EV_COMPAT3
  EVFLAG_NOSIGFD   = 0, /* compatibility to pre-3.9 */
#endif
  EVFLAG_SIGNALFD  = 0x00200000U  /* attempt to use signalfd */
};

/* method bits to be ored together */
enum {
  EVBACKEND_SELECT  = 0x00000001U, /* about anywhere */
  EVBACKEND_POLL    = 0x00000002U, /* !win */
  EVBACKEND_EPOLL   = 0x00000004U, /* linux */
  EVBACKEND_KQUEUE  = 0x00000008U, /* bsd */
  EVBACKEND_DEVPOLL = 0x00000010U, /* solaris 8 */ /* NYI */
  EVBACKEND_PORT    = 0x00000020U, /* solaris 10 */
  EVBACKEND_ALL     = 0x0000003FU
};

#if EV_PROTOTYPES
int ez_version_major(void);
int ez_version_minor(void);

unsigned int ez_supported_backends(void);
unsigned int ez_recommended_backends(void);
unsigned int ez_embeddable_backends(void);

ez_tstamp ez_time(void);
void ez_sleep(ez_tstamp delay);  /* sleep for a while */

/* Sets the allocation function to use, works like realloc.
 * It is used to allocate and free memory.
 * If it returns zero when memory needs to be allocated, the library might abort
 * or take some potentially destructive action.
 * The default is your system realloc function.
 */
void ez_set_allocator(void* (*cb)(void* ptr, size_t size));

/* set the callback function to call on a
 * retryable syscall error
 * (such as failed select, poll, epoll_wait)
 */
void ez_set_syserr_cb(void (*cb)(const char* msg));

#if EV_MULTIPLICITY

/* the default loop is the only one that handles signals and child watchers */
/* you can call this as often as you like */
struct ez_loop* ez_default_loop(unsigned int flags EV_CPP( = 0));

EV_INLINE struct ez_loop*
ez_default_loop_uc_(void) {
  extern struct ez_loop*   easy_default_loop_ptr;

  return easy_default_loop_ptr;
}

EV_INLINE int
ez_is_default_loop(EV_P) {
  return EV_A == EV_DEFAULT_UC;
}

/* create and destroy alternative loops that don't handle signals */
struct ez_loop* ez_loop_new(unsigned int flags EV_CPP( = 0));

ez_tstamp ez_now(EV_P);  /* time w.r.t. timers and the eventloop, updated after each poll */

#else

int ez_default_loop(unsigned int flags EV_CPP( = 0));   /* returns true when successful */

EV_INLINE ez_tstamp
ez_now(void) {
  extern ez_tstamp        ez_rt_now;

  return ez_rt_now;
}

/* looks weird, but ez_is_default_loop (EV_A) still works if this exists */
EV_INLINE int
ez_is_default_loop(void) {
  return 1;
}

#endif /* multiplicity */

/* destroy event loops, also works for the default loop */
void ez_loop_destroy(EV_P);

/* this needs to be called after fork, to duplicate the loop */
/* when you want to re-use it in the child */
/* you can call it in either the parent or the child */
/* you can actually call it at any time, anywhere :) */
void ez_loop_fork(EV_P);

unsigned int ez_backend(EV_P);  /* backend in use by loop */

void ez_now_update(EV_P);  /* update event loop time */

#if EV_WALK_ENABLE
/* walk (almost) all watchers in the loop of a given type, invoking the */
/* callback on every such watcher. The callback might stop the watcher, */
/* but do nothing else with the loop */
void ez_walk(EV_P_ int types, void (*cb)(EV_P_ int type, void* w));
#endif

#endif /* prototypes */

/* ez_run flags values */
enum {
  EVRUN_NOWAIT = 1, /* do not block/wait */
  EVRUN_ONCE   = 2  /* block *once* only */
};

/* ez_break how values */
enum {
  EVBREAK_CANCEL = 0, /* undo unloop */
  EVBREAK_ONE    = 1, /* unloop once */
  EVBREAK_ALL    = 2  /* unloop all loops */
};

#if EV_PROTOTYPES
void ez_run(EV_P_ int flags EV_CPP( = 0));
void ez_break(EV_P_ int how EV_CPP( = EVBREAK_ONE));   /* break out of the loop */

/*
 * ref/unref can be used to add or remove a refcount on the mainloop. every watcher
 * keeps one reference. if you have a long-running watcher you never unregister that
 * should not keep ez_loop from running, unref() after starting, and ref() before stopping.
 */
void ez_ref(EV_P);
void ez_unref(EV_P);

/*
 * convenience function, wait for a single event, without registering an event watcher
 * if timeout is < 0, do wait indefinitely
 */
void ez_once(EV_P_ int fd, int events, ez_tstamp timeout, void (*cb)(int revents, void* arg), void* arg);

# if EV_FEATURE_API
unsigned int ez_iteration(EV_P);  /* number of loop iterations */
unsigned int ez_depth(EV_P);      /* #ez_loop enters - #ez_loop leaves */
void         ez_verify(EV_P);     /* abort if loop data corrupted */

void ez_set_io_collect_interval(EV_P_ ez_tstamp interval);  /* sleep at least this time, default 0 */
void ez_set_timeout_collect_interval(EV_P_ ez_tstamp interval);  /* sleep at least this time, default 0 */

/* advanced stuff for threading etc. support, see docs */
void ez_set_userdata(EV_P_ void* data);
void* ez_userdata(EV_P);
void ez_set_invoke_pending_cb(EV_P_ void (*invoke_pending_cb)(EV_P));
void ez_set_loop_release_cb(EV_P_ void (*release)(EV_P), void (*acquire)(EV_P));

unsigned int ez_pending_count(EV_P);  /* number of pending events, if any */
void ez_invoke_pending(EV_P);  /* invoke all pending watchers */

/*
 * stop/start the timer handling.
 */
void ez_suspend(EV_P);
void ez_resume(EV_P);
#endif

#endif

/* these may evaluate ev multiple times, and the other arguments at most once */
/* either use ez_init + ez_TYPE_set, or the ez_TYPE_init macro, below, to first initialise a watcher */
#define ez_init(ev,cb_) do {            \
        ((ez_watcher *)(void *)(ev))->active  =   \
                ((ez_watcher *)(void *)(ev))->pending = 0;    \
        ez_set_priority ((ev), 0);            \
        ez_set_cb ((ev), cb_);            \
    } while (0)

#define ez_io_set(ev,fd_,events_)            do { (ev)->fd = (fd_); (ev)->events = (events_) | EV__IOFDSET; } while (0)
#define ez_timer_set(ev,after_,repeat_)      do { (ev)->at = (after_); (ev)->repeat = (repeat_); } while (0)
#define ez_periodic_set(ev,ofs_,ival_,rcb_)  do { (ev)->offset = (ofs_); (ev)->interval = (ival_); (ev)->reschedule_cb = (rcb_); } while (0)
#define ez_signal_set(ev,signum_)            do { (ev)->signum = (signum_); } while (0)
#define ez_child_set(ev,pid_,trace_)         do { (ev)->pid = (pid_); (ev)->flags = !!(trace_); } while (0)
#define ez_stat_set(ev,path_,interval_)      do { (ev)->path = (path_); (ev)->interval = (interval_); (ev)->wd = -2; } while (0)
#define ez_idle_set(ev)                      /* nop, yes, this is a serious in-joke */
#define ez_prepare_set(ev)                   /* nop, yes, this is a serious in-joke */
#define ez_check_set(ev)                     /* nop, yes, this is a serious in-joke */
#define ez_embed_set(ev,other_)              do { (ev)->other = (other_); } while (0)
#define ez_fork_set(ev)                      /* nop, yes, this is a serious in-joke */
#define ez_cleanup_set(ev)                   /* nop, yes, this is a serious in-joke */
#define ez_async_set(ev)                     /* nop, yes, this is a serious in-joke */

#define ez_io_init(ev,cb,fd,events)          do { ez_init ((ev), (cb)); ez_io_set ((ev),(fd),(events)); } while (0)
#define ez_timer_init(ev,cb,after,repeat)    do { ez_init ((ev), (cb)); ez_timer_set ((ev),(after),(repeat)); } while (0)
#define ez_periodic_init(ev,cb,ofs,ival,rcb) do { ez_init ((ev), (cb)); ez_periodic_set ((ev),(ofs),(ival),(rcb)); } while (0)
#define ez_signal_init(ev,cb,signum)         do { ez_init ((ev), (cb)); ez_signal_set ((ev), (signum)); } while (0)
#define ez_child_init(ev,cb,pid,trace)       do { ez_init ((ev), (cb)); ez_child_set ((ev),(pid),(trace)); } while (0)
#define ez_stat_init(ev,cb,path,interval)    do { ez_init ((ev), (cb)); ez_stat_set ((ev),(path),(interval)); } while (0)
#define ez_idle_init(ev,cb)                  do { ez_init ((ev), (cb)); ez_idle_set ((ev)); } while (0)
#define ez_prepare_init(ev,cb)               do { ez_init ((ev), (cb)); ez_prepare_set ((ev)); } while (0)
#define ez_check_init(ev,cb)                 do { ez_init ((ev), (cb)); ez_check_set ((ev)); } while (0)
#define ez_embed_init(ev,cb,other)           do { ez_init ((ev), (cb)); ez_embed_set ((ev),(other)); } while (0)
#define ez_fork_init(ev,cb)                  do { ez_init ((ev), (cb)); ez_fork_set ((ev)); } while (0)
#define ez_cleanup_init(ev,cb)               do { ez_init ((ev), (cb)); ez_cleanup_set ((ev)); } while (0)
#define ez_async_init(ev,cb)                 do { ez_init ((ev), (cb)); ez_async_set ((ev)); } while (0)

#define ez_is_pending(ev)                    (0 + ((ez_watcher *)(void *)(ev))->pending) /* ro, true when watcher is waiting for callback invocation */
#define ez_is_active(ev)                     (0 + ((ez_watcher *)(void *)(ev))->active) /* ro, true when the watcher has been started */

#define ez_cb(ev)                            (ev)->cb /* rw */

#if EV_MINPRI == EV_MAXPRI
# define ez_priority(ev)                     ((ev), EV_MINPRI)
# define ez_set_priority(ev,pri)             ((ev), (pri))
#else
# define ez_priority(ev)                     (+(((ez_watcher *)(void *)(ev))->priority))
# define ez_set_priority(ev,pri)             (   (ez_watcher *)(void *)(ev))->priority = (pri)
#endif

#define ez_periodic_at(ev)                   (+((ez_watcher_time *)(ev))->at)

#ifndef ez_set_cb
# define ez_set_cb(ev,cb_)                   ez_cb (ev) = (cb_)
#endif

/* stopping (enabling, adding) a watcher does nothing if it is already running */
/* stopping (disabling, deleting) a watcher does nothing unless its already running */
#if EV_PROTOTYPES

/* feeds an event into a watcher as if the event actually occured */
/* accepts any ez_watcher type */
void ez_feed_event(EV_P_ void* w, int revents);
void ez_feed_fd_event(EV_P_ int fd, int revents);
#if EV_SIGNAL_ENABLE
void ez_feed_signal_event(EV_P_ int signum);
#endif
void ez_invoke(EV_P_ void* w, int revents);
int  ez_clear_pending(EV_P_ void* w);

void ez_io_start(EV_P_ ez_io* w);
void ez_io_stop(EV_P_ ez_io* w);
void ez_io_stop_ctrl(EV_P_ ez_io* w);

void ez_timer_start(EV_P_ ez_timer* w);
void ez_timer_stop(EV_P_ ez_timer* w);
/* stops if active and no repeat, restarts if active and repeating, starts if inactive and repeating */
void ez_timer_again(EV_P_ ez_timer* w);
/* return remaining time */
ez_tstamp ez_timer_remaining(EV_P_ ez_timer* w);

#if EV_PERIODIC_ENABLE
void ez_periodic_start(EV_P_ ez_periodic* w);
void ez_periodic_stop(EV_P_ ez_periodic* w);
void ez_periodic_again(EV_P_ ez_periodic* w);
#endif

/* only supported in the default loop */
#if EV_SIGNAL_ENABLE
void ez_signal_start(EV_P_ ez_signal* w);
void ez_signal_stop(EV_P_ ez_signal* w);
#endif

/* only supported in the default loop */
# if EV_CHILD_ENABLE
void ez_child_start(EV_P_ ez_child* w);
void ez_child_stop(EV_P_ ez_child* w);
# endif

# if EV_STAT_ENABLE
void ez_stat_start(EV_P_ ez_stat* w);
void ez_stat_stop(EV_P_ ez_stat* w);
void ez_stat_stat(EV_P_ ez_stat* w);
# endif

# if EV_IDLE_ENABLE
void ez_idle_start(EV_P_ ez_idle* w);
void ez_idle_stop(EV_P_ ez_idle* w);
# endif

#if EV_PREPARE_ENABLE
void ez_prepare_start(EV_P_ ez_prepare* w);
void ez_prepare_stop(EV_P_ ez_prepare* w);
#endif

#if EV_CHECK_ENABLE
void ez_check_start(EV_P_ ez_check* w);
void ez_check_stop(EV_P_ ez_check* w);
#endif

# if EV_FORK_ENABLE
void ez_fork_start(EV_P_ ez_fork* w);
void ez_fork_stop(EV_P_ ez_fork* w);
# endif

# if EV_CLEANUP_ENABLE
void ez_cleanup_start(EV_P_ ez_cleanup* w);
void ez_cleanup_stop(EV_P_ ez_cleanup* w);
# endif

# if EV_EMBED_ENABLE
/* only supported when loop to be embedded is in fact embeddable */
void ez_embed_start(EV_P_ ez_embed* w);
void ez_embed_stop(EV_P_ ez_embed* w);
void ez_embed_sweep(EV_P_ ez_embed* w);
# endif

# if EV_ASYNC_ENABLE
void ez_async_start(EV_P_ ez_async* w);
void ez_async_stop(EV_P_ ez_async* w);
void ez_async_send(EV_P_ ez_async* w);
void ez_async_fsend(EV_P_ ez_async* w);
# endif

#if EV_COMPAT3
#define EVLOOP_NONBLOCK EVRUN_NOWAIT
#define EVLOOP_ONESHOT  EVRUN_ONCE
#define EVUNLOOP_CANCEL EVBREAK_CANCEL
#define EVUNLOOP_ONE    EVBREAK_ONE
#define EVUNLOOP_ALL    EVBREAK_ALL
#if EV_PROTOTYPES
EV_INLINE void ez_loop(EV_P_ int flags) {
  ez_run(EV_A_ flags);
}
EV_INLINE void ez_unloop(EV_P_ int how) {
  ez_break(EV_A_ how);
}
EV_INLINE void ez_default_destroy(void) {
  ez_loop_destroy(EV_DEFAULT);
}
EV_INLINE void ez_default_fork(void) {
  ez_loop_fork(EV_DEFAULT);
}
#if EV_FEATURE_API
EV_INLINE unsigned int ez_loop_count(EV_P) {
  return ez_iteration(EV_A);
}
EV_INLINE unsigned int ez_loop_depth(EV_P) {
  return ez_depth(EV_A);
}
EV_INLINE void         ez_loop_verify(EV_P) {
  ez_verify(EV_A);
}
#endif
#endif
#else
typedef struct ez_loop  ez_loop;
#endif

#endif

EV_CPP(
})

#endif

