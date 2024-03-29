/* ********************************
 * Author:       Johan Hanssen Seferidis
 * License:	     MIT
 * Description:  Library providing a threading pool where you can add
 *               work. For usage, check the thpool.h file or README.md
 *
 */
/** @file thpool.h */ /*
                       *
                       ********************************/

//#define _POSIX_C_SOURCE 200809L
#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>
#if defined(__linux__)
#include <sys/prctl.h>
#endif

#include "thpool.h"
#include "../common.h"

#include "../mr_memory.h"

#ifdef THPOOL_DEBUG
#define THPOOL_DEBUG 1
#else
#define THPOOL_DEBUG 0
#endif

#if !defined(DISABLE_PRINT) || defined(THPOOL_DEBUG)
#define err(str) fprintf(stderr, str)
#else
#define err(str)
#endif

static volatile int threads_keepalive;
static volatile int threads_on_hold;

/* ========================== STRUCTURES ============================ */

/* Binary semaphore */
typedef struct mr_bsem {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int v;
} mr_bsem;

/* Job */
typedef struct mr_job {
  struct mr_job* prev;            /* pointer to previous job   */
  void (*function)(void* arg); /* function pointer          */
  void* arg;                   /* function's argument       */
} mr_job;

/* Job queue */
typedef struct mr_jobqueue {
  pthread_mutex_t rwmutex; /* used for queue r/w access */
  mr_job* front;              /* pointer to front of queue */
  mr_job* rear;               /* pointer to rear  of queue */
  mr_bsem* has_jobs;          /* flag as binary semaphore  */
  int len;                 /* number of jobs in queue   */
} mr_jobqueue;

/* Thread */
typedef struct mr_thread {
  int id;                   /* friendly id               */
  pthread_t pthread;        /* pointer to actual thread  */
  struct mr_thpool_* thpool_p; /* access to thpool          */
} mr_thread;

/* Threadpool */
typedef struct mr_thpool_ {
  mr_thread** threads;                 /* pointer to threads        */
  volatile int num_threads_alive;   /* threads currently alive   */
  volatile int num_threads_working; /* threads currently working */
  volatile int total_num_of_threads; /* total requested num of threads */
  volatile int is_threads_started;   /* indication if the threads already started */
  pthread_mutex_t is_threads_started_lock;     /* used for thread count etc */
  pthread_mutex_t thcount_lock;     /* used for thread count etc */
  pthread_cond_t threads_all_idle;  /* signal to thpool_wait     */
  mr_jobqueue jobqueue;                /* job queue                 */
} mr_thpool_;

/* ========================== PROTOTYPES ============================ */

static int thread_init(mr_thpool_* thpool_p, struct mr_thread** thread_p, int id);
static void* thread_do(struct mr_thread* thread_p);
static void thread_hold(int sig_id);
static void thread_destroy(struct mr_thread* thread_p);

static int jobqueue_init(mr_jobqueue* jobqueue_p);
static void jobqueue_clear(mr_jobqueue* jobqueue_p);
static void jobqueue_push(mr_jobqueue* jobqueue_p, struct mr_job* newjob_p);
static struct mr_job* jobqueue_pull(mr_jobqueue* jobqueue_p);
static void jobqueue_destroy(mr_jobqueue* jobqueue_p);

static void bsem_init(struct mr_bsem* bsem_p, int value);
static void bsem_reset(struct mr_bsem* bsem_p);
static void bsem_post(struct mr_bsem* bsem_p);
static void bsem_post_all(struct mr_bsem* bsem_p);
static void bsem_wait(struct mr_bsem* bsem_p);

static void mr_thpool_start_threads(mr_thpool_* thpool_p) {
    /* Thread init */
    if (thpool_p->is_threads_started) {
        return;
    }
    pthread_mutex_lock(&thpool_p->is_threads_started_lock);
    if (thpool_p->is_threads_started) {
        pthread_mutex_unlock(&thpool_p->is_threads_started_lock);
        return;
    }
    int n;
    for (n = 0; n < thpool_p->total_num_of_threads; n++) {
        thread_init(thpool_p, &thpool_p->threads[n], n);
#if THPOOL_DEBUG
    printf("THPOOL_DEBUG: Created thread %d in pool \n", n);
#endif
    }

    /* Wait for threads to initialize */
    while (thpool_p->num_threads_alive != thpool_p->total_num_of_threads) {
        // avoid busy loop, wait for a very small amount of time.
        usleep(1);
    }

    thpool_p->is_threads_started = 1;
    pthread_mutex_unlock(&thpool_p->is_threads_started_lock);
}

/* ========================== THREADPOOL ============================ */

/* Initialise thread pool */
struct mr_thpool_* mr_thpool_init(int num_threads) {

  threads_on_hold = 0;
  threads_keepalive = 1;

  if (num_threads < 0) {
    num_threads = 0;
  }

  /* Make new thread pool */
  mr_thpool_* thpool_p;
  thpool_p = (struct mr_thpool_*)MR_ALLOC(sizeof(struct mr_thpool_));
  if (thpool_p == NULL) {
    err("thpool_init(): Could not allocate memory for thread pool\n");
    return NULL;
  }
  thpool_p->num_threads_alive = 0;
  thpool_p->num_threads_working = 0;
  thpool_p->total_num_of_threads = num_threads;
  thpool_p->is_threads_started = 0;

  /* Initialise the job queue */
  if (jobqueue_init(&thpool_p->jobqueue) == -1) {
    err("thpool_init(): Could not allocate memory for job queue\n");
    MR_FREE(thpool_p);
    return NULL;
  }

  /* Make threads in pool */
  thpool_p->threads = (struct mr_thread**)MR_ALLOC(num_threads * sizeof(struct mr_thread*));
  if (thpool_p->threads == NULL) {
    err("thpool_init(): Could not allocate memory for threads\n");
    jobqueue_destroy(&thpool_p->jobqueue);
    MR_FREE(thpool_p);
    return NULL;
  }

  pthread_mutex_init(&(thpool_p->thcount_lock), NULL);
  pthread_mutex_init(&(thpool_p->is_threads_started_lock), NULL);
  pthread_cond_init(&thpool_p->threads_all_idle, NULL);

  return thpool_p;
}

/* Add work to the thread pool */
int mr_thpool_add_work(mr_thpool_* thpool_p, void (*function_p)(void*), void* arg_p) {
  mr_job* newjob;

  mr_thpool_start_threads(thpool_p);

  newjob = (struct mr_job*)MR_ALLOC(sizeof(struct mr_job));
  if (newjob == NULL) {
    err("thpool_add_work(): Could not allocate memory for new job\n");
    return -1;
  }

  /* add function and argument */
  newjob->function = function_p;
  newjob->arg = arg_p;

  /* add job to queue */
  jobqueue_push(&thpool_p->jobqueue, newjob);

  return 0;
}

/* Wait until all jobs have finished */
void mr_thpool_wait(mr_thpool_* thpool_p) {
  pthread_mutex_lock(&thpool_p->thcount_lock);
  while (thpool_p->jobqueue.len || thpool_p->num_threads_working) {
    pthread_cond_wait(&thpool_p->threads_all_idle, &thpool_p->thcount_lock);
  }
  pthread_mutex_unlock(&thpool_p->thcount_lock);
}

/* Destroy the threadpool */
void mr_thpool_destroy(mr_thpool_* thpool_p) {
  /* No need to destory if it's NULL */
  if (thpool_p == NULL) return;

  volatile int threads_total = thpool_p->num_threads_alive;

  /* End each thread 's infinite loop */
  threads_keepalive = 0;

  /* Give one second to kill idle threads */
  double TIMEOUT = 1.0;
  time_t start, end;
  double tpassed = 0.0;
  time(&start);
  while (tpassed < TIMEOUT && thpool_p->num_threads_alive) {
    bsem_post_all(thpool_p->jobqueue.has_jobs);
    time(&end);
    tpassed = difftime(end, start);
  }

  /* Poll remaining threads */
  while (thpool_p->num_threads_alive) {
    bsem_post_all(thpool_p->jobqueue.has_jobs);
    sleep(1);
  }

  /* Job queue cleanup */
  jobqueue_destroy(&thpool_p->jobqueue);
  /* Deallocs */
  int n;
  for (n = 0; n < threads_total; n++) {
    thread_destroy(thpool_p->threads[n]);
  }
  MR_FREE(thpool_p->threads);
  MR_FREE(thpool_p);
}

/* Pause all threads in threadpool */
void mr_thpool_pause(mr_thpool_* thpool_p) {
  int n;
  for (n = 0; n < thpool_p->num_threads_alive; n++) {
    pthread_kill(thpool_p->threads[n]->pthread, SIGUSR2);
  }
}

/* Resume all threads in threadpool */
void mr_thpool_resume(mr_thpool_* thpool_p) {
  // resuming a single threadpool hasn't been
  // implemented yet, meanwhile this supresses
  // the warnings
  (void)thpool_p;

  threads_on_hold = 0;
}

int mr_thpool_num_threads_working(mr_thpool_* thpool_p) {
  return thpool_p->num_threads_working;
}

/* ============================ THREAD ============================== */

/* Initialize a thread in the thread pool
 *
 * @param thread        address to the pointer of the thread to be created
 * @param id            id to be given to the thread
 * @return 0 on success, -1 otherwise.
 */
static int thread_init(mr_thpool_* thpool_p, struct mr_thread** thread_p, int id) {

  *thread_p = (struct mr_thread*)MR_ALLOC(sizeof(struct mr_thread));
  if (thread_p == NULL) {
    err("thread_init(): Could not allocate memory for thread\n");
    return -1;
  }

  (*thread_p)->thpool_p = thpool_p;
  (*thread_p)->id = id;

  pthread_create(&(*thread_p)->pthread, NULL, (void*)thread_do, (*thread_p));
  pthread_detach((*thread_p)->pthread);
  return 0;
}

/* Sets the calling thread on hold */
static void thread_hold(int sig_id) {
  (void)sig_id;
  threads_on_hold = 1;
  while (threads_on_hold) {
    sleep(1);
  }
}

/* What each thread is doing
 *
 * In principle this is an endless loop. The only time this loop gets interuppted is once
 * thpool_destroy() is invoked or the program exits.
 *
 * @param  thread        thread that will run this function
 * @return nothing
 */
static void* thread_do(struct mr_thread* thread_p) {
  /* Set thread name for profiling and debuging */
  char thread_name[128] = {0};
  sprintf(thread_name, xstr(MODULE_NAME)"-%d", thread_p->id);

#if defined(__linux__)
  /* Use prctl instead to prevent using _GNU_SOURCE flag and implicit declaration */
  prctl(PR_SET_NAME, thread_name);
#elif defined(__APPLE__) && defined(__MACH__)
  pthread_setname_np(thread_name);
#else
  err("thread_do(): pthread_setname_np is not supported on this system");
#endif

  /* Assure all threads have been created before starting serving */
  mr_thpool_* thpool_p = thread_p->thpool_p;

  /* Register signal handler */
  struct sigaction act;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  act.sa_handler = thread_hold;
  if (sigaction(SIGUSR2, &act, NULL) == -1) {
    err("thread_do(): cannot handle SIGUSR1");
  }

  /* Mark thread as alive (initialized) */
  pthread_mutex_lock(&thpool_p->thcount_lock);
  thpool_p->num_threads_alive += 1;
  pthread_mutex_unlock(&thpool_p->thcount_lock);

  while (threads_keepalive) {

    bsem_wait(thpool_p->jobqueue.has_jobs);

    if (threads_keepalive) {

      pthread_mutex_lock(&thpool_p->thcount_lock);
      thpool_p->num_threads_working++;
      pthread_mutex_unlock(&thpool_p->thcount_lock);

      /* Read job from queue and execute it */
      void (*func_buff)(void*);
      void* arg_buff;
      mr_job* job_p = jobqueue_pull(&thpool_p->jobqueue);
      if (job_p) {
        func_buff = job_p->function;
        arg_buff = job_p->arg;
        func_buff(arg_buff);
        MR_FREE(job_p);
      }

      pthread_mutex_lock(&thpool_p->thcount_lock);
      thpool_p->num_threads_working--;
      if (!thpool_p->num_threads_working) {
        pthread_cond_signal(&thpool_p->threads_all_idle);
      }
      pthread_mutex_unlock(&thpool_p->thcount_lock);
    }
  }
  pthread_mutex_lock(&thpool_p->thcount_lock);
  thpool_p->num_threads_alive--;
  pthread_mutex_unlock(&thpool_p->thcount_lock);

  return NULL;
}

/* Frees a thread  */
static void thread_destroy(mr_thread* thread_p) {
  MR_FREE(thread_p);
}

/* ============================ JOB QUEUE =========================== */

/* Initialize queue */
static int jobqueue_init(mr_jobqueue* jobqueue_p) {
  jobqueue_p->len = 0;
  jobqueue_p->front = NULL;
  jobqueue_p->rear = NULL;

  jobqueue_p->has_jobs = (struct mr_bsem*)MR_ALLOC(sizeof(struct mr_bsem));
  if (jobqueue_p->has_jobs == NULL) {
    return -1;
  }

  pthread_mutex_init(&(jobqueue_p->rwmutex), NULL);
  bsem_init(jobqueue_p->has_jobs, 0);

  return 0;
}

/* Clear the queue */
static void jobqueue_clear(mr_jobqueue* jobqueue_p) {

  while (jobqueue_p->len) {
    MR_FREE(jobqueue_pull(jobqueue_p));
  }

  jobqueue_p->front = NULL;
  jobqueue_p->rear = NULL;
  bsem_reset(jobqueue_p->has_jobs);
  jobqueue_p->len = 0;
}

/* Add (allocated) job to queue
 */
static void jobqueue_push(mr_jobqueue* jobqueue_p, struct mr_job* newjob) {

  pthread_mutex_lock(&jobqueue_p->rwmutex);
  newjob->prev = NULL;

  switch (jobqueue_p->len) {

    case 0: /* if no jobs in queue */
      jobqueue_p->front = newjob;
      jobqueue_p->rear = newjob;
      break;

    default: /* if jobs in queue */
      jobqueue_p->rear->prev = newjob;
      jobqueue_p->rear = newjob;
  }
  jobqueue_p->len++;

  bsem_post(jobqueue_p->has_jobs);
  pthread_mutex_unlock(&jobqueue_p->rwmutex);
}

/* Get first job from queue(removes it from queue)
<<<<<<< HEAD
 *
 * Notice: Caller MUST hold a mutex
=======
>>>>>>> da2c0fe45e43ce0937f272c8cd2704bdc0afb490
 */
static struct mr_job* jobqueue_pull(mr_jobqueue* jobqueue_p) {

  pthread_mutex_lock(&jobqueue_p->rwmutex);
  mr_job* job_p = jobqueue_p->front;

  switch (jobqueue_p->len) {

    case 0: /* if no jobs in queue */
      break;

    case 1: /* if one job in queue */
      jobqueue_p->front = NULL;
      jobqueue_p->rear = NULL;
      jobqueue_p->len = 0;
      break;

    default: /* if >1 jobs in queue */
      jobqueue_p->front = job_p->prev;
      jobqueue_p->len--;
      /* more than one job in queue -> post it */
      bsem_post(jobqueue_p->has_jobs);
  }

  pthread_mutex_unlock(&jobqueue_p->rwmutex);
  return job_p;
}

/* Free all queue resources back to the system */
static void jobqueue_destroy(mr_jobqueue* jobqueue_p) {
  jobqueue_clear(jobqueue_p);
  MR_FREE(jobqueue_p->has_jobs);
}

/* ======================== SYNCHRONISATION ========================= */

/* Init semaphore to 1 or 0 */
static void bsem_init(mr_bsem* bsem_p, int value) {
  if (value < 0 || value > 1) {
    err("bsem_init(): Binary semaphore can take only values 1 or 0");
    exit(1);
  }
  pthread_mutex_init(&(bsem_p->mutex), NULL);
  pthread_cond_init(&(bsem_p->cond), NULL);
  bsem_p->v = value;
}

/* Reset semaphore to 0 */
static void bsem_reset(mr_bsem* bsem_p) {
  bsem_init(bsem_p, 0);
}

/* Post to at least one thread */
static void bsem_post(mr_bsem* bsem_p) {
  pthread_mutex_lock(&bsem_p->mutex);
  bsem_p->v = 1;
  pthread_cond_signal(&bsem_p->cond);
  pthread_mutex_unlock(&bsem_p->mutex);
}

/* Post to all threads */
static void bsem_post_all(mr_bsem* bsem_p) {
  pthread_mutex_lock(&bsem_p->mutex);
  bsem_p->v = 1;
  pthread_cond_broadcast(&bsem_p->cond);
  pthread_mutex_unlock(&bsem_p->mutex);
}

/* Wait on semaphore until semaphore has value 0 */
static void bsem_wait(mr_bsem* bsem_p) {
  pthread_mutex_lock(&bsem_p->mutex);
  while (bsem_p->v != 1) {
    pthread_cond_wait(&bsem_p->cond, &bsem_p->mutex);
  }
  bsem_p->v = 0;
  pthread_mutex_unlock(&bsem_p->mutex);
}
