#ifndef SRC_EVENT_LOOP_H_
#define SRC_EVENT_LOOP_H_

#include <stddef.h>

typedef void (*EventLoopTaskCallback)(void* ctx);

typedef struct MR_LoopTaskCtx MR_LoopTaskCtx;

/* Schedule a task with delay.
 * Return an opqueue struct
 * which allows to  later cancel
 * the task. */
MR_LoopTaskCtx* MR_EventLoopAddTaskWithDelay(EventLoopTaskCallback callback, void* ctx, size_t delayMs);

/* Cancel a delayed task, should only be called on
 * the even loop itself and only if the task did not
 * yet run. The user should make sure those conditions
 * apply. */
void MR_EventLoopDelayTaskCancel(MR_LoopTaskCtx* dtCtx);

void MR_EventLoopAddTask(EventLoopTaskCallback callback, void* ctx);
struct event_base* MR_EventLoopGet();
void MR_EventLoopStart();


#endif /* SRC_EVENT_LOOP_H_ */
