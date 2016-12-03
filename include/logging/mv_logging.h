/**
 * mv_logging.h
 * Author: David Hatch
 */


#include <fstream>

#include "concurrent_queue.h"
#include "mv_action.h"
#include "no_copy.h"
#include "runnable.hh"

#include "db.h"
#include "logging/buffer.h"

/**
 * MVLogging: Provides durable logging in the MV system.
 */
class MVLogging : public Runnable {
    DISALLOW_COPY(MVLogging);
public:
    MVLogging(SimpleQueue<ActionBatch> *inputQueue,
              SimpleQueue<ActionBatch> *outputQueue,
              const char* logFileName,
              int cpuNumber);

    ~MVLogging();

   void logAction(const mv_action *action, Buffer* buffer);
   void logBatch(ActionBatch batch, Buffer* buffer);
protected:
    virtual void Init() override;
    virtual void StartWorking() override;

private:
    SimpleQueue<ActionBatch> *inputQueue;
    SimpleQueue<ActionBatch> *outputQueue;

    const char* logFileName;
    int logFileFd;
};
