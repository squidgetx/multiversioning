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
    /**
     * Initialize MVlogging.
     *
     * allowRestore: If true, restore from log file on startup.
     *
     * Otherwise refuse to start if the file exists.
     */
    MVLogging(SimpleQueue<ActionBatch> *inputQueue,
              SimpleQueue<ActionBatch> *outputQueue,
              SimpleQueue<bool> *exitSignalIn,
              SimpleQueue<bool> *exitSignalOut,
              const char* logFileName,
              bool allowRestore,
              uint64_t batchSize,
              uint64_t epochStart,
              bool asyncMode,
              int cpuNumber);

    ~MVLogging();
protected:
    virtual void Init() override;
    virtual void StartWorking() override;

private:
    void runAsync();
    void runSync();

    /**
     * Restore all transactions in the logFile.
     */
    void restore();
    void logAction(const mv_action *action, Buffer* buffer);
    void logBatch(ActionBatch batch, Buffer* buffer);

    SimpleQueue<ActionBatch> *inputQueue;
    SimpleQueue<ActionBatch> *outputQueue;

    /**
     * Used to coordinate waiting between the main thread and the
     * logging thread to make sure log is done before exit.
     */
    SimpleQueue<bool> *exitSignalIn;
    SimpleQueue<bool> *exitSignalOut;
    bool shouldExit = false;

    bool allowRestore;
    const char* logFileName;
    int logFileFd;

    uint64_t batchSize;
    uint64_t epochNo;

    bool asyncMode;
};
