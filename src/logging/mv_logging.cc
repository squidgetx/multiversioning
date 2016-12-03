#include "logging/mv_logging.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>

#include "concurrent_queue.h"
#include "db.h"
#include "logging/buffer.h"
#include "logging/mv_action_serializer.h"

MVLogging::MVLogging(SimpleQueue<ActionBatch> *inputQueue,
                     SimpleQueue<ActionBatch> *outputQueue,
                     const char* logFileName,
                     int cpuNumber) :
    Runnable(cpuNumber), inputQueue(inputQueue), outputQueue(outputQueue), logFileName(logFileName) {
}

MVLogging::~MVLogging() {
    if (logFileFd >= 0) {
        close(logFileFd);
    }
}

void MVLogging::Init() {
    logFileFd = open(logFileName, O_CREAT | O_APPEND | O_WRONLY | O_DSYNC,
                     S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (logFileFd == -1) {
        // TODO is there some kind of existing error handling.
        std::cerr << "Fatal error: failed to open log file. Reason: "
                  << strerror(errno)
                  << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

void MVLogging::StartWorking() {
    while (true) {
        ActionBatch batch = inputQueue->DequeueBlocking();

        // Output batch.
        outputQueue->EnqueueBlocking(batch);

        // Serialize batch.
        // TODO Make sure batch isn't deallocated before writing to log.
        Buffer batchBuf;
        logBatch(batch, &batchBuf);

        // Write to file.
        batchBuf.writeToFile(logFileFd);
    }
}

void MVLogging::logAction(const mv_action *action, Buffer* buffer) {
    if (action->__readonly)
        return;

    MVActionSerializer serializer;
    serializer.serialize(action, buffer);
}

void MVLogging::logBatch(ActionBatch batch, Buffer* buffer) {
    for (uint32_t i = 0; i < batch.numActions; i++) {
        const mv_action *action = batch.actionBuf[i];
        logAction(action, buffer);
    }
}
