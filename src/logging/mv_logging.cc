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
                     int cpuNumber) :
    Runnable(cpuNumber), inputQueue(inputQueue), outputQueue(outputQueue) {
    logFileFd = open("log.mvlog", O_DIRECT | O_DSYNC);
    if (logFileFd == -1) {
        // TODO is there some kind of existing error handling.
        std::cerr << "Fatal error: failed to open log file."
                  << strerror(errno)
                  << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

MVLogging::~MVLogging() {
    if (logFileFd >= 0) {
        close(logFileFd);
    }
}

void MVLogging::Init() {
}

void MVLogging::StartWorking() {
    while (true) {
        ActionBatch batch = inputQueue->DequeueBlocking();

        // Serialize batch.
        Buffer batchBuf;
        logBatch(batch, batchBuf);

        // Output batch.
        outputQueue->EnqueueBlocking(batch);

        // Write to file.
        batchBuf.writeToFile(logFileFd);
    }
}

void MVLogging::logAction(const mv_action *action, Buffer* buffer) {
    MVActionSerializer serializer;
    serializer.serialize(action, buffer);
}

void MVLogging::logBatch(ActionBatch batch, Buffer* buffer) {
    for (uint32_t i = 0; i < batch.numActions; i++) {
        const mv_action *action = batch.actionBuf[i];
        logAction(action, buffer);
    }
}
