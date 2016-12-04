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
#include "mv_action_batch_factory.h"
#include "logging/buffer.h"
#include "logging/read_buffer.h"
#include "logging/mv_action_serializer.h"
#include "logging/mv_txn_deserializer.h"

MVLogging::MVLogging(SimpleQueue<ActionBatch> *inputQueue,
                     SimpleQueue<ActionBatch> *outputQueue,
                     const char* logFileName,
                     bool allowRestore,
                     uint64_t batchSize,
                     uint64_t epochStart,
                     int cpuNumber) :
    Runnable(cpuNumber), inputQueue(inputQueue), outputQueue(outputQueue),
    allowRestore(allowRestore),
    logFileName(logFileName),
    batchSize(batchSize),
    epochNo(epochStart) {
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
    if (allowRestore) {
        restore();
    }

    while (true) {
        ActionBatch batch = inputQueue->DequeueBlocking();
        if (GET_MV_EPOCH(batch.actionBuf[0]->__version) != epochNo) {
            for (uint64_t i = 0; i < batch.numActions; i++) {
                batch.actionBuf[i]->__version = CREATE_MV_TIMESTAMP(epochNo, i);
            }
        }

        // Output batch.
        outputQueue->EnqueueBlocking(batch);
        epochNo++;

        // Serialize batch.
        // TODO Make sure batch isn't deallocated before writing to log.
        Buffer batchBuf;
        logBatch(batch, &batchBuf);

        // Write to file.
        batchBuf.writeToFile(logFileFd);
    }
}

void MVLogging::restore() {
    if (access(logFileName, R_OK) != 0) {
        return;
    }

    int restoreFd = open(logFileName, O_RDONLY);
    if (restoreFd == -1) {
        std::cerr << "Fatal error: failed to open log file for restore. Reason: "
                  << strerror(errno)
                  << std::endl;
        std::exit(EXIT_FAILURE);
    }

    MVActionBatchFactory batchFactory(epochNo, batchSize);
    MVTransactionDeserializer txnDeserializer;
    FileBuffer readBuffer(restoreFd);
    TxnType type;
    while (readBuffer.read(&type)) {
        uint64_t txnDataLength = 0;
        assert(readBuffer.read(&txnDataLength));

        ReadViewBuffer txnBuffer(&readBuffer, txnDataLength);
        auto *txn = txnDeserializer.deserialize(type, &txnBuffer);
        std::cerr << "Read txn " << static_cast<uint32_t>(txn->type()) << std::endl;
        while (!batchFactory.addTransaction(txn)) {
            ActionBatch batch = batchFactory.getBatch();
            outputQueue->EnqueueBlocking(batch);
            epochNo++;
            batchFactory.reset();
        }

        assert(txnBuffer.exhausted());
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
