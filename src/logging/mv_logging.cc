#include "logging/mv_logging.h"

#include <aio.h>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>
#include <memory>

#include "concurrent_queue.h"
#include "db.h"
#include "mv_action_batch_factory.h"
#include "logging/buffer.h"
#include "logging/read_buffer.h"
#include "logging/mv_action_serializer.h"
#include "logging/mv_txn_deserializer.h"

// The maximum number of batches we are allowed to be behind the rest of the
// system.
#define MAX_BEHIND 10

MVLogging::MVLogging(SimpleQueue<ActionBatch> *inputQueue,
                     SimpleQueue<ActionBatch> *outputQueue,
                     SimpleQueue<bool> *exitSignalIn,
                     SimpleQueue<bool> *exitSignalOut,
                     const char* logFileName,
                     bool allowRestore,
                     uint64_t batchSize,
                     uint64_t epochStart,
                     bool asyncMode,
                     int cpuNumber) :
    Runnable(cpuNumber), inputQueue(inputQueue), outputQueue(outputQueue),
    exitSignalIn(exitSignalIn),
    exitSignalOut(exitSignalOut),
    allowRestore(allowRestore),
    logFileName(logFileName),
    batchSize(batchSize),
    epochNo(epochStart),
    asyncMode(asyncMode) {
}

MVLogging::~MVLogging() {
    if (logFileFd >= 0) {
        close(logFileFd);
    }
}

void MVLogging::Init() {
    int dsync = !asyncMode ? O_DSYNC : 0;
    logFileFd = open(logFileName, O_CREAT | O_APPEND | O_WRONLY | dsync,
                     S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (logFileFd == -1) {
        // TODO is there some kind of existing error handling.
        std::cerr << "Fatal error: failed to open log file. Reason: "
                  << strerror(errno)
                  << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

struct IORequest {
    Buffer buf;
    std::deque<std::unique_ptr<aiocb>> aio_blocks;
    uint64_t epochNo;

    /**
     * Check whether more are completed.
     *
     * Returns 0 if there was no IO error.
     */
    int check_completion() {
        while (true) {
            if (aio_blocks.size() == 0)
                return 0;

            std::unique_ptr<aiocb>& ioblk = aio_blocks.front();
            int err;
            if ((err = aio_error(ioblk.get())) == EINPROGRESS) {
                break;
            } else if (err > 0) {
                return err;
            } else if (err == 0) {
                aio_blocks.pop_front();
                continue;
            } else {
                break;
            }
        }

        return 0;
    };

    bool completed() {
        return aio_blocks.size() == 0;
    };
};

void MVLogging::runAsync() {
    std::deque<IORequest> requests;

    while (true) {
        // Get new action batch.

        if (requests.size() < MAX_BEHIND && !shouldExit) {
            ActionBatch batch;
            if (inputQueue->Dequeue(&batch)) {

                if (GET_MV_EPOCH(batch.actionBuf[0]->__version) != epochNo) {
                    for (uint64_t i = 0; i < batch.numActions; i++) {
                        batch.actionBuf[i]->__version = CREATE_MV_TIMESTAMP(epochNo, i);
                    }
                }

                // Output batch.
                outputQueue->EnqueueBlocking(batch);

                requests.emplace_back();
                IORequest &request = requests.back();
                request.epochNo = epochNo;

                // Serialize batch.
                // TODO Make sure batch isn't deallocated before writing to log.
                logBatch(batch, &request.buf);

                // Write to file async.
                request.buf.writeToFileAsync(logFileFd, &request.aio_blocks);
                epochNo++;
            } else {
                // See if we should be exiting.
                exitSignalIn->Dequeue(&shouldExit);
            }
        }

        // Clean up any resources.  Acknowledgement would happen here.
        while (requests.size()) {
            IORequest& head = requests.front();
            int err;
            if ((err = head.check_completion()) != 0) {
                // Error
                std::cerr << "ASYNC IO Error. Reason: " << strerror(err) << std::endl;
                exit(EXIT_FAILURE);
            }

            if (head.completed()) {
                requests.pop_front();
            } else {
                break;
            }
        }

        if (shouldExit && requests.size() == 0) {
            exitSignalOut->EnqueueBlocking(true);
        }
    }
}

void MVLogging::runSync() {
    // We don't care if we are terminated since we run sync
    exitSignalOut->EnqueueBlocking(true);

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

void MVLogging::StartWorking() {
    if (allowRestore) {
        restore();
    }

    if (asyncMode) {
        runAsync();
    } else {
        runSync();
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
        txn->mark_is_restore();
        while (!batchFactory.addTransaction(txn)) {
            ActionBatch batch = batchFactory.getBatch();
            outputQueue->EnqueueBlocking(batch);
            epochNo++;
            batchFactory = MVActionBatchFactory(epochNo, batchSize);
        }

        assert(txnBuffer.exhausted());
    }

    ActionBatch batch = batchFactory.getBatch();
    // Enqueue a partial batch.
    if (batch.numActions < batchSize) {
            outputQueue->EnqueueBlocking(batch);
            epochNo++;
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
