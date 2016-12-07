/**
 * mv_action_batch_factory.h
 * Author: David Hatch
 */

#ifndef _MV_ACTION_BATCH_FACTORY_H
#define _MV_ACTION_BATCH_FACTORY_H

#include <cstdint>

#include "mv_action.h"
#include "db.h"
#include "no_copy.h"

/**
 * MVActionBatchFactory is responsible for creating action batches that are
 * prepared to be submitted to the scheduling layer.
 *
 * It is utilized via the experiment generation path, and the log restore
 * path.
 */
class MVActionBatchFactory {
public:
    MVActionBatchFactory(uint64_t epoch, uint64_t batchSize);
    ~MVActionBatchFactory() = default;
    MVActionBatchFactory(const MVActionBatchFactory& other) = default;

    MVActionBatchFactory& operator=(const MVActionBatchFactory& other) = default;

    /**
     * Add a transaction to the batch.
     *
     * txn: The transaction to add.  Memory must outlive the life of the batch.
     *
     * Returns: 'True' if the batch has space remaining, 'False' otherwise.
     */
    bool addTransaction(txn *txn);

    /**
     * Get the generated action batch.
     *
     * Precondition: addTransaction returned 'False', so that this batch
     * is full.
     */
    ActionBatch getBatch();

    /**
     * Reset the factory to be able to generate a new batch.
     */
    void reset();

    bool full() {
        return _numAddedTxns >= _batchSize;
    }
private:
    uint64_t _batchSize;
    uint64_t _epoch;

    ActionBatch _batch;
    uint64_t _numAddedTxns = 0;
};

#endif /* _MV_ACTION_BATCH_FACTORY_H */
