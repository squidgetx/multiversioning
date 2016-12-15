/**
 * mv_action_batch_factory.cc
 * Author: David Hatch
 */

#include <cstdlib>

#include "mv_action_batch_factory.h"

#include "db.h"
#include "mv_action.h"

namespace {

void convert_keys(mv_action *action, txn *txn)
{
    uint32_t i, num_reads, num_rmws, num_writes, num_entries;
    struct big_key *array;

    /* Alloc an array to poke txn information. */
    num_reads = txn->num_reads();
    num_rmws = txn->num_rmws();
    num_writes = txn->num_writes();
    if (num_reads >= num_rmws && num_reads >= num_writes)
        num_entries = num_reads;
    else if (num_rmws >= num_writes)
        num_entries = num_rmws;
    else
        num_entries = num_writes;
    array = (struct big_key*)malloc(sizeof(struct big_key)*num_entries);

    /* Handle writes. */
    txn->get_writes(array);
    for (i = 0; i < num_writes; ++i)
        action->add_write_key(array[i].table_id, array[i].key, false);

    /* Handle rmws. */
    txn->get_rmws(array);
    for (i = 0; i < num_rmws; ++i)
        action->add_write_key(array[i].table_id, array[i].key, true);

    /* Handle reads. */
    txn->get_reads(array);
    for (i = 0; i < num_reads; ++i)
        action->add_read_key(array[i].table_id, array[i].key);
    if (num_rmws == 0 && num_writes == 0)
        action->__readonly = true;
    free(array);
}

mv_action* generate_mv_action(txn *txn)
{
    mv_action *action;

    /* Get the transaction's rw-sets. */
    action = new mv_action(txn);
    assert(action != nullptr);
    txn->set_translator(action);
    convert_keys(action, txn);

    action->setup_reverse_index();
    return action;
}

}

MVActionBatchFactory::MVActionBatchFactory(uint64_t epoch, uint64_t batchSize)
    : _batchSize(batchSize), _epoch(epoch) {
    reset();
}

bool MVActionBatchFactory::addTransaction(txn *txn) {
    if (_numAddedTxns >= _batchSize)
        return false;

    mv_action *action;
    uint64_t timestamp = CREATE_MV_TIMESTAMP(_epoch, _numAddedTxns);
    action = generate_mv_action(txn);
    action->__version = timestamp;
    _batch.actionBuf[_numAddedTxns++] = action;

    return _numAddedTxns <= _batchSize;
}


ActionBatch MVActionBatchFactory::getBatch() {
    _batch.numActions = _numAddedTxns;
    return _batch;
}

void MVActionBatchFactory::reset() {
    _batch = ActionBatch();
    _batch.numActions = _batchSize;
    _batch.actionBuf = (mv_action**)malloc(sizeof(mv_action*)*_batchSize);
    assert(_batch.actionBuf != NULL);
    _numAddedTxns = 0;
}
