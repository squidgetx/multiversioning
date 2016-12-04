/**
 * mv_txn_deserializer.cc
 * Author: David Hatch
 */

#include "logging/mv_txn_deserializer.h"

#include "logging_experiment.h"
#include "db.h"
#include "small_bank.h"
#include "ycsb.h"
#include "txn_type.h"

txn* MVTransactionDeserializer::deserialize(TxnType type, IReadBuffer *readBuffer) {
    txn *ret = nullptr;
    switch (type) {
        case TxnType::SB_LOAD_CUSTOMER_RANGE:
            ret = SmallBank::LoadCustomerRange::deserialize(readBuffer);
            break;
        case TxnType::SB_BALANCE:
            assert(false);
            break;
        case TxnType::SB_DEPOSIT_CHECKING:
            ret = SmallBank::DepositChecking::deserialize(readBuffer);
            break;
        case TxnType::SB_TRANSACT_SAVING:
            ret = SmallBank::TransactSaving::deserialize(readBuffer);
            break;
        case TxnType::SB_AMALGAMATE:
            ret = SmallBank::Amalgamate::deserialize(readBuffer);
            break;
        case TxnType::SB_WRITE_CHECK:
            ret = SmallBank::WriteCheck::deserialize(readBuffer);
            break;
        case TxnType::YCSB_INSERT:
            ret = ycsb_insert::deserialize(readBuffer);
            break;
        case TxnType::YCSB_READONLY:
            assert(false);
            break;
        case TxnType::YCSB_RMW:
            ret = ycsb_rmw::deserialize(readBuffer);
            break;
        case TxnType::LOGGING_INSERT:
            ret = LoggingExperiment::InsertValue::deserialize(readBuffer);
            break;
        case TxnType::LOGGING_READ:
            assert(false);
            break;
        default:
            assert(false);
    }

    return ret;
}
