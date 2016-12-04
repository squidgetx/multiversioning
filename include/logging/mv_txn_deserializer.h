/**
 * mv_txn_deserializer.h
 * Author: David Hatch
 */

#ifndef _LOGGING_MV_TXN_DESERIALIZER_H
#define _LOGGING_MV_TXN_DESERIALIZER_H

#include "logging/read_buffer.h"
#include "txn_type.h"

class txn;

class MVTransactionDeserializer {
public:
    MVTransactionDeserializer() = default;
    txn* deserialize(TxnType type, IReadBuffer *readBuffer);
};

#endif /* _LOGGING_MV_TXN_DESERIALIZER_H */
