// txn_type.h: Defines transaction types.

#ifndef _TXN_TYPE_H
#define _TXN_TYPE_H

#include <cstdint>

enum class TxnType : uint32_t {
  /** Small bank transactions */
  SB_LOAD_CUSTOMER_RANGE,
  SB_BALANCE,
  SB_DEPOSIT_CHECKING,
  SB_TRANSACT_SAVING,
  SB_AMALGAMATE,
  SB_WRITE_CHECK,
  /** YCSB transactions */
  YCSB_INSERT,
  YCSB_READONLY,
  YCSB_RMW
};

#endif /* _TXN_TYPE_H */
