#ifndef LOGGING_EXPERIMENT_H_
#define LOGGING_EXPERIMENT_H_

#include <assert.h>

#include "txn_type.h"
#include "db.h"
#include "logging/read_buffer.h"

struct LoggingRecord {
    uint64_t value;
};

namespace LoggingExperiment {

class InsertValue : public txn {
public:
    InsertValue(uint64_t key, uint64_t value);
    virtual bool Run();
    virtual uint32_t num_writes();
    virtual void get_writes(struct big_key *array);

    virtual void serialize(IBuffer *buffer);
    virtual TxnType type() const override {
        return TxnType::LOGGING_INSERT;
    };

    static txn* deserialize(IReadBuffer *readBuffer);
private:
    uint64_t _key;
    uint64_t _value;
};

class ReadValue : public txn {
public:
    ReadValue(uint64_t key, uint64_t expectedValue);
    virtual bool Run();
    virtual uint32_t num_reads();
    virtual void get_reads(struct big_key *array);

    virtual void serialize(IBuffer *) { assert(false); };
    virtual TxnType type() const override {
        return TxnType::LOGGING_READ;
    };

    static txn* deserialize(IReadBuffer *) { assert(false); };
private:
    uint64_t _key;
    uint64_t _expectedValue;
};

}

#endif /* LOGGING_EXPERIMENT_H_ */
