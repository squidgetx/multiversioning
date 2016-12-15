#define LOGGING_EXPERIMENT_TABLE 0

#include "logging_experiment.h"

#include <iostream>

#include "logging/buffer.h"
#include "logging/read_buffer.h"

LoggingExperiment::InsertValue::InsertValue(uint64_t key, uint64_t value)
    : _key(key), _value(value) {
}

bool LoggingExperiment::InsertValue::Run() {
    LoggingRecord *rec = (LoggingRecord*)get_write_ref(_key, LOGGING_EXPERIMENT_TABLE);
    rec->value = _value;
    return true;
}

uint32_t LoggingExperiment::InsertValue::num_writes() {
    return 1;
}

void LoggingExperiment::InsertValue::get_writes(struct big_key *array) {
    array[0].key = _key;
    array[0].table_id = LOGGING_EXPERIMENT_TABLE;
}

void LoggingExperiment::InsertValue::serialize(IBuffer *buffer) {
    assert(buffer->write(_key));
    assert(buffer->write(_value));
}

txn* LoggingExperiment::InsertValue::deserialize(IReadBuffer *buffer) {
    uint64_t key, value;

    assert(buffer->read(&key));
    assert(buffer->read(&value));

    return new LoggingExperiment::InsertValue(key, value);
}

LoggingExperiment::ReadValue::ReadValue(uint64_t key, uint64_t expectedValue)
    : _key(key), _expectedValue(expectedValue) {
}

bool LoggingExperiment::ReadValue::Run() {
    LoggingRecord *rec = (LoggingRecord*)get_read_ref(_key, LOGGING_EXPERIMENT_TABLE);
    assert(rec->value == _expectedValue);
    return true;
}

uint32_t LoggingExperiment::ReadValue::num_reads() {
    return 1;
}

void LoggingExperiment::ReadValue::get_reads(struct big_key *array) {
    array[0].key = _key;
    array[0].table_id = LOGGING_EXPERIMENT_TABLE;
}
