#ifndef YCSB_H_
#define YCSB_H_

#include <db.h>
#include <logging/buffer.h>
#include <vector>

#include <assert.h>

#define YCSB_RECORD_SIZE 1000

using namespace std;

class ycsb_insert : public txn {
 private:
        uint64_t start;
        uint64_t end;

        void gen_rand(char *array);
        
 public:
        ycsb_insert(uint64_t start, uint64_t end);
        virtual bool Run();
        virtual uint32_t num_writes();
        virtual void get_writes(struct big_key *array);

        virtual void serialize(IBuffer *buffer);
        virtual TxnType type() const override {
                return TxnType::YCSB_INSERT;
        }
};

class ycsb_readonly : public txn {
 private:
        volatile uint64_t accumulated;
        vector<uint64_t> reads;
 public:
        ycsb_readonly(vector<uint64_t> reads);
        virtual bool Run();
        virtual uint32_t num_reads();
        virtual void get_reads(struct big_key *array);

        virtual void serialize(IBuffer *) { assert(false); };
        virtual TxnType type() const override {
                return TxnType::YCSB_READONLY;
        }
};

class ycsb_rmw : public txn {
 private:
        vector<uint64_t> reads;
        vector<uint64_t> writes;
        
 public:
        ycsb_rmw(vector<uint64_t> reads, vector<uint64_t> writes);
        virtual bool Run();
        virtual uint32_t num_reads();
        virtual uint32_t num_rmws();
        virtual void get_reads(struct big_key *array);
        virtual void get_rmws(struct big_key *array);

        virtual void serialize(IBuffer *buffer);
        virtual TxnType type() const override {
                return TxnType::YCSB_RMW;
        }
};

#endif // YCSB_H_
