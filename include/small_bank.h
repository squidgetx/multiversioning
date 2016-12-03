#ifndef         SMALL_BANK_H_
#define         SMALL_BANK_H_

#include <vector>
#include <cstdlib>

#include <db.h>
#include <logging/buffer.h>
#include <mv_action.h>
#include <occ_action.h>
#include <action.h>

#include <assert.h>

#define METADATA_SIZE 0

enum SmallBankTable {
  CHECKING = 0,
  SAVINGS,
};

struct SmallBankRecord {
        long amount;
        char meta_data[METADATA_SIZE];
};

namespace SmallBank {

        class LoadCustomerRange : public txn {
        private:
                const uint64_t _customer_start;
                const uint64_t _customer_end;

                std::vector<long> balances;
                std::vector<uint64_t> customers;
                
        public:
                LoadCustomerRange(uint64_t customer_start,
                                  uint64_t customer_end);
                virtual bool Run();
                virtual uint32_t num_writes();
                virtual void get_writes(struct big_key *array);

                virtual void serialize(IBuffer *buffer);

                virtual TxnType type() const override {
                  return TxnType::SB_LOAD_CUSTOMER_RANGE;
                }
        };
        
        class Balance : public txn {
        private:
                long totalBalance;
                uint64_t customer_id;
        public:
                Balance(uint64_t customer_id);
                virtual bool Run();
                virtual uint32_t num_reads();
                virtual void get_reads(struct big_key *array);

                virtual void serialize(IBuffer *) { assert(false) };

                virtual TxnType type() const override {
                  return TxnType::SB_BALANCE;
                }
        };

        class DepositChecking : public txn {
        private:
                long amount;
                uint64_t customer_id;
        
        public:
                DepositChecking(uint64_t customer, long amount);               
                virtual bool Run();
                virtual uint32_t num_rmws();
                virtual void get_rmws(struct big_key *array);

                virtual void serialize(IBuffer *buffer);

                virtual TxnType type() const override {
                  return TxnType::SB_DEPOSIT_CHECKING;
                }
        };

        class TransactSaving : public txn {    
        private:
                long amount;
                uint64_t customer_id;
                
        public:
                TransactSaving(uint64_t customer, long amount);
                virtual bool Run();
                virtual uint32_t num_rmws();
                virtual void get_rmws(struct big_key *array);

                virtual void serialize(IBuffer *buffer);
                virtual TxnType type() const override {
                  return TxnType::SB_TRANSACT_SAVING;
                }
        };

        class Amalgamate : public txn {
                char *meta_data;
                uint64_t from_customer;
                uint64_t to_customer;
        public:
                Amalgamate(uint64_t fromCustomer, uint64_t toCustomer);
                virtual bool Run();
                virtual uint32_t num_rmws();
                virtual void get_rmws(struct big_key *array);

                virtual void serialize(IBuffer *buffer);
                virtual TxnType type() const override {
                  return TxnType::SB_AMALGAMATE;
                }
        };
  
        class WriteCheck : public txn {
        private:
                long check_amount;
                uint64_t customer_id;
        public:
                WriteCheck(uint64_t customer, long check_amount);
                virtual bool Run();

                virtual uint32_t num_reads();
                virtual uint32_t num_rmws();
                virtual void get_reads(struct big_key *array);
                virtual void get_rmws(struct big_key *array);

                virtual void serialize(IBuffer *buffer);
                virtual TxnType type() const override {
                  return TxnType::SB_WRITE_CHECK;
                }
        };  
};

#endif          // SMALL_BANK_H_
