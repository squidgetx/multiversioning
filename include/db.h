#ifndef DB_H_
#define DB_H_

#include <unordered_map>
#include <stdint.h>
#include <city.h>

#include "logging/buffer.h"
#include "txn_type.h"

class txn;

struct big_key {
        uint64_t key;
        uint32_t table_id;
        
        bool operator==(const big_key &other) const {
                return other.table_id == this->table_id &&
                other.key == this->key;
        }

        bool operator!=(const big_key &other) const {
                return !(*this == other);
        }
  
        bool operator<(const big_key &other) const {
                return ((this->table_id < other.table_id) || 
                        (
                         (this->table_id == other.table_id) &&
                         (this->key < other.key)
                         ));
        }
  
        bool operator>(const big_key &other) const {
                return ((this->table_id > other.table_id) ||
                        (
                         (this->table_id == other.table_id) &&
                         (this->key > other.key)
                         ));
        }
  
        bool operator<=(const big_key &other) const {
                return !(*this > other);
        }
  
        bool operator>=(const big_key &other) const {
                return !(*this < other);
        }

        static inline uint64_t Hash(const big_key *key) {
                return Hash128to64(std::make_pair(key->key,
                                                  (uint64_t)(key->table_id)));
        }
  
        static inline uint64_t HashKey(const big_key *key) {
                return Hash128to64(std::make_pair((uint64_t)key->table_id,
                                                  key->key));
        }
};

namespace std {
        template <>
                struct hash<big_key>
                {
                        std::size_t operator()(const big_key& k) const
                                {
                                        return big_key::Hash(&k);
                                }
                };
};

enum usage_type {
        READ,
        WRITE,
        RMW,
};

class translator;

/*
 * Every transaction implementation must conform to this interface. Independent 
 * of concurrency control technique used.
 */
class txn {
 private:
        translator *trans;

        // TODO: a hack to keep track of whether transactions are part of the
        // experiment input.  Only used to prevent setup_mv from stopping prior
        // to completing restore process.
        bool is_restore_txn = false;
 protected:
        void* get_write_ref(uint64_t key, uint32_t table_id);
        void* get_read_ref(uint64_t key, uint32_t table_id);
        void* get_insert_ref(uint64_t key, uint32_t table_id);
        int txn_rand();
        
 public:
        txn();
        virtual bool Run() = 0;
        
        virtual uint32_t num_reads();
        virtual uint32_t num_writes();
        virtual uint32_t num_rmws();
        virtual void get_reads(struct big_key *array);
        virtual void get_writes(struct big_key *array);
        virtual void get_rmws(struct big_key *array);
        void set_translator(translator *trans);

        /**
         * Marks a transaction as coming from a log restore.
         */
        void mark_is_restore();
        bool get_is_restore();

        // Serialize the transaction into the buffer.
        //
        // Note: For readonly transactions, this can be an empty
        // implementation.  They will not be serialized to the log.
        //
        // It should invariant(false) if called.
        virtual void serialize(IBuffer *buffer) = 0;

        // Return a constant representing this transactions type, for
        // serialization.
        //
        // *Note* This MUST be unique across transaction implementations.
        virtual TxnType type() const = 0;
};

/*
 * Interface for all database implementations. We want to keep a uniform 
 * interface so that we have a single benchmark implementation that does not 
 * have to be repeated for each baseline we want to measure.
 */
class translator {
 protected:
        txn *t;
        
 public:
        translator(txn *t) { this->t = t; };
        virtual void *write_ref(uint64_t key, uint32_t table) = 0;
        virtual void *read(uint64_t key, uint32_t table) = 0;
        virtual int rand() = 0;

        bool get_is_restore() { return t->get_is_restore(); };
};


#endif // DB_H_
