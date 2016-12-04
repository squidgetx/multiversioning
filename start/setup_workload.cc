#include <db.h>
#include <setup_workload.h>
#include <config.h>
#include <record_generator.h>
#include <uniform_generator.h>
#include <zipf_generator.h>
#include <ycsb.h>
#include <small_bank.h>
#include <logging_experiment.h>
#include <set>
#include <common.h>

RecordGenerator *my_gen = NULL;

static uint32_t logging_idx = 0;
static const uint32_t n_logging_records = 10000;

static uint64_t logging_value_for_idx(uint64_t idx) {
        return idx * 2;
}

uint64_t gen_unique_key(RecordGenerator *gen,
                        std::set<uint64_t> *seen_keys)
{
        while (true) {
                uint64_t key = gen->GenNext();
                if (seen_keys->find(key) == seen_keys->end()) {
                        seen_keys->insert(key);
                        return key;
                }
        }
}


txn* generate_small_bank_action(uint32_t num_records, bool read_only)
{
        txn *t;
        int mod, txn_type;
        long amount;
        uint64_t customer, from_customer, to_customer;
        
        if (read_only == true) 
                mod = 1;
        else 
                mod = 5;        
        txn_type = rand() % mod;
        if (txn_type == 0) {
                customer = (uint64_t)(rand() % num_records);
                t = new SmallBank::Balance(customer);
        } else if (txn_type == 1) {
                customer = (uint64_t)(rand() % num_records);
                amount = (long)(rand() % 25);
                t = new SmallBank::DepositChecking(customer, amount);
        } else if (txn_type == 2) {
                customer = (uint64_t)(rand() % num_records);
                amount = (long)(rand() % 25);
                t = new SmallBank::TransactSaving(customer, amount);
        } else if (txn_type == 3) {
                from_customer = (uint64_t)(rand() % num_records);
                do {
                        to_customer = (uint64_t)(rand() % num_records);
                } while (to_customer == from_customer);
                t = new SmallBank::Amalgamate(from_customer,
                                                     to_customer);
        } else if (txn_type == 4) {
                customer = (uint64_t)(rand() % num_records);
                amount = (long)(rand() % 25);
                if (rand() % 2 == 0) {
                        amount *= -1;
                }
                t = new SmallBank::WriteCheck(customer, amount);
        } else {
                assert(false);
        }
        return t;
}

txn* generate_ycsb_readonly(RecordGenerator *gen, workload_config config)
{
        using namespace std;
        
        uint32_t i;
        uint64_t key;
        txn *ret;
        vector<uint64_t> ctor_keys;
        set<uint64_t> seen_keys;

        /* Generate the read-set. */
        ret = NULL;
        for (i = 0; i < config.read_txn_size; ++i) {
                if (i < 10)
                        key = gen_unique_key(gen, &seen_keys);
                else
                        key = gen_unique_key(gen, &seen_keys);
                ctor_keys.push_back(key);
        }

        /* Generate the txn. */
        ret = new ycsb_readonly(ctor_keys);
        assert(ret != NULL);
        assert(ret->num_reads() == ctor_keys.size());
        assert(ret->num_writes() == 0);
        assert(ret->num_rmws() == 0);
        return ret;
}
/*
void gen_increasing(RecordGenerator *gen, uint64_t bound, 
                    vector<uint64_t> *seen)
{
        uint64_t key;
        uint32_t sz, i;
        
        sz = seen->size();
        while (true) {
                key = gen->GenNext();
                if (key < bound) {
                        for (i = 0; i < sz; ++i) 
                                if (key <= (*seen)[i])
                                        break;                
                        if (i == sz)
                                break;
                }
        }
        seen->push_back(key);
}
*/

txn* generate_ycsb_hot(RecordGenerator *gen, uint32_t num_rmws, 
                       uint32_t hot_position, 
                       workload_config w_conf)
{
        assert(hot_position < num_rmws);
        using namespace std;
        
        uint32_t i;
        uint64_t hot_key, delta, key;

        vector<uint64_t> reads, rmws;
        txn *ret;

        /* Generate the txn's read- and write-sets. */
        ret = NULL;
        
        /* 
         * Simulate degenerate contention case by forcing first rmw to always 
         * occur on a single hot record.
         */
        hot_key = (w_conf.num_records / num_rmws) * hot_position;
        delta = w_conf.num_records / num_rmws;

        for (i = 0; i < num_rmws; ++i) {
                if (i == hot_position) {
                        rmws.push_back(hot_key);
                } else {
                        while (true) {
                                key = (gen->GenNext() % delta) + delta*i;
                                if (key != hot_key)
                                        break;
                        }
                        rmws.push_back(key);
                }
        }
        assert(rmws.size() == num_rmws);
        for (i = 1; i < num_rmws; ++i) {
                assert(rmws[i-1] < rmws[i]);
                if (i == hot_position)
                        assert(rmws[i] == hot_key);
        }

        /* Create a txn to return. */
        assert(reads.size() == 0);
        ret = new ycsb_rmw(reads, rmws);
        assert(ret != NULL);
        assert(ret->num_reads() == reads.size());
        assert(ret->num_rmws() == rmws.size());
        assert(ret->num_writes() == 0);
        return ret;
}

txn* generate_ycsb_rmw(RecordGenerator *gen, uint32_t num_reads,
                       uint32_t num_rmws)
{
        using namespace std;
        
        uint32_t i;
        uint64_t key;
        set<uint64_t> seen_keys;
        vector<uint64_t> reads, rmws;
        txn *ret;

        /* Generate the txn's read- and write-sets. */
        ret = NULL;
        for (i = 0; i < num_rmws; ++i) {
                key = gen_unique_key(gen, &seen_keys);
                rmws.push_back(key);
        }

        for (i = 0; i < num_reads; ++i) {
                key = gen_unique_key(gen, &seen_keys);
                reads.push_back(key);
        }

        /* Create a txn to return. */
        ret = new ycsb_rmw(reads, rmws);
        assert(ret != NULL);
        assert(ret->num_reads() == reads.size());
        assert(ret->num_rmws() == rmws.size());
        assert(ret->num_writes() == 0);
        return ret;
}

txn* generate_ycsb_action(RecordGenerator *gen, workload_config config)
{
        uint32_t num_reads, num_rmws;
        int flip;

        num_reads = 0;
        num_rmws = 0;        
        flip = (uint32_t)rand() % 100;
        assert(flip >= 0 && flip < 100);
        if (flip < config.read_pct) {
                return generate_ycsb_readonly(gen, config);
        } else if (config.experiment == 0) {
                num_rmws = config.txn_size;
                num_reads = 0;
        } else if (config.experiment == 1) {
                assert(RMW_COUNT <= config.txn_size);
                num_rmws = RMW_COUNT;
                num_reads = config.txn_size - RMW_COUNT;
        } else if (config.experiment == 2) {
                num_rmws = config.txn_size;
                num_reads = 0;
                return generate_ycsb_hot(gen, num_rmws, 
                                         config.hot_position, 
                                         config);
        }
        return generate_ycsb_rmw(gen, num_reads, num_rmws);
}

uint32_t generate_small_bank_input(workload_config conf, txn ***loaders)
{
        using namespace SmallBank;
        
        uint32_t num_txns, i, remainder;
        uint64_t start, end;
        txn **ret;

        /* Each txn performs 1000 insertions. */
        num_txns = conf.num_records / 1000;
        remainder = conf.num_records % 1000;
        if (remainder > 0)
                num_txns += 1;
        ret = (txn**)malloc(sizeof(txn*)*num_txns);
        for (i = 0; i < num_txns; ++i) {
                start = 1000*i;
                if (remainder > 0 && i == num_txns - 1)
                        end = start + remainder;
                else
                        end = start + 1000;
                ret[i] = new LoadCustomerRange(start, end);
        }
        *loaders = ret;
        return num_txns;
}

uint32_t generate_ycsb_input(workload_config conf, txn ***loaders)
{
        using namespace SmallBank;
        uint32_t num_txns, i, remainder;
        uint64_t start, end;
        txn **ret;

        /* Each txn performs 1000 insertions. */
        num_txns = conf.num_records / 1000;
        remainder = conf.num_records % 1000;
        if (remainder > 0)
                num_txns += 1;
        ret = (txn**)malloc(sizeof(txn*)*num_txns);
        for (i = 0; i < num_txns; ++i) {
                start = 1000*i;
                if (remainder > 0 && i == num_txns - 1)
                        end = start + remainder;
                else 
                        end = start + 1000;
                ret[i] = new ycsb_insert(start, end);
        }
        *loaders = ret;
        return num_txns;
}

uint32_t generate_logging_experiment_input(workload_config conf, txn ***loaders) {
        txn **ret;

        uint64_t num_txns = conf.num_records;
        ret = (txn**)malloc(sizeof(txn*)*num_txns);
        for (uint32_t i = 0; i < num_txns; i++) {
                if (conf.experiment == 6) {
                        ret[i] = new LoggingExperiment::InsertValue(i, logging_value_for_idx(i));
                } else {
                        ret[i] = new LoggingExperiment::ReadValue(i, logging_value_for_idx(i));
                }
        }
        *loaders = ret;
        return num_txns;
}

txn* generate_logging_experiment_action(uint64_t record_idx) {
        return new LoggingExperiment::ReadValue(
                        record_idx % n_logging_records,
                        logging_value_for_idx(record_idx % n_logging_records));
}

uint32_t generate_input(workload_config conf, txn ***loaders)
{
        if (conf.experiment == 6 || conf.experiment == 5) {
                return generate_logging_experiment_input(conf, loaders);
        } else if (conf.experiment == 3 || conf.experiment == 4) {
                return generate_small_bank_input(conf, loaders);
        } else if (conf.experiment < 3) {
                return generate_ycsb_input(conf, loaders);
        } else {
                assert(false);
        }
}

txn* generate_transaction(workload_config config)
{
        txn *txn = NULL;
        
        if (config.experiment == 5 || config.experiment == 6) {
                txn = generate_logging_experiment_action(logging_idx++);
        } else if (config.experiment == 3) {
                txn = generate_small_bank_action(config.num_records, false);
        } else if (config.experiment == 4) {
                txn = generate_small_bank_action(config.num_records, true);
        } else if (config.experiment < 3) {
                if (config.distribution == UNIFORM && my_gen == NULL)
                        my_gen = new UniformGenerator(config.num_records);
                else if (config.distribution == ZIPFIAN && my_gen == NULL)
                        my_gen = new ZipfGenerator((uint64_t)config.num_records,
                                                config.theta);
                assert(my_gen != NULL);
                if (config.experiment < 3)
                        txn = generate_ycsb_action(my_gen, config);
        } else {
                assert(false);
        }
        assert(txn != NULL);
        return txn;
}
