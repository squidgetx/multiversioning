#ifndef         CONFIG_H_
#define         CONFIG_H_

#include <stdint.h>
#include <iostream>
#include <getopt.h>
#include <stdlib.h>
#include <unordered_map>
#include <cassert>

using namespace std;

static struct option long_options[] = {
  {"epoch_size", required_argument, NULL, 0},
  {"num_txns", required_argument, NULL, 1},
  {"num_lock_threads", required_argument, NULL, 2},
  {"num_worker_threads", required_argument, NULL, 3},
  {"num_cc_threads", required_argument, NULL, 4},
  {"num_records", required_argument, NULL, 5},
  {"num_contended", required_argument, NULL, 6},
  {"txn_size", required_argument, NULL, 7},
  {"cc_type", required_argument, NULL, 8},
  {"experiment", required_argument, NULL, 9},
  {"record_size", required_argument, NULL, 10},
  {"distribution", required_argument, NULL, 11},
  {"theta", required_argument, NULL, 12},
  {"occ_epoch", required_argument, NULL, 13},
  {"read_pct", required_argument, NULL, 14},
  {"read_txn_size", required_argument, NULL, 15},
  {"hot_position", required_argument, NULL, 16},
  {"num_ppp_threads", required_argument, NULL, 17},
  {"log_file", required_argument, NULL, 18},
  {"log_restore", no_argument, NULL, 19},
  {"log_async", no_argument, NULL, 20},
  {NULL, no_argument, NULL, 21},
};

enum distribution_t {
        UNIFORM = 0,
        ZIPFIAN,
};

struct workload_config
{
        uint32_t num_records;
        uint32_t txn_size;
        uint32_t experiment;
        distribution_t distribution;        
        double theta;
        uint32_t read_pct;
        uint32_t read_txn_size;
        uint32_t hot_position;
};

enum ConcurrencyControl {
  MULTIVERSION = 0,
  LOCKING = 1,
  OCC,
  HEK,
};

struct OCCConfig {
        uint32_t numThreads;
        uint32_t numTxns;
        uint32_t numRecords;
        uint32_t numContendedRecords;
        uint32_t txnSize;
        uint32_t experiment;
        uint64_t recordSize;
        uint32_t distribution;
        double theta;
        bool globalTime;
        uint64_t occ_epoch;
        int read_pct;
        int read_txn_size;
};

struct hek_config {
        uint32_t num_threads;
        uint32_t num_txns;
        uint32_t num_records;
        uint32_t num_contended_records;
        uint32_t txn_size;
        uint32_t experiment;
        uint64_t record_size;
        uint32_t distribution;
        double theta;
        bool global_time;
        uint64_t occ_epoch;
        int read_pct;
        int read_txn_size;
};


struct locking_config {
        uint32_t num_threads;
        uint32_t num_txns;
        uint32_t num_records;
        uint32_t num_contended_records;
        uint32_t txn_size;
        uint32_t experiment;
        uint64_t record_size;
        uint32_t distribution;
        double theta;
        int read_pct;
        int read_txn_size;
};

struct MVConfig {
  uint32_t numCCThreads;
  uint32_t numTxns;
  uint32_t epochSize;
  uint32_t numRecords;
  uint32_t numPPPThreads = 1;
  uint32_t numWorkerThreads;
  uint32_t txnSize;
  uint32_t experiment;
  uint64_t recordSize;
  uint32_t distribution;
  bool loggingEnabled;
  bool logRestore;
  bool logAsync;
  const char* logFileName;
  double theta;
        int read_pct;
        int read_txn_size;
        
};

class ExperimentConfig {
 private:
  enum OptionCode {
    EPOCH_SIZE = 0,
    NUM_TXNS,
    NUM_LOCK_THREADS,
    NUM_WORKER_THREADS,
    NUM_CC_THREADS,
    NUM_RECORDS,
    NUM_CONTENDED,
    TXN_SIZE,
    CC_TYPE,
    EXPERIMENT,
    RECORD_SIZE,
    DISTRIBUTION,
    THETA,
    OCC_EPOCH,
    READ_PCT,
    READ_TXN_SIZE,
    HOT_POSITION,
    NUM_PPP_THREADS,
    LOG_FILE,
    LOG_RESTORE,
    LOG_ASYNC
  };
  unordered_map<int, char*> argMap;

 public:
  ConcurrencyControl ccType;
  locking_config lockConfig;
  OCCConfig occConfig;
  MVConfig mvConfig;    
  hek_config hek_conf;
  workload_config w_conf;
  
  ExperimentConfig(int argc, char **argv) {
    ReadArgs(argc, argv);
    InitConfig();
  }
  
  workload_config get_workload_config()
  {
          return this->w_conf;
  }
  
  void InitConfig() {
    int ccType = -1;
    if ((argMap.count(CC_TYPE) == 0) || 
        ((ccType = atoi(argMap[CC_TYPE])) != MULTIVERSION && 
         ccType != LOCKING && ccType != OCC && ccType != HEK)) {
      std::cerr << "Undefined concurrency control type\n";
      exit(-1);
    }

    if (ccType == MULTIVERSION) {
      if (argMap.count(NUM_CC_THREADS) == 0 ||
          argMap.count(NUM_TXNS) == 0 ||
          argMap.count(EPOCH_SIZE) == 0 ||
          argMap.count(NUM_RECORDS) == 0 ||
          argMap.count(NUM_WORKER_THREADS) == 0 ||
          argMap.count(TXN_SIZE) == 0 || 
          argMap.count(EXPERIMENT) == 0 ||
          argMap.count(RECORD_SIZE) == 0 || 
          argMap.count(DISTRIBUTION) == 0 ||
          argMap.count(READ_PCT) == 0 ||
          argMap.count(READ_TXN_SIZE) == 0 ||
          argMap.count(NUM_PPP_THREADS) == 0) {
        std::cerr << "Missing one or more multiversion concurrency control params\n";
        std::cerr << "--" << long_options[NUM_CC_THREADS].name << "\n";
        std::cerr << "--" << long_options[NUM_TXNS].name << "\n";
        std::cerr << "--" << long_options[EPOCH_SIZE].name << "\n";
        std::cerr << "--" << long_options[NUM_RECORDS].name << "\n";
        std::cerr << "--" << long_options[NUM_WORKER_THREADS].name << "\n";
        std::cerr << "--" << long_options[TXN_SIZE].name << "\n";
        std::cerr << "--" << long_options[EXPERIMENT].name << "\n";
        std::cerr << "--" << long_options[RECORD_SIZE].name << "\n";
        std::cerr << "--" << long_options[DISTRIBUTION].name << "\n";
        std::cerr << "--" << long_options[READ_PCT].name << "\n";
        std::cerr << "--" << long_options[READ_TXN_SIZE].name << "\n";
        std::cerr << "--" << long_options[NUM_PPP_THREADS].name << "\n";
        exit(-1);        
      }
      
      if (atoi(argMap[DISTRIBUTION]) == 1 && argMap.count(THETA) == 0) {
        std::cerr << "Zipfian config parameter, theta, missing!\n";
        exit(-1);
      }
      
      mvConfig.numCCThreads = (uint32_t)atoi(argMap[NUM_CC_THREADS]);
      mvConfig.numTxns = (uint32_t)atoi(argMap[NUM_TXNS]);
      mvConfig.epochSize = (uint32_t)atoi(argMap[EPOCH_SIZE]);
      mvConfig.numRecords = (uint32_t)atoi(argMap[NUM_RECORDS]);
      mvConfig.numWorkerThreads = (uint32_t)atoi(argMap[NUM_WORKER_THREADS]);
      mvConfig.numPPPThreads = (uint32_t)atoi(argMap[NUM_PPP_THREADS]);
      mvConfig.txnSize = (uint32_t)atoi(argMap[TXN_SIZE]);
      mvConfig.experiment = (uint32_t)atoi(argMap[EXPERIMENT]);
      mvConfig.recordSize = (uint64_t)atoi(argMap[RECORD_SIZE]);
      mvConfig.distribution = (uint32_t)atoi(argMap[DISTRIBUTION]);
      mvConfig.read_pct = (int)atoi(argMap[READ_PCT]);
      mvConfig.read_txn_size = (int)atoi(argMap[READ_TXN_SIZE]);

      if (argMap.count(LOG_FILE) > 0) {
        mvConfig.loggingEnabled = true;
        mvConfig.logFileName = argMap[LOG_FILE];
      } else {
        mvConfig.loggingEnabled = false;
      }

      if (argMap.count(LOG_RESTORE) > 0) {
        mvConfig.logRestore = true;
      } else {
        mvConfig.logRestore = false;
      }
      
      if (argMap.count(LOG_ASYNC) > 0) {
        mvConfig.logAsync = true;
      } else {
        mvConfig.logAsync = false;
      }
      
      if (argMap.count(THETA) > 0) {
        mvConfig.theta = (double)atof(argMap[THETA]);
      }
      this->ccType = MULTIVERSION;
    } else if (ccType == LOCKING) {  // ccType == LOCKING
      
      if (argMap.count(NUM_LOCK_THREADS) == 0 || 
          argMap.count(NUM_TXNS) == 0 ||
          argMap.count(NUM_RECORDS) == 0 ||
          argMap.count(NUM_CONTENDED) == 0 ||
          argMap.count(TXN_SIZE) == 0 || 
          argMap.count(EXPERIMENT) == 0 ||
          argMap.count(RECORD_SIZE) == 0 || 
          argMap.count(DISTRIBUTION) == 0 ||
          argMap.count(READ_PCT) == 0 ||
          argMap.count(READ_TXN_SIZE) == 0) {
        
        std::cerr << "Missing one or more locking concurrency control params\n";
        std::cerr << "--" << long_options[NUM_LOCK_THREADS].name << "\n";
        std::cerr << "--" << long_options[NUM_TXNS].name << "\n";
        std::cerr << "--" << long_options[NUM_RECORDS].name << "\n";
        std::cerr << "--" << long_options[NUM_CONTENDED].name << "\n";
        std::cerr << "--" << long_options[TXN_SIZE].name << "\n";
        std::cerr << "--" << long_options[EXPERIMENT].name << "\n";
        std::cerr << "--" << long_options[RECORD_SIZE].name << "\n";
        std::cerr << "--" << long_options[DISTRIBUTION].name << "\n";
        std::cerr << "--" << long_options[READ_PCT].name << "\n";
        std::cerr << "--" << long_options[READ_TXN_SIZE].name << "\n";
        exit(-1);
      }

      if (atoi(argMap[DISTRIBUTION]) == 1 && argMap.count(THETA) == 0) {
        std::cerr << "Zipfian config parameter, theta, missing!\n";
        exit(-1);
      }
      
      lockConfig.num_threads = (uint32_t)atoi(argMap[NUM_LOCK_THREADS]);
      lockConfig.num_txns = (uint32_t)atoi(argMap[NUM_TXNS]);
      lockConfig.num_records = (uint32_t)atoi(argMap[NUM_RECORDS]);
      lockConfig.txn_size = (uint32_t)atoi(argMap[TXN_SIZE]);
      lockConfig.experiment = (uint32_t)atoi(argMap[EXPERIMENT]);
      lockConfig.record_size = (uint64_t)atoi(argMap[RECORD_SIZE]);
      lockConfig.distribution = (uint32_t)atoi(argMap[DISTRIBUTION]);
      lockConfig.read_pct = (int)atoi(argMap[READ_PCT]);
      lockConfig.read_txn_size = (int)atoi(argMap[READ_TXN_SIZE]);
      if (argMap.count(THETA) > 0) {
        lockConfig.theta = (double)atof(argMap[THETA]);
      }

      this->ccType = LOCKING;
    } else if (ccType == OCC) {

      if (argMap.count(NUM_LOCK_THREADS) == 0 || 
          argMap.count(NUM_TXNS) == 0 ||
          argMap.count(NUM_RECORDS) == 0 ||
          argMap.count(NUM_CONTENDED) == 0 ||
          argMap.count(TXN_SIZE) == 0 || 
          argMap.count(EXPERIMENT) == 0 ||
          argMap.count(RECORD_SIZE) == 0 || 
          argMap.count(DISTRIBUTION) == 0 ||
          argMap.count(OCC_EPOCH) == 0 ||
          argMap.count(READ_PCT) == 0 ||
          argMap.count(READ_TXN_SIZE) == 0) {
        
        std::cerr << "Missing one or more OCC params\n";
        std::cerr << "--" << long_options[NUM_LOCK_THREADS].name << "\n";
        std::cerr << "--" << long_options[NUM_TXNS].name << "\n";
        std::cerr << "--" << long_options[NUM_RECORDS].name << "\n";
        std::cerr << "--" << long_options[NUM_CONTENDED].name << "\n";
        std::cerr << "--" << long_options[TXN_SIZE].name << "\n";
        std::cerr << "--" << long_options[EXPERIMENT].name << "\n";
        std::cerr << "--" << long_options[RECORD_SIZE].name << "\n";
        std::cerr << "--" << long_options[DISTRIBUTION].name << "\n";
        std::cerr << "--" << long_options[OCC_EPOCH].name << "\n";
        std::cerr << "--" << long_options[READ_PCT].name << "\n";
        std::cerr << "--" << long_options[READ_TXN_SIZE].name << "\n";
        exit(-1);
      }
      occConfig.numThreads = (uint32_t)atoi(argMap[NUM_LOCK_THREADS]);
      occConfig.numTxns = (uint32_t)atoi(argMap[NUM_TXNS]);
      occConfig.numRecords = (uint32_t)atoi(argMap[NUM_RECORDS]);
      occConfig.numContendedRecords = 
        (uint32_t)atoi(argMap[NUM_CONTENDED]);
      occConfig.txnSize = (uint32_t)atoi(argMap[TXN_SIZE]);
      occConfig.experiment = (uint32_t)atoi(argMap[EXPERIMENT]);
      occConfig.recordSize = (uint64_t)atoi(argMap[RECORD_SIZE]);
      occConfig.distribution = (uint32_t)atoi(argMap[DISTRIBUTION]);
      occConfig.read_pct = (int)atoi(argMap[READ_PCT]);
      occConfig.read_txn_size = (int)atoi(argMap[READ_TXN_SIZE]);
      if (argMap.count(THETA) > 0) {
        occConfig.theta = (double)atof(argMap[THETA]);
      }
      occConfig.occ_epoch = (uint32_t)atoi(argMap[OCC_EPOCH]);              
      this->ccType = OCC;
    } else if (ccType == HEK) {

      if (argMap.count(NUM_LOCK_THREADS) == 0 || 
          argMap.count(NUM_TXNS) == 0 ||
          argMap.count(NUM_RECORDS) == 0 ||
          argMap.count(TXN_SIZE) == 0 || 
          argMap.count(EXPERIMENT) == 0 ||
          argMap.count(RECORD_SIZE) == 0 || 
          argMap.count(DISTRIBUTION) == 0 ||
          argMap.count(READ_PCT) == 0 ||
          argMap.count(READ_TXN_SIZE) == 0) {
        
        std::cerr << "Missing one or more HEK params\n";
        std::cerr << "--" << long_options[NUM_LOCK_THREADS].name << "\n";
        std::cerr << "--" << long_options[NUM_TXNS].name << "\n";
        std::cerr << "--" << long_options[NUM_RECORDS].name << "\n";
        std::cerr << "--" << long_options[TXN_SIZE].name << "\n";
        std::cerr << "--" << long_options[EXPERIMENT].name << "\n";
        std::cerr << "--" << long_options[RECORD_SIZE].name << "\n";
        std::cerr << "--" << long_options[DISTRIBUTION].name << "\n";
        std::cerr << "--" << long_options[READ_PCT].name << "\n";
        std::cerr << "--" << long_options[READ_TXN_SIZE].name << "\n";
        exit(-1);
      }

      hek_conf.num_threads = (uint32_t)atoi(argMap[NUM_LOCK_THREADS]);
      hek_conf.num_txns = (uint32_t)atoi(argMap[NUM_TXNS]);
      hek_conf.num_records = (uint32_t)atoi(argMap[NUM_RECORDS]);
      hek_conf.txn_size = (uint32_t)atoi(argMap[TXN_SIZE]);
      hek_conf.experiment = (uint32_t)atoi(argMap[EXPERIMENT]);
      hek_conf.record_size = (uint64_t)atoi(argMap[RECORD_SIZE]);
      hek_conf.distribution = (uint32_t)atoi(argMap[DISTRIBUTION]);
      hek_conf.read_pct = (int)atoi(argMap[READ_PCT]);
      hek_conf.read_txn_size = (int)atoi(argMap[READ_TXN_SIZE]);
      if (argMap.count(THETA) > 0) {
        hek_conf.theta = (double)atof(argMap[THETA]);
      }
      this->ccType = HEK;
            
    } else {
            assert(false);
    }

    /* Initialize workload config. */
    this->w_conf.num_records = (uint32_t)atoi(argMap[NUM_RECORDS]);
    this->w_conf.txn_size = (uint32_t)atoi(argMap[TXN_SIZE]);
    this->w_conf.experiment = (uint32_t)atoi(argMap[EXPERIMENT]);
    this->w_conf.distribution = (distribution_t)atoi(argMap[DISTRIBUTION]);
    assert(this->w_conf.distribution == UNIFORM ||
           this->w_conf.distribution == ZIPFIAN);
    this->w_conf.theta = 0.0;
    if (this->w_conf.distribution == ZIPFIAN) {
            assert(argMap.count(THETA) > 0);
            this->w_conf.theta = (double)atof(argMap[THETA]);
    }
    this->w_conf.read_pct = (uint32_t)atoi(argMap[READ_PCT]);
    this->w_conf.read_txn_size = (uint32_t)atoi(argMap[READ_TXN_SIZE]);
    
    /* 
     * If experiment varies hot record location, make sure that location has 
     * been specified. 
     */
    assert(w_conf.experiment != 2 || argMap.count(HOT_POSITION) != 0);
    if (w_conf.experiment == 2)
            w_conf.hot_position = (uint32_t)atoi(argMap[HOT_POSITION]);
  }

  void ReadArgs(int argc, char **argv) {

    int index = -1;
    while (getopt_long_only(argc, argv, "", long_options, &index) != -1) {
      if (index != -1 && argMap.count(index) == 0) {  // Correct argument
        argMap[index] = optarg;
      }
      
      else if (index == -1) { // Unknown argument
        std::cerr << "Error. Unknown argument\n";
        exit(-1);
      }
      else {  // Duplicate argument
        std::cerr << "Error duplicate argument\n";// <<  << "\n";
        exit(-1);
      }
      index = -1;
    }
  }
};

#endif          // CONFIG_H_
