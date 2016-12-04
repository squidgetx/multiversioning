#include <database.h>
#include <cpuinfo.h>
#include <config.h>
#include <eager_worker.h>
#include <uniform_generator.h>
#include <zipf_generator.h>
#include <logging_experiment.h>
#include <small_bank.h>
#include <setup_occ.h>
#include <setup_mv.h>
#include <setup_hek.h>
#include <setup_locking.h>
#include <algorithm>
#include <fstream>
#include <set>
#include <iostream>
#include <common.h>
#include <sys/mman.h>

#define RECYCLE_QUEUE_SIZE 64
#define INPUT_SIZE 1024
#define OFFSET 0
#define OFFSET_CORE(x) (x+OFFSET)

uint32_t GLOBAL_RECORD_SIZE;

Database DB(2);

uint64_t dbSize = ((uint64_t)1<<36);

uint32_t NUM_CC_THREADS;

int NumProcs;
uint32_t numLockingRecords;
uint64_t recordSize;

int main(int argc, char **argv) {

  srand(time(NULL));
  ExperimentConfig cfg(argc, argv);
  std::cout << cfg.ccType << "\n";

  
  
  if (cfg.ccType == MULTIVERSION) {
          if (cfg.mvConfig.experiment < 3) 
                  recordSize = cfg.mvConfig.recordSize;
          else if (cfg.mvConfig.experiment < 5)
                  recordSize = sizeof(SmallBankRecord);
          else if (cfg.mvConfig.experiment == 5 || cfg.mvConfig.experiment == 6)
                  recordSize = sizeof(LoggingRecord);
          else
                  assert(false);
          if (cfg.mvConfig.experiment < 3)
                  GLOBAL_RECORD_SIZE = 1000;
          else
                  GLOBAL_RECORD_SIZE = sizeof(SmallBankRecord);

          do_mv_experiment(cfg.mvConfig, cfg.get_workload_config());
          exit(0);
  } else if (cfg.ccType == LOCKING) {
          recordSize = cfg.lockConfig.record_size;
          assert(recordSize == 8 || recordSize == 1000);
          assert(cfg.lockConfig.distribution < 2);
          if (cfg.lockConfig.experiment < 3)
                  GLOBAL_RECORD_SIZE = 1000;
          else
                  GLOBAL_RECORD_SIZE = sizeof(SmallBankRecord);
          locking_experiment(cfg.lockConfig, cfg.get_workload_config());
          exit(0);
  } else if (cfg.ccType == OCC) {
          recordSize = cfg.occConfig.recordSize;
          assert(cfg.occConfig.distribution < 2);
          assert(recordSize == 8 || recordSize == 1000);
          if (cfg.occConfig.experiment < 3)
                  GLOBAL_RECORD_SIZE = 1000;
          else
                  GLOBAL_RECORD_SIZE = sizeof(SmallBankRecord);

          occ_experiment(cfg.occConfig, cfg.get_workload_config());
          exit(0);
  } else if (cfg.ccType == HEK) {
          recordSize = cfg.hek_conf.record_size;
          assert(cfg.hek_conf.distribution < 2);
          if (cfg.hek_conf.experiment < 3)
                  GLOBAL_RECORD_SIZE = 1000;
          else
                  GLOBAL_RECORD_SIZE = sizeof(SmallBankRecord);
          assert(recordSize == 8 || recordSize == 1000);
          do_hekaton_experiment(cfg.hek_conf, cfg.get_workload_config());
          exit(0);
  }
}
