#ifndef PREPROCESSOR_H_
#define PREPROCESSOR_H_

#include <sstream>

#include <mv_action.h>
#include <runnable.hh>
#include <concurrent_queue.h>
#include <numa.h>
#include <mv_table.h>


extern uint64_t recordSize;

class CompositeKey;
class mv_action;
class MVRecordAllocator;
class MVTablePartition;

struct MVActionDistributorConfig {
  uint32_t cpuNumber;
  uint32_t threadId;
  uint32_t numSubords;
  SimpleQueue<ActionBatch> *inputQueue;
  SimpleQueue<ActionBatch> *outputQueue;
  SimpleQueue<ActionBatch> **pubQueues;
  SimpleQueue<ActionBatch> **subQueues;
  int64_t label;
};

/* An MVActionDistributor is the real first stage of the transaction
 * processing pipeline (meant to replace MVActionHasher)
 * Its job is to take incoming batches and assign transactions to a
 * concurrency control worker thread (or virtual partition)
 *
 * The number of virtual partitions is fixed for now
 */

class MVActionDistributor : public Runnable {
  private:
    void log(string msg);
    MVActionDistributorConfig config;

    static uint32_t GetCCThread(CompositeKey& key);

  protected:

    virtual void Init();
    virtual void StartWorking();
    void ProcessAction(mv_action * action, int* lastActions, mv_action** batch, int index);
    bool leader;

  public:
    void* operator new(std::size_t sz, int cpu);

    MVActionDistributor(MVActionDistributorConfig config);
  static uint32_t NUM_CC_THREADS;
};


#endif    /* PREPROCESSOR_H_ */
