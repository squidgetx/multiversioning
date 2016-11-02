#ifndef         PREPROCESSOR_H_
#define         PREPROCESSOR_H_

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


struct MVSchedulerConfig {
  int cpuNumber;
  uint32_t threadId;
  size_t allocatorSize;         // Scheduler thread's local sticky allocator
  uint32_t numTables;           // Number of tables in the system
  size_t *tblPartitionSizes;    // Size of each table's partition
  
  uint32_t numOutputs;
        
  uint32_t numSubords;
  uint32_t numRecycleQueues;

  SimpleQueue<ActionBatch> *inputQueue;
  SimpleQueue<ActionBatch> *outputQueues;
  SimpleQueue<ActionBatch> **pubQueues;
  SimpleQueue<ActionBatch> **subQueues;
  SimpleQueue<MVRecordList> **recycleQueues;

  int worker_start;
  int worker_end;
};

/*
 * MVScheduler implements scheduling logic. The scheduler is partitioned across 
 * several physical cores.
 */
class MVScheduler : public Runnable {
        friend class SchedulerTest;
        
 private:
        static inline uint32_t GetCCThread(CompositeKey key);

    MVSchedulerConfig config;
    MVRecordAllocator *alloc;

    ConsistentHash *shards;
    MVTablePartition **partitions;

    uint32_t epoch;
    uint32_t txnCounter;
    uint64_t txnMask;

    uint32_t threadId;

 protected:
        virtual void StartWorking();
        void ScheduleTransaction(mv_action *action);
    virtual void Init();
    virtual void Recycle();
 public:
    
    void* operator new (std::size_t sz, int cpu) {
            return alloc_mem(sz, cpu);
    }

    static uint32_t NUM_CC_THREADS;
    MVScheduler(MVSchedulerConfig config);
};


#endif          /* PREPROCESSOR_H_ */
