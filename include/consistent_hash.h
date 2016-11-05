#ifndef CONSISTENT_HASH_HH
#define CONSISTENT_HASH_HH

class ConsistentHash {
  public:
    // initializer
    ConsistentHash(uint64_t* _partition_map, 
                    uint64_t** _thread_map,
                    uint64_t _threads, 
                    uint64_t _partitions) {
      this->partition_map = _partition_map;
      this->thread_map = _thread_map;

      this->num_partitions = _partitions;
      this->num_threads = _threads;
      this->unique_thread_count = _threads;

      for (uint64_t i = 0; i < this->num_partitions; i++) {
        this->partition_map[i] = i % this->num_threads;
        this->thread_map[i % this->num_threads][(uint64_t) i / 64] |= (uint64_t)1 << (i % 64);
      }
    }

    // TODO: write destructor

    // hashes the key to get thread_id
    uint64_t HashCCThread(CompositeKey& key) {
      uint64_t hash = CompositeKey::Hash(&key);
      return (uint64_t) this->partition_map[hash % this->num_partitions];
    }

    // adds a new thread (id = unique_thread_count++) - returns thread_id if SUCCESS
    //  1. num_threads++;
    //  2. assign it (num_partitions / num_threads) partitions at random
    //  3. in partition_map, reassign these partitions to the new thread
    uint64_t addThread();

    // removes a current thread with given id
    //  1. count number of partitions this thread is responsible for
    //  2. For each p in thread_map[thread_id]:
    //      - pick at random another partition and choose its thread as new_thread
    //      - remove p from thread_map[thread_id], add to thread_map[new_thread]
    //      - set partition_map[p] = new_thread
    void removeThread(uint64_t thread_id);

  //private:
    uint64_t num_threads;
    uint64_t num_partitions = 1000;

    // count to assign thread id when adding threads
    uint64_t unique_thread_count;


    // maps virtual partitions to thread_ids (size: num_partitions)
    uint64_t* partition_map;

    // maps thread ids to virtual partitions (size: num_threads)
    //  array of pointers to an array of uint64_t's, enough so each vpartition gets a bit
    uint64_t** thread_map;
};

#endif
