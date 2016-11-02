#ifndef CONSISTENT_HASH_HH
#define CONSISTENT_HASH_HH

class ConsistentHash {
  public:
    // initializer - 
    //  1. initialize variables
    //  2. allocate partition_map and thread_map (done in setup_mv)
    //  3. allocate each _partition in round robin format for threads
    ConsistentHash(uint64_t _threads, uint64_t _partitions) {
      num_partitions = _partitions;
      num_threads = _threads;
      unique_thread_count = _threads;
    }

    // hashes the key to get thread_id - 
    //  1. get virtual partition (key % num_partitions)
    //  2. get thread_id of partition from partition_map
    uint64_t GetCCThread(CompositeKey& key);

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

  private:
    uint64_t num_threads;
    uint64_t num_partitions;

    // count to assign thread id when adding threads
    uint64_t unique_thread_count;

    // maps virtual partitions to thread_ids (size: num_partitions)
    uint64_t* partition_map;

    // maps thread ids to virtual partitions (size: num_threads)
    //  array of pointers to an array of the virtual paritions that thread is responsible for
    uint64_t** thread_map;
}

#endif