#include <executor.h>
#include <sstream>
#include <algorithm>

extern uint32_t GLOBAL_RECORD_SIZE;

PendingActionList::PendingActionList(uint32_t freeListSize) 
{
        freeList = (ActionListNode*)malloc(sizeof(ActionListNode)*freeListSize);
        memset(freeList, 0x00, sizeof(ActionListNode)*freeListSize);
  
        for (uint32_t i = 0; i < freeListSize; ++i) {
                freeList[i].next = &freeList[i+1];
        }
        freeList[freeListSize-1].next = NULL;
        this->head = NULL;
        this->tail = NULL;
        this->cursor = NULL;
        this->size = 0;
}

inline void PendingActionList::EnqueuePending(mv_action *action) 
{
        assert((head != NULL && tail != NULL && size > 0) || 
               (head == NULL && tail == NULL && size == 0));

        assert(freeList != NULL);
        assert(action != NULL);

        ActionListNode *node = freeList;
        freeList = freeList->next;  
        node->action = action;
        node->next = NULL;  
        node->prev = NULL;

        if (tail == NULL) {
                assert(head == NULL && size == 0);
                node->prev = NULL;
                head = node;  
                tail = node;
        }
        else {
                assert(head != NULL && size > 0);
                tail->next = node;
                node->prev = tail;
        }
        this->size += 1;
        tail = node;

        assert((head != NULL && tail != NULL && size > 0) || 
               (head == NULL && tail == NULL && size == 0));
}

inline void PendingActionList::DequeuePending(ActionListNode *node) 
{
        assert((head != NULL && tail != NULL && size > 0) || 
               (head == NULL && tail == NULL && size == 0));
        assert(node != cursor);
        assert(size > 0);

        if (node->next == NULL) {
                tail = node->prev;
        }
        else {
                node->next->prev = node->prev;
        }
  
        if (node->prev == NULL) {
                head = node->next;
        }
        else {
                node->prev->next = node->next;
        }
  
        node->prev = NULL;
        node->action = NULL;
        node->next = freeList;
        freeList = node;
        this->size -= 1;

        assert((head != NULL && tail != NULL && size > 0) || 
               (head == NULL && tail == NULL && size == 0));
}

inline void PendingActionList::ResetCursor() 
{
        cursor = head;
}

inline ActionListNode* PendingActionList::GetNext() 
{
        ActionListNode *temp = cursor;
        if (cursor != NULL) {
                cursor = cursor->next;
        }
        return temp;
}

inline bool PendingActionList::IsEmpty() 
{
        assert((head == NULL && size == 0) || (head != NULL && size > 0));
        return head == NULL;
}

inline uint32_t PendingActionList::Size() 
{
        return this->size;
}

Executor::Executor(ExecutorConfig cfg) : Runnable (cfg.cpu) 
{        
        std::stringstream msg;
        msg << "Executor thread " << cfg.threadId << " started on cpu " << cfg.cpu << "\n";
        std::cout << msg.str();


        this->config = cfg;
        this->counter = 0;
        this->pendingList = new (config.cpu) PendingActionList(1000);
        this->garbageBin = new (config.cpu) GarbageBin(config.garbageConfig);
}

void Executor::Init() 
{
}

/* Adjusts the low watermark of completed epoch#s across execution threads */
void Executor::adjust_lowwatermark()
{
        assert(config.threadId == 0);
        
        volatile uint32_t min_epoch;
        uint32_t i, temp;

        min_epoch = *config.epochPtr;
        for (i = 0; i < config.numExecutors; ++i) {
                barrier();
                temp = config.epochPtr[i];
                barrier();
                
                if (temp < min_epoch) {
                        min_epoch = temp;
                }                
        }        
        
        barrier();
        *config.lowWaterMarkPtr = min_epoch;
        barrier();
}

void Executor::StartWorking() 
{
        uint32_t epoch = 1;
        ActionBatch batch;

        while (true) {

                if (config.threadId == 0) {
                        while (!config.inputQueue->Dequeue(&batch)) {
                                adjust_lowwatermark();
                        }
                        ProcessBatch(batch);
                } else {
                        batch = config.inputQueue->DequeueBlocking();
                        ProcessBatch(batch);    
                }

                barrier();
                *config.epochPtr = epoch;
                barrier();
    
                // If this is the leader thread, try to advance the low-water mark to 
                // trigger garbage collection
                if (config.threadId == 0) 
                        adjust_lowwatermark();

                // Try to return records that are no longer visible to their owners
                garbageBin->FinishEpoch(epoch);
                epoch += 1;
        }
}

// Check if other worker threads have returned data to be recycled.
void Executor::RecycleData() 
{
        uint32_t numTables = config.numTables;
        uint32_t numQueues = config.numQueuesPerTable;
        for (uint32_t i = 0; i < numTables; ++i) {
                for (uint32_t j = 0; j < numQueues; ++j) {
                        RecordList recycled;
      
                        // Use non-blocking dequeue
                        while (config.recycleQueues[i*numQueues+j].Dequeue(&recycled)) {
                                //        std::cout << "Received " << recycled.count << " records\n";
                                allocators[i]->Recycle(recycled);
                        }
                }
        }
}

void Executor::ExecPending() 
{
        pendingList->ResetCursor();
        for (ActionListNode *node = pendingList->GetNext(); node != NULL; 
             node = pendingList->GetNext()) {
                if (ProcessSingle(node->action)) {
                        pendingList->DequeuePending(node);
                }
        }
}

/* Process a single batch of transactions. */
void Executor::ProcessBatch(const ActionBatch &batch) 
{
        bool isLogBatch = false;

        for (int i = config.threadId; i < (int)batch.numActions;
             i += config.numExecutors) {
                while (pendingList->Size() > 0) {
                        ExecPending();
                }

                mv_action *cur = batch.actionBuf[i];
                if (!isLogBatch && cur->get_is_restore()) {
                        isLogBatch = true;
                }

                if (!ProcessSingle(cur)) {
                        pendingList->EnqueuePending(cur);
                }
        }

        while (!pendingList->IsEmpty()) {
                ExecPending();
        }

        ActionBatch dummy = {NULL, 0};

        if (!isLogBatch) { // We don't want to output log restore batches to our output queue.
                config.outputQueue->EnqueueBlocking(dummy);  
        }
}

// Returns the epoch of the oldest pending record.
uint32_t Executor::DoPendingGC() 
{
        static uint64_t mask = 0xFFFFFFFF00000000;
        pendingGC->ResetCursor();
        for (ActionListNode *node = pendingGC->GetNext(); node != NULL; 
             node = pendingGC->GetNext()) {
                assert(node->action != NULL);
                if (ProcessSingleGC(node->action)) {      
                        pendingGC->DequeuePending(node);
                }
        }

        if (pendingGC->IsEmpty()) {
                return 0xFFFFFFFF;
        }
        else {
                pendingGC->ResetCursor();
                ActionListNode *node = pendingGC->GetNext();
                assert(node != NULL && node->action != NULL);
                return ((node->action->__version & mask) >> 32);
        }
}

bool Executor::ProcessSingleGC(mv_action *action) 
{
        assert(action->__state == SUBSTANTIATED);
        uint32_t numWrites = action->__writeset.size();
        bool ret = true;

        // Garbage collect the previous versions
        for (uint32_t i = 0; i < numWrites; ++i) {
                MVRecord *previous = action->__writeset[i].value->recordLink;
                if (previous != NULL) {
                        ret &= (previous->writer == NULL || 
                                previous->writer->__state == SUBSTANTIATED);
                }
        }
  
        if (ret) {
                for (uint32_t i = 0; i < numWrites; ++i) {
                        MVRecord *previous = action->__writeset[i].value->recordLink;
                        if (previous != NULL) {
        
                                // Since the previous txn has been substantiated, the record's value 
                                // shouldn't be NULL.
                                assert(previous->value != NULL);
                                garbageBin->AddRecord(previous->writingThread, 
                                                      action->__writeset[i].tableId,
                                                      previous->value);
                                //        garbageBin->AddMVRecord(action->__writeset[i].threadId, previous);
                        }
                }
        }
  
        return ret;
}

/* Take ownership of a transaction's execution. */
bool Executor::ProcessSingle(mv_action *action) 
{
        assert(action != NULL);
        volatile uint64_t state;
        barrier();
        state = action->__state;
        barrier();
        if (state != SUBSTANTIATED) {
                if (state == STICKY &&
                    cmp_and_swap(&action->__state, STICKY, PROCESSING)) {
                        if (ProcessTxn(action)) {
                                return true;
                        } else {
                                xchgq(&action->__state, STICKY);
                                return false;
                        }
                } else {      // cmp_and_swap failed
                        return false;
                }
        }
        else {        // action->state == SUBSTANTIATED
                return true;
        }
}

/* 
 * Check whether all of a transaction's conflicting ancestors have finished 
 * executing.
 */
bool Executor::check_ready(mv_action *action)
{
        uint32_t num_reads, num_writes, i;
        bool ready;
        mv_action *depend_action;
        MVRecord *prev;
        void *new_data, *old_data;
        uint32_t *read_index, *write_index;

        ready = true;
        num_reads = action->__readset.size();
        num_writes = action->__writeset.size();
        read_index = &action->read_index;
        write_index = &action->write_index;
        for (; *read_index < num_reads; *read_index += 1) {
                i = *read_index;

                assert(action->__readset[i].value != NULL);
                depend_action = action->__readset[i].value->writer;
                if (depend_action != NULL &&
                    depend_action->__state != SUBSTANTIATED &&
                    !ProcessSingle(depend_action)) {
                        ready = false;
                        break;
                }
        }
        for (; *write_index < num_writes; *write_index += 1) {
                i = *write_index;
                assert(action->__writeset[i].value != NULL);
                if (action->__writeset[i].is_rmw) {
                        prev = action->__writeset[i].value->recordLink;
                        assert(prev != NULL);
                        depend_action = prev->writer;
                        if (depend_action != NULL &&
                            depend_action->__state != SUBSTANTIATED && 
                            !ProcessSingle(depend_action)) {
                                ready = false;
                                break;
                        } else if (action->__writeset[i].initialized == false) {
                                /* 
                                 * XXX This is super hacky. Need to separate 
                                 * record allocation from version allocation to 
                                 * make it work -- "engineering work". 
                                 */
                                new_data = action->__writeset[i].value->value;
                                old_data = prev->value;
                                memcpy(new_data, old_data, GLOBAL_RECORD_SIZE);
                                action->__writeset[i].initialized = true;
                        }
                }
        }
        return ready;
}

/* 
 * Run a read-only transaction against an epoch which immediately precedes that 
 * of the transaction. 
*/
bool Executor::run_readonly(mv_action *action)
{
        assert(action != NULL);
        assert(action->__state == PROCESSING); 
        assert(action->__readonly == true);
        assert(action->__writeset.size() == 0);
        
        uint32_t num_reads, i;
        uint64_t read_epoch;
        MVRecord *rec, *snapshot;
        void *value_ptr;

        read_epoch = GET_MV_EPOCH(action->__version);
        num_reads = action->__readset.size();
        for (i = 0; i < num_reads; ++i) {
                rec = action->__readset[i].value;
                if (read_epoch == GET_MV_EPOCH(rec->createTimestamp)) 
                        snapshot = rec->epoch_ancestor;
                else 
                        snapshot = rec;

                barrier();
                value_ptr = snapshot->value;
                barrier();
                if (value_ptr == NULL)
                        return false;
        }
        action->exec = this;
        action->Run();
        xchgq(&action->__state, SUBSTANTIATED);
        return true;
        
}

/* 
 * Check that a transaction's conflicting predecessors have finished executing, 
 * and then execute the transaction. 
 */
bool Executor::ProcessTxn(mv_action *action) 
{
        assert(action != NULL && action->__state == PROCESSING);
        
        uint32_t num_writes, i;
        MVRecord *pred_version;

        if (action->__readonly == true) 
                return run_readonly(action);
        
        if (check_ready(action) == false)
                return false;
        
        action->exec = this;
        action->Run();
        xchgq(&action->__state, SUBSTANTIATED);

        /* Register over-written versions for garbage collection */
        num_writes = action->__writeset.size();
        for (i = 0; i < num_writes; ++i) {
                pred_version = action->__writeset[i].value->recordLink;
                if (pred_version != NULL) {
                        garbageBin->AddMVRecord(action->__writeset[i].threadId, pred_version);
                }
        }
        return true;
}

GarbageBin::GarbageBin(GarbageBinConfig config) 
{
        assert(sizeof(MVRecordList) == sizeof(RecordList));
        this->config = config;
        this->snapshotEpoch = 0;

        // total number of structs
        uint32_t numStructs = 
                (config.numCCThreads + config.numWorkers*config.numTables);
        uint32_t ccOffset = config.numCCThreads*sizeof(MVRecordList);
        uint32_t workerOffset = 
                config.numWorkers*config.numTables*sizeof(MVRecordList);

        // twice #structs: one for live, one for snapshot
        void *data = alloc_mem(2*numStructs*sizeof(MVRecordList), config.cpu);
        memset(data, 0x00, 2*numStructs*sizeof(MVRecordList));
  
        this->curStickies = (MVRecordList*)data;
        this->snapshotStickies = (MVRecordList*)((char*)data + ccOffset);
        for (uint32_t i = 0; i < 2*config.numCCThreads; ++i) {
                curStickies[i].tail = &curStickies[i].head;
                curStickies[i].head = NULL;
                curStickies[i].count = 0;
        }
  
        this->curRecords = (RecordList*)((char*)data + 2*ccOffset);
        this->snapshotRecords = (RecordList*)((char*)data + 2*ccOffset+workerOffset);
        for (uint32_t i = 0; i < 2*config.numWorkers; ++i) {
                curRecords[i].tail = &curRecords[i].head;
                curRecords[i].head = NULL;
                curRecords[i].count = 0;
        }
}

void GarbageBin::AddMVRecord(uint32_t ccThread, MVRecord *rec) 
{
        rec->allocLink = NULL;
        // rec->allocLink = NULL;
        *(curStickies[ccThread].tail) = rec;
        curStickies[ccThread].tail = &rec->allocLink;
        curStickies[ccThread].count += 1;
        assert(curStickies[ccThread].head != NULL);
}

void GarbageBin::AddRecord(uint32_t workerThread, uint32_t tableId, 
                           Record *rec) 
{
        //  rec->next = NULL;
        *(curRecords[workerThread*config.numTables+tableId].tail) = rec;
        curRecords[workerThread*config.numTables+tableId].tail = &rec->next;  
        curRecords[workerThread*config.numTables+tableId].count += 1;
}

void GarbageBin::ReturnGarbage() 
{
        for (uint32_t i = 0; i < config.numCCThreads; ++i) {
                // *curStickies[i].tail = NULL;

                // Try to enqueue garbage. If enqueue fails, we'll just try again during the
                // next call.
                if (snapshotStickies[i].head != NULL) {
                        if (!config.ccChannels[i]->Enqueue(snapshotStickies[i])) {
                                *(curStickies[i].tail) = snapshotStickies[i].head;
                                curStickies[i].tail = snapshotStickies[i].tail;
                                curStickies[i].count += snapshotStickies[i].count;
                        }
                        else {
                                //        std::cout << "Recycle!\n";
                        }
                }
                snapshotStickies[i] = curStickies[i];
                curStickies[i].head = NULL;
                curStickies[i].tail = &curStickies[i].head;
                curStickies[i].count = 0;
        }
}

void GarbageBin::FinishEpoch(uint32_t epoch) 
{
        barrier();
        uint32_t lowWatermark = *config.lowWaterMarkPtr;
        barrier();
  
        if (lowWatermark >= snapshotEpoch) {
                ReturnGarbage();
                snapshotEpoch = epoch;
                //    std::cout << "Success: " << epoch << "\n";
        }
}

RecordAllocator::RecordAllocator(size_t recordSize, uint32_t numRecords, 
                                 int cpu) 
{
        char *data = 
                (char*)alloc_mem(numRecords*(sizeof(Record)+recordSize), cpu);
        memset(data, 0x00, numRecords*(sizeof(Record)+recordSize));
        for (uint32_t i = 0; i < numRecords; ++i) {
                ((Record*)(data + i*(sizeof(Record)+recordSize)))->next = 
                        (Record*)(data + (i+1)*(sizeof(Record)+recordSize));
        }
        ((Record*)(data + (numRecords-1)*(sizeof(Record)+recordSize)))->next = NULL;
        //  data[numRecords-1].next = NULL;
        freeList = (Record*)data;
}

bool RecordAllocator::GetRecord(Record **OUT_REC) 
{
        if (freeList != NULL) {
                Record *temp = freeList;
                freeList = freeList->next;
                temp->next = NULL;
                *OUT_REC = temp;    
                return true;
        }
        else {
                *OUT_REC = NULL;
                return false;
        }
}

void RecordAllocator::FreeSingle(Record *rec) 
{
        rec->next = freeList;
        freeList = rec;
}

void RecordAllocator::Recycle(RecordList recList) 
{
        *(recList.tail) = freeList;
        freeList = recList.head;
}
