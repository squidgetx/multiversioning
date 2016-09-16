#ifndef 	PIPELINED_EXECUTOR_H_
#define 	PIPELINED_EXECUTOR_H_

#include <runnable.hh>
#include <concurrent_queue.h>

class RecordBuffers;
class RecordBuffersConfig;
class insert_buf_mgr;
class mcs_mgr;
class LockManager;
class locking_key;
class locking_action;

namespace pipelined {

class action;

struct action_batch {
        uint32_t 	_batch_sz;
        action		**_txns;
};

struct dep_node {
        action		*_txn;
        dep_node	*_next;
};

struct txn_context {
        action 		*_txn;
        dep_node 	*_head;
        dep_node 	*_tail;
};

typedef SimpleQueue<action_batch> txn_queue;

typedef enum {
        READ,
        WRITE,
} dep_type;

struct executor_config {
        LockManager 		*_mgr;
        int 			_cpu;
        txn_queue 		*_input;
        txn_queue		*_output;
};

class dependency_table {
 private:
        uint32_t 		***_tbl;

 public:
        dependency_table();
        uint32_t get_dependent_piece(uint32_t dependent_type, 
                                     uint32_t dependency_type, 
                                     uint32_t piece_num);
};

class executor : public Runnable {

 private:
        txn_context 		_context;
        executor_config 	_conf;
        LockManager 		*_lck_mgr;
        dep_node 		*_depnode_list;
        dependency_table 	*_dep_tbl;
        RecordBuffers 		*_record_buffers;
        insert_buf_mgr 		*_insert_buf_mgr;
        mcs_mgr 		*_mcs_mgr;

 protected:
        virtual void StartWorking();
        virtual void exec_txn(action *txn);
        virtual void wait_deps(action *txn, uint32_t piece);
        virtual void add_dep(action *txn, uint32_t piece);
        virtual void get_deps(action *txn, uint32_t piece);

        virtual void add_prev_write(action *txn, locking_key *start);
        virtual void add_prev_reads(action *txn, locking_key *start);
        virtual void get_operation_deps(action *txn, locking_key *start);

        void add_single_dep(locking_key *to_add, locking_key **head);        
        
        dep_node* alloc_depnode();
        void return_depnodes(dep_node *head, dep_node *tail);
        void init_depnodes();

        void set_context(action *txn);
        void clear_context();
        void add_dep_context(action *txn);

        void prepare(action *txn);
        
        locking_action* get_dependent_piece(action *txn, action *dependent_piece,
                                            uint32_t piece);

 public:
        executor(executor_config conf, RecordBuffersConfig rb_conf);
};

};

#endif 		// PIPELINED_EXECUTOR_H_