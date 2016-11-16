#include <ycsb.h>
#include <cassert>
#include <string.h>

ycsb_insert::ycsb_insert(uint64_t start, uint64_t end)
{
        // TODO serialize random state.
        assert(start < end);
        this->start = start;
        this->end = end;
}

void ycsb_insert::gen_rand(char *array)
{
        uint32_t num_words, i, *int_array;
        
        num_words = YCSB_RECORD_SIZE / sizeof(uint32_t);
        int_array = (uint32_t*)array;
        for (i = 0; i < num_words; ++i) 
                int_array[i] = (uint32_t)txn_rand();
}

bool ycsb_insert::Run()
{
        uint64_t i;
        char rand_array[YCSB_RECORD_SIZE], *record_ptr;

        for (i = this->start; i < this->end; ++i) {
                gen_rand(rand_array);
                record_ptr = (char*)get_write_ref(i, 0);
                memcpy(record_ptr, rand_array, YCSB_RECORD_SIZE);
        }
        return true;
}

uint32_t ycsb_insert::num_writes()
{
        return (uint32_t)(end - start);
}

void ycsb_insert::get_writes(struct big_key *array)
{
        uint64_t i;
        for (i = this->start; i < this->end; ++i) {
                array[i-this->start].key = i;
                array[i-this->start].table_id = 0;
        }
}

void ycsb_insert::serialize(IBuffer *buffer) {
        buffer->write(start);
        buffer->write(end);
}

ycsb_readonly::ycsb_readonly(vector<uint64_t> reads)
{
        uint32_t num_reads, i;
        
        num_reads = reads.size();
        for (i = 0; i < num_reads; ++i)
                this->reads.push_back(reads[i]);
}

bool ycsb_readonly::Run()
{
        return true;
}

uint32_t ycsb_readonly::num_reads()
{
        return this->reads.size();
}

void ycsb_readonly::get_reads(struct big_key *array)
{
        uint32_t i, num_reads;
        struct big_key k;

        k.table_id = 0;
        num_reads = this->reads.size();
        for (i = 0; i < num_reads; ++i) {
                k.key = this->reads[i];
                array[i] = k;
        }
        return;
}

ycsb_rmw::ycsb_rmw(vector<uint64_t> reads, vector<uint64_t> writes)
{
        uint32_t num_reads, num_writes, i;

        num_reads = reads.size();
        num_writes = writes.size();
        for (i = 0; i < num_reads; ++i) 
                this->reads.push_back(reads[i]);
        for (i = 0; i < num_writes; ++i) 
                this->writes.push_back(writes[i]);
}

uint32_t ycsb_rmw::num_reads()
{
        return this->reads.size();
}

uint32_t ycsb_rmw::num_rmws()
{
        return this->writes.size();
}

void ycsb_rmw::get_reads(struct big_key *array)
{
        uint32_t num_reads, i;
        struct big_key k;

        k.table_id = 0;
        num_reads = this->reads.size();
        for (i = 0; i < num_reads; ++i) {
                k.key = this->reads[i];
                array[i] = k;
        }
        return;
}

void ycsb_rmw::get_rmws(struct big_key *array)
{
        uint32_t num_rmws, i;
        struct big_key k;

        k.table_id = 0;
        num_rmws = this->writes.size();
        for (i = 0; i < num_rmws; ++i) {
                k.key = this->writes[i];
                array[i] = k;
        }
        return;
}

bool ycsb_rmw::Run()
{
        uint32_t i, j, num_reads, num_writes;
        uint64_t counter;
        char *field_ptr, *write_ptr;

        num_reads = this->reads.size();
        num_writes = this->writes.size();

        /* Accumulate each field of records in the readset into "counter". */
        counter = 0;
        for (i = 0; i < num_reads; ++i) {
                field_ptr = (char*)get_read_ref(reads[i], 0);
                for (j = 0; j < 10; ++j)
                        counter += *((uint64_t*)&field_ptr[j*100]);
        }

        /* Perform an RMW operation on each element of the writeset. */
        for (i = 0; i < num_writes; ++i) {
                write_ptr = (char*)get_write_ref(writes[i], 0);
                for (j = 0; j < 10; ++j)
                        *((uint64_t*)&write_ptr[j*100]) += j+1+counter;
        }
        return true;
}

void ycsb_rmw::serialize(IBuffer *buffer) {
        buffer->write(reads.size());
        buffer->write(reinterpret_cast<const unsigned char*>(reads.data()),
                      reads.size() * sizeof(decltype(*reads.begin())));

        buffer->write(writes.size());
        buffer->write(reinterpret_cast<const unsigned char*>(writes.data()),
                      writes.size() * sizeof(decltype(*writes.begin())));
}
