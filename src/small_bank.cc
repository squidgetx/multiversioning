#include <small_bank.h>

SmallBank::LoadCustomerRange::LoadCustomerRange(uint64_t customer_start,
                                                uint64_t customer_end)
        : _customer_start(customer_start), _customer_end(customer_end)
{
        assert(customer_end > customer_start);        
        uint64_t i;
        long savings, checking;
       
        // TODO serialize rand() state.
        for (i = customer_start; i < customer_end; ++i) {
                savings = rand() % 100;
                checking = rand() % 100;
                balances.push_back(savings);
                balances.push_back(checking);
                customers.push_back(i);
        }
}

bool SmallBank::LoadCustomerRange::Run()
{
        long savings, checking;
        uint64_t i, num_customers, customer_id;
        SmallBankRecord *savings_rec, *checking_rec;
        
        num_customers = customers.size();
        for (i = 0; i < num_customers; ++i) {
                customer_id = this->customers[i];
                savings = this->balances[2*i];
                checking = this->balances[2*i+1];
                savings_rec =
                        (SmallBankRecord*)get_write_ref(customer_id, SAVINGS);
                checking_rec =
                        (SmallBankRecord*)get_write_ref(customer_id, CHECKING);
                savings_rec->amount = savings;
                checking_rec->amount = checking;
        }
        return true;
}

uint32_t SmallBank::LoadCustomerRange::num_writes()
{
        return 2*this->customers.size();
}

void SmallBank::LoadCustomerRange::get_writes(struct big_key *array)
{
        uint32_t num_customers, i;
        
        num_customers = this->customers.size();
        for (i = 0; i < num_customers; ++i) {
                array[2*i].key = this->customers[i];
                array[2*i].table_id = SAVINGS;
                array[2*i+1].key = this->customers[i];
                array[2*i+1].table_id = CHECKING;
        }
}

void SmallBank::LoadCustomerRange::serialize(IBuffer *buffer) {
        buffer->write(_customer_start);
        buffer->write(_customer_end);
}


SmallBank::Balance::Balance(uint64_t customer_id)
{
        this->totalBalance = 0;
        this->customer_id = customer_id;
}

bool SmallBank::Balance::Run()
{
        SmallBankRecord *checking =
                (SmallBankRecord*)get_read_ref(customer_id, CHECKING);
        SmallBankRecord *savings =
                (SmallBankRecord*)get_read_ref(customer_id, SAVINGS);
        this->totalBalance = checking->amount + savings->amount;
        do_spin();
        return true;        
}

uint32_t SmallBank::Balance::num_reads()
{
        return 2;
}

void SmallBank::Balance::get_reads(struct big_key *array)
{
        array[0].key = this->customer_id;
        array[0].table_id = CHECKING;
        array[1].key = this->customer_id;
        array[1].table_id = SAVINGS;
}

SmallBank::DepositChecking::DepositChecking(uint64_t customer, long amount)
{
        this->customer_id = customer;
        this->amount = amount;
}

bool SmallBank::DepositChecking::Run()
{
        SmallBankRecord *checking;

        checking = (SmallBankRecord*)get_write_ref(this->customer_id, CHECKING);
        checking->amount += this->amount;
        do_spin();
        return true;        
}

uint32_t SmallBank::DepositChecking::num_rmws()
{
        return 1;
}

void SmallBank::DepositChecking::get_rmws(struct big_key *array)
{
        array[0].key = this->customer_id;
        array[0].table_id = CHECKING;
}

void SmallBank::DepositChecking::serialize(IBuffer *buffer) {
        buffer->write(customer_id);
        buffer->write(amount);
}

SmallBank::TransactSaving::TransactSaving(uint64_t customer, long amount)
{
        this->amount = amount;
        this->customer_id = customer;
}

bool SmallBank::TransactSaving::Run()
{
        SmallBankRecord *savings;
        savings = (SmallBankRecord*)get_write_ref(customer_id, SAVINGS);
        savings->amount += this->amount;
        do_spin();
        return true;
}

uint32_t SmallBank::TransactSaving::num_rmws()
{
        return 1;
}

void SmallBank::TransactSaving::get_rmws(struct big_key *array)
{
        array[0].key = this->customer_id;
        array[0].table_id = SAVINGS;
}

void SmallBank::TransactSaving::serialize(IBuffer *buffer) {
        buffer->write(customer_id);
        buffer->write(amount);
}

SmallBank::Amalgamate::Amalgamate(uint64_t from_customer, uint64_t to_customer)
{
        this->from_customer = from_customer;
        this->to_customer = to_customer;
}

bool SmallBank::Amalgamate::Run()
{
        SmallBankRecord *from_checking, *from_savings, *to_checking;

        from_checking =
                (SmallBankRecord*)get_write_ref(this->from_customer, CHECKING);
        from_savings =
                (SmallBankRecord*)get_write_ref(this->from_customer, SAVINGS);
        to_checking =
                (SmallBankRecord*)get_write_ref(this->to_customer, CHECKING);
        to_checking->amount += from_checking->amount + from_savings->amount;
        from_checking->amount = 0;
        from_savings->amount = 0;
        do_spin();
        return true;
}

uint32_t SmallBank::Amalgamate::num_rmws()
{
        return 3;
}

void SmallBank::Amalgamate::get_rmws(struct big_key *array)
{
        array[0].key = this->from_customer;
        array[0].table_id = CHECKING;

        array[1].key = this->from_customer;
        array[1].table_id = SAVINGS;

        array[2].key = this->to_customer;
        array[2].table_id = CHECKING;
}

void SmallBank::Amalgamate::serialize(IBuffer *buffer) {
        buffer->write(from_customer);
        buffer->write(to_customer);
}

SmallBank::WriteCheck::WriteCheck(uint64_t customer_id, long amount)
{
        this->customer_id = customer_id;
        this->check_amount = amount;
}

bool SmallBank::WriteCheck::Run()
{
        SmallBankRecord *checking, *savings;

        checking = (SmallBankRecord*)get_write_ref(customer_id, CHECKING);
        savings = (SmallBankRecord*)get_read_ref(customer_id, SAVINGS);
        if (checking->amount + savings->amount - check_amount < 0)
                check_amount += 1;
        checking->amount -= check_amount;
        do_spin();
        return true;
}

uint32_t SmallBank::WriteCheck::num_reads()
{
        return 1;
}

uint32_t SmallBank::WriteCheck::num_rmws()
{
        return 1;
}

void SmallBank::WriteCheck::get_reads(struct big_key *array)
{
        array[0].key = this->customer_id;
        array[0].table_id = SAVINGS;
}

void SmallBank::WriteCheck::get_rmws(struct big_key *array)
{
        array[0].key = this->customer_id;
        array[0].table_id = CHECKING;
}

void SmallBank::WriteCheck::serialize(IBuffer *buffer) {
        buffer->write(customer_id);
        buffer->write(check_amount);
}
