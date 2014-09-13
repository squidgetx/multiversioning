#include <database.h>

Database::Database() {
	catalog = new Catalog();
}

bool Database::PutTable(uint32_t tableId, MVTable *in) {
	return catalog->PutTable(tableId, in);
}

bool Database::GetTable(uint32_t tableId, MVTable **out) {
	return catalog->GetTable(tableId, out);
}