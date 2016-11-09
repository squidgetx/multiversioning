#include "logging/mv_action_serializer.h"

#include <cassert>
#include "logging/buffer.h"
#include "mv_action.h"

void MVActionSerializer::serialize(const mv_action *action, Buffer* buffer) {
    // Write Txn Type.
    buffer->write(static_cast<uint32_t>(action->t->type()));

    BufferReservation reservation{buffer->reserve(sizeof(uint64_t))};
    CountedBuffer txnBuffer(buffer);

    action->t->serialize(&txnBuffer);
    reservation.write(static_cast<uint64_t>(txnBuffer.getCount()));
    assert(reservation.remaining() == 0);
}
