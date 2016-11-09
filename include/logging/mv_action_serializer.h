#ifndef _LOGGING_MV_ACTION_SERIALIZER_H
#define _LOGGING_MV_ACTION_SERIALIZER_H

#include "logging/buffer.h"
#include "mv_action.h"

class MVActionSerializer {
public:
    void serialize(const mv_action *action, Buffer* buffer);
};

#endif
