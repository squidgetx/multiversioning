#ifndef _NO_COPY_H
#define _NO_COPY_H

#define DISALLOW_COPY(TypeName) \
    TypeName(const TypeName&) = delete; \
    void operator=(const TypeName&) = delete;

#endif /* _NO_COPY_H */
