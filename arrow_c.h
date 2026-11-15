#ifndef SOLO_ARROW_C_H
#define SOLO_ARROW_C_H
#include <stdint.h>
struct ArrowSchema { const char *format,*name,*metadata; int64_t flags,n_children; struct ArrowSchema **children,*dictionary; void (*release)(struct ArrowSchema*); void *private_data; };
struct ArrowArray { int64_t length,null_count,offset,n_buffers,n_children; const void **buffers; struct ArrowArray **children,*dictionary; void (*release)(struct ArrowArray*); void *private_data; };
struct ArrowArrayStream { int (*get_schema)(struct ArrowArrayStream*,struct ArrowSchema*); int (*get_next)(struct ArrowArrayStream*,struct ArrowArray*); const char* (*get_last_error)(struct ArrowArrayStream*); void (*release)(struct ArrowArrayStream*); void *private_data; };
void make_stream(struct ArrowArrayStream*);
int live_batches(void); int released_batches(void); int released_streams(void);
#endif
