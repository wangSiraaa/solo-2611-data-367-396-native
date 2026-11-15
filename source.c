#include "arrow_c.h"
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
static atomic_int live=0,freed=0,streams=0;
struct Batch { uint8_t valid; int64_t values[5]; const void *ptrs[2]; };
struct Source { int next; };
static void schema_release(struct ArrowSchema *s){s->release=NULL;}
static int schema(struct ArrowArrayStream *s,struct ArrowSchema *o){(void)s;memset(o,0,sizeof(*o));o->format="l";o->release=schema_release;return 0;}
static void batch_release(struct ArrowArray *a){if(!a->release)return;free(a->private_data);a->private_data=NULL;a->release=NULL;atomic_fetch_sub(&live,1);atomic_fetch_add(&freed,1);}
static int next(struct ArrowArrayStream *s,struct ArrowArray *o){struct Source *x=s->private_data;memset(o,0,sizeof(*o));if(x->next==3)return 0;struct Batch *b=calloc(1,sizeof(*b));if(!b)return 12;b->valid=0x1b;for(int i=0;i<5;i++)b->values[i]=100*x->next+i;b->ptrs[0]=&b->valid;b->ptrs[1]=b->values;o->length=5;o->null_count=1;o->n_buffers=2;o->buffers=b->ptrs;o->release=batch_release;o->private_data=b;atomic_fetch_add(&live,1);x->next++;return 0;}
static const char* error(struct ArrowArrayStream *s){(void)s;return "allocation failed";}
static void stream_release(struct ArrowArrayStream *s){if(!s->release)return;free(s->private_data);s->private_data=NULL;s->release=NULL;atomic_fetch_add(&streams,1);}
void make_stream(struct ArrowArrayStream *s){memset(s,0,sizeof(*s));s->get_schema=schema;s->get_next=next;s->get_last_error=error;s->release=stream_release;s->private_data=calloc(1,sizeof(struct Source));}
int live_batches(void){return atomic_load(&live);} int released_batches(void){return atomic_load(&freed);} int released_streams(void){return atomic_load(&streams);}
