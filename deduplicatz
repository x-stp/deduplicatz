/* deduplicatz.c v1.0.3zv pipelined io_uring +
                                    O_DIRECT +
                                    xxHash3
                          uniq tool

@ Author : Pepijn van der Stap <oss@vanderstap[.]info>
@ License: AGPLv3 (probably on your *FILE somewhere)
@ Year   : 2025

Build:
£ gcc -O3 -march=native -std=gnu11 -Wall -o deduplicatz deduplicatz.c 
-lpthread -luring -lxxhash

Install dependencies:
£ apt    : sudo apt install liburing-dev libxxhash-dev
£ yum/dnf: sudo dnf install liburing-devel xxhash-devel
£ pacman : sudo pacman -S liburing xxhash

Usage: ./deduplicatz in.txt out.txt - can't beat that logic.

vi: set ts=4 sw=4 et nowrap: */

#define _GNU_SOURCE
#include <liburing.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/sysinfo.h>
#include <sys/uio.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdatomic.h>
#include <errno.h>
#include <time.h>
#include <xxhash.h>
#include <sys/utsname.h>

#define CHUNK_SIZE      65536
#define MAX_LINE_SIZE   16384
#define INIT_HT_SIZE    10000019UL
#define LOAD_FACTOR     0.85
#define PROBE_LIMIT     64
#define QUEUE_SIZE      256
#define IOV_BATCH       128
#define RING_DEPTH      32

static atomic_int   done_reading = 0;
static atomic_ulong total_seen   = 0;
static atomic_ulong total_unique = 0;
static int          out_fd;

// --- dynamic hashset ---
typedef struct {
    uint64_t*        table;
    size_t           size;
    atomic_ulong     count;
    pthread_rwlock_t lock;
} hashset_t;

static void hashset_init(hashset_t* hs, size_t sz) {
    hs->size = sz;
    hs->table = calloc(sz, sizeof(uint64_t));
    atomic_store(&hs->count, 0);
    pthread_rwlock_init(&hs->lock, NULL);
    if (!hs->table) {
        perror("calloc");
        exit(1);
    }
}

static void hashset_rehash(hashset_t* hs) {
    pthread_rwlock_wrlock(&hs->lock);
    size_t new_sz = hs->size * 2 + 1;
    while (1) {
        int p = 1;
        for (size_t d = 3; d * d <= new_sz; d += 2)
            if (new_sz % d == 0) {
                p = 0;
                break;
            }
        if (p) break;
        new_sz += 2;
    }
    uint64_t* new_tab = calloc(new_sz, sizeof(uint64_t));
    if (!new_tab) {
        perror("calloc rehash");
        exit(1);
    }
    for (size_t i = 0; i < hs->size; ++i) {
        uint64_t h = hs->table[i];
        if (!h) continue;
        size_t idx = h % new_sz;
        for (int j = 0; j < PROBE_LIMIT; ++j) {
            if (!new_tab[idx]) {
                new_tab[idx] = h;
                break;
            }
            idx = (idx + 1) % new_sz;
        }
    }
    free(hs->table);
    hs->table = new_tab;
    hs->size  = new_sz;
    pthread_rwlock_unlock(&hs->lock);
}

static int hashset_insert(hashset_t* hs, uint64_t h) {
    pthread_rwlock_rdlock(&hs->lock);
    size_t idx = h % hs->size;
    for (int j = 0; j < PROBE_LIMIT; ++j) {
        uint64_t exp = 0;
        if (__atomic_compare_exchange_n(&hs->table[idx], &exp, h, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
            size_t used = atomic_fetch_add(&hs->count, 1) + 1;
            pthread_rwlock_unlock(&hs->lock);
            if ((double)used / hs->size > LOAD_FACTOR)
                hashset_rehash(hs);
            return 1;
        }
        if (hs->table[idx] == h) {
            pthread_rwlock_unlock(&hs->lock);
            return 0;
        }
        idx = (idx + 1) % hs->size;
    }
    pthread_rwlock_unlock(&hs->lock);
    return 0;
}

static hashset_t hset;

// --- chunk queue ---
typedef struct { char* data; size_t sz; int last; } chunk_t;
typedef struct {
    chunk_t*        buf[QUEUE_SIZE];
    int             head, tail, count;
    pthread_mutex_t lock;
    pthread_cond_t  ne, nf;
} queue_t;

static queue_t queue;

static void queue_init(queue_t* q) {
    memset(q, 0, sizeof(*q));
    pthread_mutex_init(&q->lock, NULL);
    pthread_cond_init(&q->ne, NULL);
    pthread_cond_init(&q->nf, NULL);
}

static void queue_push(queue_t* q, chunk_t* c) {
    pthread_mutex_lock(&q->lock);
    while (q->count == QUEUE_SIZE)
        pthread_cond_wait(&q->nf, &q->lock);
    q->buf[q->tail] = c;
    q->tail = (q->tail + 1) % QUEUE_SIZE;
    q->count++;
    pthread_cond_signal(&q->ne);
    pthread_mutex_unlock(&q->lock);
}

static chunk_t* queue_pop(queue_t* q) {
    pthread_mutex_lock(&q->lock);
    while (q->count == 0) {
        if (atomic_load(&done_reading)) {
            pthread_mutex_unlock(&q->lock);
            return NULL;
        }
        pthread_cond_wait(&q->ne, &q->lock);
    }
    chunk_t* c = q->buf[q->head];
    q->head = (q->head + 1) % QUEUE_SIZE;
    q->count--;
    pthread_cond_signal(&q->nf);
    pthread_mutex_unlock(&q->lock);
    return c;
}

// --- worker ---
static void* worker_fn(void* _) {
    char* line = malloc(MAX_LINE_SIZE);
    struct iovec iovs[IOV_BATCH];
    int ic = 0;
    size_t llen = 0;
    while (1) {
        chunk_t* c = queue_pop(&queue);
        if (!c) break;
        for (size_t i = 0; i < c->sz; ++i) {
            char ch = c->data[i];
            if (llen < MAX_LINE_SIZE - 1)
                line[llen++] = ch;
            else {
                llen = 0;
                continue;
            }
            if (ch == '\n') {
                atomic_fetch_add(&total_seen, 1);
                uint64_t h = XXH3_64bits(line, llen);
                if (hashset_insert(&hset, h)) {
                    char* cp = malloc(llen);
                    memcpy(cp, line, llen);
                    iovs[ic].iov_base = cp;
                    iovs[ic].iov_len  = llen;
                    if (++ic == IOV_BATCH) {
                        writev(out_fd, iovs, ic);
                        for (int j = 0; j < ic; ++j)
                            free(iovs[j].iov_base);
                        ic = 0;
                    }
                    atomic_fetch_add(&total_unique, 1);
                }
                llen = 0;
            }
        }
        free(c);
    }
    if (ic > 0) {
        writev(out_fd, iovs, ic);
        for (int j = 0; j < ic; ++j)
            free(iovs[j].iov_base);
    }
    free(line);
    return NULL;
}

// --- progress ---

static void* progress_fn(void* _) {
    while (!atomic_load(&done_reading)) {
        fprintf(stderr, "\r[+] Unique: %lu | Seen: %lu",
            atomic_load(&total_unique), atomic_load(&total_seen));
        fflush(stderr);
        struct timespec ts = {0, 500000000};
        nanosleep(&ts, NULL);
    }
    fprintf(stderr, "\r[+] Unique: %lu | Seen: %lu (done)\n",
        atomic_load(&total_unique), atomic_load(&total_seen));
    return NULL;
}

int main(int argc, char* argv[]) {
    struct utsname u;
    uname(&u);
    if (strstr(u.sysname, "SunOS")) {
        fprintf(stderr, "[!] how did u even\n");
        return 1;
    }

    if (argc != 3) {
        fprintf(stderr, "Usage: %s in.txt out.txt\n", argv[0]);
        return 1;
    }
    int in_fd  = open(argv[1], O_RDONLY | O_DIRECT);
    out_fd     = open(argv[2], O_CREAT|O_WRONLY|O_TRUNC, 0644);
    if (in_fd < 0 || out_fd < 0) { perror("open"); return 1; }

    char* bufs[RING_DEPTH];
    for (int i = 0; i < RING_DEPTH; ++i)
        if (posix_memalign((void**)&bufs[i], 4096, CHUNK_SIZE))
            { perror("posix_memalign"); return 1; }

    struct io_uring ring;
    if (io_uring_queue_init(RING_DEPTH, &ring, 0) < 0)
        { perror("io_uring_queue_init"); return 1; }

    hashset_init(&hset, INIT_HT_SIZE);
    queue_init(&queue);

    int n = get_nprocs() - 1; if (n < 1) n = 1;
    pthread_t th[n], pr;
    for (int i = 0; i < n; ++i)
        pthread_create(&th[i], NULL, worker_fn, NULL);
    pthread_create(&pr, NULL, progress_fn, NULL);

    off_t offset = 0; int inflight = 0;
    for (int i = 0; i < RING_DEPTH; ++i) {
        struct io_uring_sqe* sq = io_uring_get_sqe(&ring);
        io_uring_prep_read(sq, in_fd, bufs[i], CHUNK_SIZE, offset);
        sq->user_data = (uintptr_t)i;
        io_uring_submit(&ring);
        offset += CHUNK_SIZE; inflight++;
    }

    while (inflight) {
        struct io_uring_cqe* cqe;
        int ret = io_uring_wait_cqe(&ring, &cqe);
        if (ret < 0 || !cqe) break;
        int idx = cqe->user_data;
        int res = cqe->res;
        io_uring_cqe_seen(&ring, cqe);
        inflight--;
        if (res > 0) {
            chunk_t* ch = malloc(sizeof(chunk_t));
            ch->data = bufs[idx];
            ch->sz   = res;
            ch->last = (res < CHUNK_SIZE);
            queue_push(&queue, ch);

            struct io_uring_sqe* sq = io_uring_get_sqe(&ring);
            io_uring_prep_read(sq, in_fd, bufs[idx], CHUNK_SIZE, offset);
            sq->user_data = (uintptr_t)idx;
            io_uring_submit(&ring);
            offset += CHUNK_SIZE; inflight++;
        }
    }

    atomic_store(&done_reading, 1);
    pthread_cond_broadcast(&queue.ne);
    for (int i = 0; i < n; ++i) pthread_join(th[i], NULL);
    pthread_join(pr, NULL);

    io_uring_queue_exit(&ring);
    close(in_fd); close(out_fd);
    free(hset.table);
    return 0;
}
