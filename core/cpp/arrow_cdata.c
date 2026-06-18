/*
 * arrow_cdata.c - Native (C) calculator kernels that read Apache Arrow arrays
 * zero-copy via the Arrow C Data Interface.
 *
 * This is the "polyglot at native speed" handoff (roadmap A1): Python exports a
 * pyarrow Array into the two standard ABI structs below (ArrowSchema/ArrowArray)
 * with NO copy and NO serialization, and this code reads the column buffers in
 * place. The ABI is deliberately tiny and has zero dependency on the Arrow C++
 * library, so the exact same structs are what a C++, Rust, or Java/JNI consumer
 * would use - this C kernel stands in for any of them.
 *
 * Spec: https://arrow.apache.org/docs/format/CDataInterface.html
 */
#include <stdint.h>
#include <string.h>

struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};

/* Return codes */
#define DY_OK 0
#define DY_UNSUPPORTED_TYPE 1
#define DY_LENGTH_MISMATCH 2
#define DY_NULL_INPUT 3

/*
 * Affine transform: out[i] = in[i] * scale + offset, written as float64.
 * Reads the input array's data buffer (buffers[1]) in place - zero copy.
 * Supports Arrow primitive formats: int32 "i", int64 "l", float32 "f",
 * float64 "g". Nulls are treated as their raw stored value (callers that need
 * null semantics should mask afterwards).
 */
int dy_affine_to_f64(uintptr_t array_ptr, uintptr_t schema_ptr,
                     double scale, double offset,
                     double* out, int64_t out_len) {
    struct ArrowArray* a = (struct ArrowArray*)array_ptr;
    struct ArrowSchema* s = (struct ArrowSchema*)schema_ptr;
    if (!a || !s || !out) return DY_NULL_INPUT;
    if (a->length != out_len) return DY_LENGTH_MISMATCH;
    if (a->n_buffers < 2 || a->buffers[1] == 0) return DY_NULL_INPUT;

    const int64_t n = a->length;
    const int64_t off = a->offset;
    const char* fmt = s->format;
    if (!fmt || fmt[1] != '\0') return DY_UNSUPPORTED_TYPE;

    switch (fmt[0]) {
        case 'l': {  /* int64 */
            const int64_t* d = (const int64_t*)a->buffers[1];
            for (int64_t i = 0; i < n; i++) out[i] = (double)d[off + i] * scale + offset;
            return DY_OK;
        }
        case 'i': {  /* int32 */
            const int32_t* d = (const int32_t*)a->buffers[1];
            for (int64_t i = 0; i < n; i++) out[i] = (double)d[off + i] * scale + offset;
            return DY_OK;
        }
        case 'g': {  /* float64 */
            const double* d = (const double*)a->buffers[1];
            for (int64_t i = 0; i < n; i++) out[i] = d[off + i] * scale + offset;
            return DY_OK;
        }
        case 'f': {  /* float32 */
            const float* d = (const float*)a->buffers[1];
            for (int64_t i = 0; i < n; i++) out[i] = (double)d[off + i] * scale + offset;
            return DY_OK;
        }
        default:
            return DY_UNSUPPORTED_TYPE;
    }
}

/* Zero-copy sum of an int64 array (used by tests to prove in-place reads). */
long long dy_sum_i64(uintptr_t array_ptr) {
    struct ArrowArray* a = (struct ArrowArray*)array_ptr;
    if (!a || a->n_buffers < 2 || a->buffers[1] == 0) return 0;
    const int64_t* d = (const int64_t*)a->buffers[1];
    long long total = 0;
    for (int64_t i = 0; i < a->length; i++) total += d[a->offset + i];
    return total;
}

/* ABI probe so Python can confirm the loaded library is ours. */
int dy_abi_version(void) { return 1; }
