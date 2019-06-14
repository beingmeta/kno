The basic dynamic type reference in Framer is the **lispval** (typedef fdtype) which is a pointer-sized unsigned integer. Every lispval has a *manifest type* which is the lower two bits of the value, which indicate:
0. a memory pointer to a CONS
1. a small (ish) integer
2. an immediate value (of various kinds)
3. an OID object reference
Each of these values except for zero (0) is arbitrary. The use of zero for memory pointers enables the machine language pun where the pointer to a CONS (which is aligned on 4-byte boundaries) and its lispval are identical.

Immediate values are used to represent constants (like #t, #f, #eof, etc), unicode characters (like #\A or #\alpha), symbols (like 'FOO), or any other objects where there are a limited number of distinct values. An immediate value has a 7-bit type field and uses the remainder of its bits (23 or 55) to represent values within a type. In the Scheme implementation, for example, they are used for representing lexical references as a pair of environment and variable offsets and for optimized function references.

CONSes and immediate values have their own type structures and these are unified into an 8-bit typespace (fd_ptr_type) where 0x00-0x04 are the manifest types, 0x04-0x84 are the *immediate types* and 0x84-0xFF are *cons types*. The macro `FD_PTR_TYPE` returns the type of a lispval, and `FD_PRIM_TYPEP(x,type)` returns non-zero if the type of the lispval *x* is *type*. Libraries and applications can allocate new cons or immediate types by calling fd_register_cons_type and fd_register_immediate_type which returns a newly allocated typecode or -1 if the number of type codes is exhausted.

The name of a type is stored in fd_type_names[typecode] and typecodes are also used as offsets into *handler* arrays including fd_recyclers, fd_copiers, fd_unparsers, fd_applyfns, fd_tablefns, fd_seqfns, etc.

Most types also have related CPP macros, including predicates like FD_FIXNUMP, FD_STRINGP, FD_SYMBOLP, etc.

Framer uses a *reference-counting garbage collector* to manage memory in full awareness of the problems with full awareness of pointer structures. There are a few clever tricks and a few annoying kludges forced by this choice (you can decide which are which).

A CONS object is a block of memory whose first 4 bytes (the *conshead*) contain both type information and a reference count. The type information is in the lower 7 bits and the reference count is in the higher 25 bits.

When possible (which is almost always for modern architectures), the reference counting GC uses a *lockless* implementation where the conshead is an atomic value. The operations fd_incref and fd_decref increment and decrement the reference count for conses and are no-ops for non-CONS lispvals. The operation fd_refcount returns an object's reference count. In multi-threaded applications, this is not necessarily stable.

A reference count of zero (0x00) indicates that an object is *static* and excluded from memory management.

Possible new implementation idea: a reference count of 0x01 indicates that a value is *ephemeral* which means that it is (a) excluded from memory management and (b) needs to be copied to be stored; a reference count of 0x02 indicates that a cons has been freed, so the lowest valid refcount is 0x03.

FramerD uses poor man's class system in that the FD_CONS struct contains the *conshead* field and that all other cons types start with FD_CONS_HEADER (a CPP macro) to be consistent. The default definition of FD_CONS has conshead be a *const* value to allow for better optimizations; the FD_RAW_CONS struct drops the *const* qualifier and the FD_REF_CONS struct both drops the *const* qualifier and adds an *atomic_t* qualifier to allow it to be changed. If you're using FD_REF_CONS, you need to be very careful.
