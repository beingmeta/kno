/* Design for new exception handling system
   Switch from mostly return value error handling to
     a catch/throw (setjmp/longjmp) model.

   libu8:
    Create a dynamic context which has a jump buf, an optional
       label, a popfn, and a pointer to the next context.
    Use a thread var to store the current dynamic context with
     operations for pushing and popping the dynamic context
    External modules can extend the dynamic context structure.
    The libu8 structure might also have queues for malloc'd blocks,
     locked mutexes, and maybe some generic cleanup functions.
    
    Exception state consists of an exception, a details string, 
     a void pointer, and a saved exception stack.  Largely compatible
     with the current model.  There might be an extra int indicating
     some type information about the void pointer with new ints being
     allocated as needed.

    Macros for exception catching and unwind protect as well as a very
     simple set of U8_EXCALLn macros which do function calls in a
     new dynamic context.

    fdkbase:

    Uses dynamic contexts for error handling and handling automatic
     deallocation.

    Extend the libu8 dynamic context with (at least) extensible vectors for:
       malloc'd blocks,
       incref'd pointers
       vectors of incref'd pointers
       fcn.ptr pairs
    might also have a start timestamp.  Have an fd_alloc which
     is like u8_alloc but adds the pointer to the dynamic context.
     Have an fd_need_decref operation which pushes an incref'd pointer
     Have an fd_need_vdecref which takes a fdtype* and a length

   Primitive fd_dapply's are all wrapped in dynamic contexts
   Calls to expression evaluation are also wrapped in dynamic contexts

   Do we convert thrown errors to error objects?  When do we do so?
   Only in error handling forms?

   Additional thoughts:
    In FD:TOI, dynamic contexts were stack allocated, which could really
     mess things up if we missed a pop.  Alternatively, we could keep a
     static vector of contexts (in a thread var) and allocate from there.
     This would let us not lose so badly when we skip a pop and would also
     give us stack overvflow checking of a sort.

*/
