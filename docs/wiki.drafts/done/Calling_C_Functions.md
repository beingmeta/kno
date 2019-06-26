Kno has a simple foreign function interface to access C functions from
either Kno itself or external libraries. The current implementation
only handles *scalar* types, including integers (various sizes),
floating point numbers (ditto), strings, KNO's native lisp pointers,
and opaque pointer values.

The procedure `FFI/PROC` creates an FFI procedure and has the form:
````scheme
`(ffi/proc *external_name* *library file (or #f)* *return_type* *arg1type* *arg2type* ...)`
````

For example, we can do:
````scheme
(define strlen (ffi/proc "strlen" #f 'size_t 'string))
#|kno>|# (strlen "foo")
3
(define unixtime (ffi/proc "time" #f time_t #[basetype ptr nullable #t]))
#|kno>|# (strlen "foo")
#T2019-06-12T15:41:56Z
(define unix-getenv (ffi/proc "getenv" #f 'strcpy 'string))
#|kno>|# (unix-getenv "USER")
"haase"
````

KNO's foreign function interface does not provide destructuring of
structured objects or pointers to structured objects, but it does
functions can return raw pointer values which are wrapped by KNO and
can be passed to other foreign functions.

## Type codes

The base type codes are symbols, including:
* `void` is a 'non value'; this only makes sense for return types
* `float` is a low precision floating point number
* `double` is a higher precision float point number
* `uint` is an unsigned integer
* `int` is an integer
* `ushort` is an unsigned short integer
* `short` is a short integer
* `ulong`  is an unsigned long integer
* `long` is a long integer
* `uchar` is an unsigned byte
* `byte` is an unsigned byte
* `ssize_t` is an integer ssize_t value
* `char` is a signed byte
* `string` is a utf8-string
* `packet` is a null terminated set of bytes
* `ptr` is some kind of pointer value
* `cons` is a raw KNO cons pointer
* `lisp` is a KNO lisp pointer, which has already been incref'd
* `lispref` is a KNO lisp pointer, which needs to be incref'd
* `time_t` is a unix time value
* `null` is any object but is always passed as a NULL pointer value

## Tagged results

KNO's FFI interface allows the annotation of values, including pointer
values, with **typetag*s. This can be used to have the foreign
function interface do type checking on arguments. For example,a simple
wrapper of the UNIX filename access could be:
````scheme
(define fopen
  (ffi/proc "fopen" #f #[basetype ptr typetag file] 'string 'string))
(define fclose
  (ffi/proc "fclose" #f 'int #[basetype ptr typetag file]))
(define fgetc
  (ffi/proc "fgetc" #f 'int #[basetype ptr typetag file]))
(define fputc
  (ffi/proc "fgetc" #f 'int 'int #[basetype ptr typetag file]))

````

In this example, `fopen` returns a *raw pointer* tagged with `file`
and the other functions will automatically check for this tagged value
when they are invoked.

## Light wrappers

For more complicated libraries, one approach for connecting with them
is to define a lightweight C wrapper which provides the basic API in
terms of the core FFI types and then linking to that wrapper using the
FFI interface.

