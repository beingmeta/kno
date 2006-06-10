directory /home/sources/framerd/src/cons
directory /home/sources/framerd/src/os
directory /home/sources/framerd/src/odb
directory /home/sources/framerd/src/index
directory /home/sources/framerd/src/eval
directory /home/sources/framerd/src/scheme
directory /home/sources/framerd/src/fdscript
directory /home/sources/framerd/src/text
directory /home/sources/framerd/src/exe
set env LD_LIBRARY_PATH /home/sources/framerd/lib
set env DYLD_LIBRARY_PATH /home/sources/framerd/lib
set env ALWAYS_BUSY yes
set env PROMISCUOUS_FDSERVLET yes
set env MALLOC_CHECK_=2
set env EF_PROTECT_BELOW=1
set env EF_PROTECT_FREE=2
set env EF_FILL=255
break fd_raise_exception
break fd_raise_detailed_exception
break fd_raise_lisp_exception
define printp
  print *($arg0)
end
define lc
  echo type\ 
  print ($arg0)->type
  echo data\  
  print ($arg0)->data.fixnum
end
define lp
  echo type\ 
  print ($arg0)->type
  echo ptr\ \ 
  print ($arg0)->data.any
  echo nref\ 
  print ($arg0)->data.acons->n_refs
end
define lstring
  print ($arg0)->data.string->data
end
define lsym
  print ($arg0)->data.symbol->name
end
define lisp
  call fd_print_lisp_to_stdout($arg0)
end
define lptr
  print *(($arg0)->data.$arg1)
end

