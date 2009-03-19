#include <Python.h>
#include <structmember.h>
#include <fdb/ptr.h>
#include <fdb/cons.h>
#include <fdb/fddb.h>
#include <fdb/pools.h>
#include <fdb/indices.h>
#include <fdb/eval.h>

typedef struct FD_PYTHON_WRAPPER {
  PyObject_HEAD /* Header stuff */
  fdtype lispval;} pylisp;

typedef struct FD_PYTHON_POOL {
  PyObject_HEAD /* Header stuff */
  fd_pool pool;} pypool;

typedef struct FD_PYTHON_INDEX {
  PyObject_HEAD /* Header stuff */
  fd_index index;} pyindex;

typedef struct FD_PYTHON_OBJECT {
  FD_CONS_HEADER; /* Header stuff */
  PyObject *pyval;} pyobject;
static fd_ptr_type python_object_type;
static fd_lispenv default_env;

static fdtype py2lisp(PyObject *o);
static fdtype py2lispx(PyObject *o);
static PyObject *lisp2py(fdtype x);

staticforward PyTypeObject ChoiceType;
staticforward PyTypeObject FramerDType;
static PyObject *FramerDError;

/* FramerD to Python errors */

static PyObject *framerd_error;

static PyObject *pass_error()
{
  PyObject *err;
  u8_condition ex; u8_context cxt; u8_string details; fdtype irritant;
  U8_OUTPUT out;
  fd_poperr(&ex,&cxt,&details,&irritant);
  U8_INIT_OUTPUT(&out,64);
  if (cxt)
    u8_printf(&out,"%m(%s):",ex,cxt);
  u8_printf(&out,"%m:",ex);
  if (details) u8_printf(&out," (%s) ",details);
  if (!(FD_VOIDP(irritant))) u8_printf(&out,"// %q",irritant);
  PyErr_SetString(framerd_error,(char *)out.u8_outbuf);
  u8_free(details); fd_decref(irritant);
  u8_free(out.u8_outbuf);
  return (PyObject *)NULL;
}

/*****************************************************************************
 * BASIC TYPE-OPERATIONS
 *****************************************************************************/

static pylisp *             /* on "x = stacktype.Stack()" */
newpylisp()                 /* instance constructor function */    
{                                /* these don't get an 'args' input */
  pylisp *self;
  self = PyObject_New(pylisp, &FramerDType);  /* malloc, init, incref */
  if (self == NULL)
    return NULL;            /* raise exception */
  self->lispval=FD_VOID;
  return self;                /* a new type-instance object */
}

static pylisp *             /* on "x = stacktype.Stack()" */
newpychoice()                 /* instance constructor function */    
{                                /* these don't get an 'args' input */
  pylisp *self;
  self = PyObject_New(pylisp, &ChoiceType);  /* malloc, init, incref */
  if (self == NULL)
    return NULL;            /* raise exception */
  self->lispval=FD_VOID;
  return self;                /* a new type-instance object */
}

static void                     /* instance destructor function */
pylisp_dealloc(self)             /* when reference-count reaches zero */
    pylisp *self;
{                               /* do cleanup activity */
  fd_decref(self->lispval);
  PyObject_Del(self);            /* same as 'free(self)' */
}

static int
pylisp_compare(v, w)
     pylisp *v, *w;
{
  int i, test;              /* compare objects and return -1, 0 or 1 */
  return (FDTYPE_COMPARE(v->lispval,w->lispval));
}

static void recycle_python_object(struct FD_CONS *obj)
{
  struct FD_PYTHON_OBJECT *po=(struct FD_PYTHON_OBJECT *)obj;
  Py_DECREF(po->pyval); u8_free(po);
}

static int unparse_python_object(u8_output out,fdtype obj)
{
  struct FD_PYTHON_OBJECT *pyo=(struct FD_PYTHON_OBJECT *)obj;
  PyObject *as_string=PyObject_Unicode(pyo->pyval);
  PyObject *u8=PyUnicode_AsEncodedString(as_string,"utf8","none");
  u8_printf(out,"#<PYTHON %s>",PyString_AS_STRING(u8));
  Py_DECREF(as_string); Py_DECREF(u8);
  return 1;
}

/* Printed representation */

#define PyUTF8String(s,len) PyUnicode_DecodeUTF8((char *)(s),len,"none")

static PyObject *pylisp_str(PyObject *self)
{
  struct FD_PYTHON_WRAPPER *pw=(struct FD_PYTHON_WRAPPER *)self;
  struct U8_OUTPUT out; u8_byte buf[64];
  PyObject *pystring; 
  U8_INIT_OUTPUT_X(&out,32,buf,U8_STREAM_GROWS);
  u8_printf(&out,"framerd.ref('%q')",pw->lispval);
  pystring=PyUTF8String(out.u8_outbuf,out.u8_outptr-out.u8_outbuf);
  if (out.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(out.u8_outbuf);
  return pystring;
}

static PyObject *choice_str(PyObject *self)
{
  struct FD_PYTHON_WRAPPER *pw=(struct FD_PYTHON_WRAPPER *)self;
  struct U8_OUTPUT out; u8_byte buf[64];
  PyObject *pystring; int i=0;
  U8_INIT_OUTPUT_X(&out,32,buf,U8_STREAM_GROWS);
  u8_puts(&out,"framerd.Choice(");
  {FD_DO_CHOICES(elt,pw->lispval) {
      if (i>0) u8_putc(&out,','); i++;
      if (FD_STRINGP(elt)) {
	int c=FD_STRDATA(elt)[0];
	u8_putc(&out,'\'');
	if (strchr("@(#",c))  u8_putc(&out,'\\');
	u8_puts(&out,FD_STRDATA(elt));
	u8_putc(&out,'\'');}
      else if (FD_SYMBOLP(elt))
	u8_printf(&out,"':%s'",FD_SYMBOL_NAME(elt));
      else u8_printf(&out,"'%q'",elt);}}
  u8_putc(&out,')');
  pystring=PyUTF8String(out.u8_outbuf,out.u8_outptr-out.u8_outbuf);
  if (out.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(out.u8_outbuf);
  return pystring;
}

static PyObject *pylisp_get(PyObject *self,PyObject *arg)
{
  struct FD_PYTHON_WRAPPER *pw=(struct FD_PYTHON_WRAPPER *)self;
  fdtype frames=py2lisp(self), slotids=py2lispx(arg), value=FD_EMPTY_CHOICE;
  if ((FD_CHOICEP(frames)) && (FD_FIXNUMP(slotids))) {
    PyObject *elt=lisp2py((FD_CHOICE_DATA(frames))[FD_FIX2INT(slotids)]);
    fd_decref(frames);
    return elt;}
  else {
    PyObject *result;
    FD_DO_CHOICES(frame,frames) {
      FD_DO_CHOICES(slotid,slotids) {
	fdtype v;
	if (FD_OIDP(frame)) v=fd_frame_get(frame,slotid);
	else v=fd_get(frame,slotid,FD_EMPTY_CHOICE);
	FD_ADD_TO_CHOICE(value,v);}}
    result=lisp2py(value);
    fd_decref(frames);
    fd_decref(slotids);
    fd_decref(value);
    return result;}
}

static int pylisp_set(PyObject *self,PyObject *arg,PyObject *val)
{
  struct FD_PYTHON_WRAPPER *pw=(struct FD_PYTHON_WRAPPER *)self;
  fdtype frames=py2lisp(self), slotids=py2lisp(arg), values=py2lisp(val);
  FD_DO_CHOICES(frame,frames) {
    FD_DO_CHOICES(slotid,slotids) {
      fd_store(frame,slotid,values);}}
  fd_decref(frames); fd_decref(slotids); fd_decref(values);
  return 1;
}

static int
pylisp_print(pw, fp, flags)
     struct FD_PYTHON_WRAPPER *pw;
     FILE *fp;
     int flags;                      /* print self to file */
{                                   /* or repr or str */
  char *exprstring=(char *)fd_dtype2string(pw->lispval); 
  fprintf(fp,"framerd.ref('%s')",exprstring);
  u8_free(exprstring);
  return 0;                       /* return status, not object */
}

static int choice_length(pw)
     struct FD_PYTHON_WRAPPER *pw;
{                                   /* or repr or str */
  return FD_CHOICE_SIZE(pw->lispval);
}

static PyObject *choice_merge(pw1,pw2)
     PyObject *pw1, *pw2;
{                                   /* or repr or str */
  fdtype choices[2]; PyObject *result;
  choices[0]=py2lisp(pw1); choices[1]=py2lisp(pw2);
  fdtype combined=fd_union(choices,2);
  result=lisp2py(combined);
  fd_decref(combined); fd_decref(choices[0]); fd_decref(choices[1]);
  return result;
}

static PyObject *choice_item(struct FD_PYTHON_WRAPPER *pw,int i)
{                                   /* or repr or str */
  return lisp2py((FD_CHOICE_DATA(pw->lispval))[i]);
}

static PyMappingMethods table_methods = {  /* mapping type supplement */
        (lenfunc)       NULL,         /* mp_length        'len(x)'  */
        (binaryfunc)    pylisp_get,      /* mp_subscript     'x[k]'    */
        (objobjargproc) pylisp_set,        /* mp_ass_subscript 'x[k] = v'*/
};

static PySequenceMethods choice_methods = {  /* sequence supplement     */
  (lenfunc)      choice_length,    /* sq_length    "len(x)"   */
  (binaryfunc)   choice_merge,     /* sq_concat    "x + y"    */
  (ssizeargfunc) NULL,             /* sq_repeat    "x * n"    */
  (ssizeargfunc) choice_item,      /* sq_item      "x[i], in" */
  (ssizessizeargfunc) NULL,             /* sq_slice     "x[i:j]"   */
  (ssizeobjargproc) NULL,              /* sq_ass_item  "x[i] = v" */
  (ssizessizeobjargproc) NULL,              /* sq_ass_slice  "x[i] = v" */
#if Py_TPFLAGS_HAVE_SEQUENCE_IN
  (objobjproc)    NULL,              /* sq_contains  "x[i] == v" */
#endif
#if Py_TPFLAGS_HAVE_INPLACEOPS
  (binaryfunc)    NULL,              /* sq_inplace_concat */
  (ssizeargfunc)  NULL               /* sq_inplace_repeat */
#endif
};

static PyTypeObject FramerDType = {      /* main python type-descriptor */
  /* type header */                    /* shared by all instances */
  PyObject_HEAD_INIT(&PyType_Type)         
  0,                               /* ob_size */
  "framerd",                         /* tp_name */
  sizeof(struct FD_PYTHON_WRAPPER),/* tp_basicsize */
  0,                               /* tp_itemsize */
  
  /* standard methods */
  (destructor)  pylisp_dealloc,   /* tp_dealloc  ref-count==0  */
  (printfunc)   NULL,     /* tp_print    "print x"     */
  (getattrfunc) NULL,   /* tp_getattr  "x.attr"      */
  (setattrfunc) NULL,               /* tp_setattr  "x.attr=v"    */
  (cmpfunc)     pylisp_compare,   /* tp_compare  "x > y"       */
  (reprfunc)    pylisp_str,               /* tp_repr     `x`, print x  */
  
  /* type categories */
  0,                             /* tp_as_number   +,-,*,/,%,&,>>,pow...*/
  0,            /* tp_as_sequence +,[i],[i:j],len, ...*/
  &table_methods, /* tp_as_mapping  [key], len, ...*/
  
  /* more methods */
  (hashfunc)     0,              /* tp_hash    "dict[x]" */
  (ternaryfunc)  0,              /* tp_call    "x()"     */
  (reprfunc)     0,              /* tp_str     "str(x)"  */
  
};  /* plus others: see Include/object.h */

static PyTypeObject ChoiceType = {      /* main python type-descriptor */
  /* type header */                    /* shared by all instances */
  PyObject_HEAD_INIT(&PyType_Type)         
  0,                               /* ob_size */
  "choice",                         /* tp_name */
  sizeof(struct FD_PYTHON_WRAPPER),/* tp_basicsize */
  0,                               /* tp_itemsize */
  
  /* standard methods */
  (destructor)  pylisp_dealloc,   /* tp_dealloc  ref-count==0  */
  (printfunc)   NULL,     /* tp_print    "print x"     */
  (getattrfunc) NULL,   /* tp_getattr  "x.attr"      */
  (setattrfunc) NULL,               /* tp_setattr  "x.attr=v"    */
  (cmpfunc)     pylisp_compare,   /* tp_compare  "x > y"       */
  (reprfunc)    choice_str,               /* tp_repr     `x`, print x  */
  
  /* type categories */
  0,                            /* tp_as_number   +,-,*,/,%,&,>>,pow...*/
  &choice_methods,              /* tp_as_sequence +,[i],[i:j],len, ...*/
  &table_methods,               /* tp_as_mapping  [key], len, ...*/
  
  /* more methods */
  (hashfunc)     0,              /* tp_hash    "dict[x]" */
  (ternaryfunc)  0,              /* tp_call    "x()"     */
  (reprfunc)     0,              /* tp_str     "str(x)"  */
  
};  /* plus others: see Include/object.h */

staticforward PyTypeObject PoolType;

/*****************************************************************************
 * BASIC TYPE-OPERATIONS
 *****************************************************************************/

static pypool *             /* on "x = stacktype.Stack()" */
newpool()                 /* instance constructor function */    
{                                /* these don't get an 'args' input */
  struct FD_PYTHON_POOL *self;
  self = PyObject_New(pypool, &PoolType);  /* malloc, init, incref */
  if (self == NULL)
    return NULL;            /* raise exception */
  self->pool=NULL;
  return self;                /* a new type-instance object */
}

static void                     /* instance destructor function */
pool_dealloc(self)             /* when reference-count reaches zero */
    struct FD_PYTHON_POOL *self;
{                               /* do cleanup activity */
  PyObject_Del(self);            /* same as 'free(self)' */
}

static int
pool_compare(v, w)
     struct FD_PYTHON_POOL *v, *w;
{
  int i, test;              /* compare objects and return -1, 0 or 1 */
  if (v->pool<w->pool) return -1;
  else if (v->pool == w->pool) return 0;
  else return 1;
}

static PyObject *pool_str(PyObject *self)
{
  struct FD_PYTHON_POOL *pw=(struct FD_PYTHON_POOL *)self;
  return PyString_FromFormat("framerd.pool('%s')",pw->pool->cid);
}

static PyTypeObject PoolType = {      /* main python type-descriptor */
  /* type header */                    /* shared by all instances */
      PyObject_HEAD_INIT(&PyType_Type)         
      0,                               /* ob_size */
      "pool",                         /* tp_name */
      sizeof(struct FD_PYTHON_POOL),/* tp_basicsize */
      0,                               /* tp_itemsize */

  /* standard methods */
      (destructor)  pool_dealloc,   /* tp_dealloc  ref-count==0  */
      (printfunc)   NULL,     /* tp_print    "print x"     */
      (getattrfunc) NULL,   /* tp_getattr  "x.attr"      */
      (setattrfunc) NULL,               /* tp_setattr  "x.attr=v"    */
      (cmpfunc)     pool_compare,   /* tp_compare  "x > y"       */
      (reprfunc)    pool_str,               /* tp_repr     `x`, print x  */

  /* type categories */
      0,                             /* tp_as_number   +,-,*,/,%,&,>>,pow...*/
      0,            /* tp_as_sequence +,[i],[i:j],len, ...*/
      0,                             /* tp_as_mapping  [key], len, ...*/

      /* more methods */
      (hashfunc)     0,              /* tp_hash    "dict[x]" */
      (ternaryfunc)  0,              /* tp_call    "x()"     */
      (reprfunc)     0,              /* tp_str     "str(x)"  */
      
};  /* plus others: see Include/object.h */

staticforward PyTypeObject IndexType;

/*****************************************************************************
 * BASIC TYPE-OPERATIONS
 *****************************************************************************/

static pyindex *             /* on "x = stacktype.Stack()" */
newindex()                 /* instance constructor function */    
{                                /* these don't get an 'args' input */
  struct FD_PYTHON_INDEX *self;
  self = PyObject_New(pyindex, &IndexType);  /* malloc, init, incref */
  if (self == NULL)
    return NULL;            /* raise exception */
  self->index=NULL;
  return self;                /* a new type-instance object */
}

static void                     /* instance destructor function */
index_dealloc(self)             /* when reference-count reaches zero */
    struct FD_PYTHON_INDEX *self;
{                               /* do cleanup activity */
  PyObject_Del(self);            /* same as 'free(self)' */
}

static int
index_compare(v, w)
     struct FD_PYTHON_INDEX *v, *w;
{
  int i, test;              /* compare objects and return -1, 0 or 1 */
  if (v->index<w->index) return -1;
  else if (v->index == w->index) return 0;
  else return 1;
}

static PyObject *index_str(PyObject *self)
{
  struct FD_PYTHON_INDEX *pw=(struct FD_PYTHON_INDEX *)self;
  return PyString_FromFormat("framerd.index('%s')",pw->index->cid);
}

static PyTypeObject IndexType = {      /* main python type-descriptor */
  /* type header */                    /* shared by all instances */
      PyObject_HEAD_INIT(&PyType_Type)         
      0,                               /* ob_size */
      "index",                         /* tp_name */
      sizeof(struct FD_PYTHON_INDEX),/* tp_basicsize */
      0,                               /* tp_itemsize */

  /* standard methods */
      (destructor)  index_dealloc,   /* tp_dealloc  ref-count==0  */
      (printfunc)   NULL,     /* tp_print    "print x"     */
      (getattrfunc) NULL,   /* tp_getattr  "x.attr"      */
      (setattrfunc) NULL,               /* tp_setattr  "x.attr=v"    */
      (cmpfunc)     index_compare,   /* tp_compare  "x > y"       */
      (reprfunc)    index_str,               /* tp_repr     `x`, print x  */

  /* type categories */
      0,                             /* tp_as_number   +,-,*,/,%,&,>>,pow...*/
      0,            /* tp_as_sequence +,[i],[i:j],len, ...*/
      0,                             /* tp_as_mapping  [key], len, ...*/

      /* more methods */
      (hashfunc)     0,              /* tp_hash    "dict[x]" */
      (ternaryfunc)  0,              /* tp_call    "x()"     */
      (reprfunc)     0,              /* tp_str     "str(x)"  */
      
};  /* plus others: see Include/object.h */

static PyObject *pylisp_ref(PyObject *self,PyObject *arg)
{
  if (PyString_Check(arg)) {
    fdtype obj;
    PyObject *u8=PyString_AsEncodedObject(arg,"utf8","none");
    if (u8) obj=fd_parse((u8_string)(PyString_AS_STRING(u8)));
    else return pass_error();
    Py_DECREF(u8);
    return lisp2py(obj);}
  else return NULL;
}

static PyObject *usepool(PyObject *self,PyObject *arg)
{
  fd_pool p; char *poolid;
  if (PyString_Check(arg)) {
    PyObject *u8=PyString_AsEncodedObject(arg,"utf8","none");
    if (u8) p=fd_use_pool((u8_string)PyString_AS_STRING(u8));
    else return pass_error();
    Py_DECREF(u8);}
  else return NULL;
  if (p) {
    struct FD_PYTHON_POOL *pw=(struct FD_PYTHON_POOL *)newpool();
    pw->pool=p;
    return (PyObject *) pw;}
  else return pass_error();
}

static PyObject *openindex(PyObject *self,PyObject *arg)
{
  fd_index ix; char *indexid;
  if (PyString_Check(arg)) {
    PyObject *u8=PyString_AsEncodedObject(arg,"utf8","none");
    if (u8) ix=fd_open_index((u8_string)PyString_AS_STRING(u8));
    else return pass_error();
    Py_DECREF(u8);}
  else return NULL;
  if (ix) {
    struct FD_PYTHON_INDEX *pw=(struct FD_PYTHON_INDEX *)newindex();
    pw->index=ix;
    return (PyObject *) pw;}
  else return pass_error();
}

static PyObject *useindex(PyObject *self,PyObject *arg)
{
  fd_index ix; char *indexid;
  if (PyString_Check(arg)) {
    PyObject *u8=PyString_AsEncodedObject(arg,"utf8","none");
    if (u8) ix=fd_use_index((u8_string)PyString_AS_STRING(u8));
    else return pass_error();
    Py_DECREF(u8);}
  else return NULL;
  if (ix) {
    struct FD_PYTHON_INDEX *pw=(struct FD_PYTHON_INDEX *)newindex();
    pw->index=ix;
    return (PyObject *) pw;}
  else return pass_error();
}

static PyObject *setcachelevel(PyObject *self,PyObject *arg)
{
  if (PyInt_Check(arg)) {
    long old_level=fd_default_cache_level;
    long val=PyInt_AS_LONG(arg);
    fd_default_cache_level=val;
    return PyInt_FromLong(old_level);}
  else return NULL;
}

static PyObject *lispget(PyObject *self,PyObject *args)
{
  fd_index ix; char *indexid;
  if ((PyTuple_Check(args)) && (PyTuple_GET_SIZE(args)==2)) {
    PyObject *arg0=PyTuple_GET_ITEM(args,0);
    PyObject *arg1=PyTuple_GET_ITEM(args,1), *result;
    fdtype frames=py2lispx(arg0), slotids=py2lispx(arg1);
    fdtype results=FD_EMPTY_CHOICE;
    Py_BEGIN_ALLOW_THREADS {
      FD_DO_CHOICES(f,frames) {
	FD_DO_CHOICES(slotid,slotids) {
	  fdtype v=fd_frame_get(f,slotid);
	  FD_ADD_TO_CHOICE(results,v);}}}
    Py_END_ALLOW_THREADS;
    result=lisp2py(results);
    fd_decref(frames); fd_decref(slotids); fd_decref(results);
    return result;}
  else return NULL;
}

static PyObject *lispfind(PyObject *self,PyObject *args)
{
  PyObject *result;
  fd_index ix; char *indexid;
  if (PyTuple_Check(args)) {
    int i=0, size=PyTuple_GET_SIZE(args), sv_start=0, sv_size=size;
    fdtype *slotvals, results, indices=FD_VOID;
    if (size%2) {
      PyObject *arg0=PyTuple_GET_ITEM(args,0);
      indices=py2lispx(arg0);
      sv_size=size-1; sv_start=1;}
    slotvals=u8_alloc_n(sv_size,fdtype);
    i=0; while (i<sv_size) {
      PyObject *arg0=PyTuple_GET_ITEM(args,sv_start+i);
      PyObject *arg1=PyTuple_GET_ITEM(args,sv_start+i+1);
      fdtype slotids=py2lispx(arg0), values=py2lisp(arg1);
      slotvals[i++]=slotids; slotvals[i++]=values;}
    Py_BEGIN_ALLOW_THREADS {
      if (FD_VOIDP(indices))
	results=fd_bgfinder(sv_size,slotvals);
      else results=fd_finder(indices,sv_size,slotvals);
      i=0; while (i<sv_size) {fdtype v=slotvals[i++]; fd_decref(v);}
      u8_free(slotvals);}
    Py_END_ALLOW_THREADS;
    result=lisp2py(results);
    fd_decref(results);
    return result;}
  else return NULL;
}

static PyObject *lispcall(PyObject *self,PyObject *args)
{
  PyObject *pyresult;
  if (PyTuple_Check(args)) {
    int i=0, size=PyTuple_GET_SIZE(args), n_args=size-1;
    fdtype fn, *argv, result;
    if (size==0) return NULL;
    fn=py2lisp(PyTuple_GET_ITEM(args,0));
    argv=u8_alloc_n(size-1,fdtype);
    while (i<n_args) {
      PyObject *arg=PyTuple_GET_ITEM(args,i+1);
      argv[i]=py2lisp(arg); i++;}
    Py_BEGIN_ALLOW_THREADS {
      result=fd_apply(fn,size-1,argv);}
    Py_END_ALLOW_THREADS;
    pyresult=lisp2py(result);
    i=0; while (i<n_args) { fdtype v=argv[i++]; fd_decref(v);}
    u8_free(argv);
    return pyresult;}
  else return NULL;
}

static PyObject *lispeval(PyObject *self,PyObject *pyexpr)
{
  fdtype expr=py2lispx(pyexpr), value;
  PyObject *pyvalue;
  Py_BEGIN_ALLOW_THREADS {
    value=fd_eval(expr,default_env);}
  Py_END_ALLOW_THREADS;
  pyvalue=lisp2py(value);
  fd_decref(expr); fd_decref(value);
  return pyvalue;
}

static PyObject *lispfn(PyObject *self,PyObject *args)
{
  if ((PyTuple_Check(args)) && (PyTuple_GET_SIZE(args)==2)) {
    fdtype modname=py2lispx(PyTuple_GET_ITEM(args,0));
    fdtype fname=py2lispx(PyTuple_GET_ITEM(args,1));
    fdtype module=fd_find_module(modname,0,0);
    fdtype fn=fd_get(module,fname,FD_VOID);
    PyObject *result=((FD_VOIDP(fn)) ? (NULL) : (lisp2py(fn)));
    fd_decref(modname); fd_decref(fname); fd_decref(module); fd_decref(fn);
    return result;}
  else return lispeval(self,args);
}

static struct PyMethodDef framerd_methods[]=
  {{"ref",pylisp_ref},
   {"pool",usepool},
   {"usepool",usepool},
   {"useindex",useindex},
   {"index",openindex},
   {"openindex",openindex},
   {"setcachelevel",setcachelevel},
   {"get",lispget},
   {"find",lispfind},
   {"call",lispcall},
   {"eval",lispeval},
   {"method",lispfn},
   {NULL,NULL}};

/* Python/LISP mapping */

static fdtype py2lisp(PyObject *o)
{
  if (PyInt_Check(o))
    return FD_INT2DTYPE(PyInt_AS_LONG(o));
  else if (PyFloat_Check(o))
    return fd_init_double(NULL,PyFloat_AsDouble(o));
  else if (PyString_Check(o)) {
    PyObject *u8=PyString_AsEncodedObject(o,"utf8","none");
    return fdtype_string((u8_string)PyString_AS_STRING(u8));}
  else if (PyUnicode_Check(o)) {
    PyObject *u8=PyUnicode_AsEncodedString(o,"utf8","none");
    fdtype v=fdtype_string((u8_string)PyString_AS_STRING(u8));
    Py_DECREF(u8);
    return v;}
  else if ((PyObject_TypeCheck(o,&PoolType))) {
    struct FD_PYTHON_POOL *pp=(struct FD_PYTHON_POOL *)o;
    return fd_pool2lisp(pp->pool);}
  else if ((PyObject_TypeCheck(o,&IndexType))) {
    struct FD_PYTHON_INDEX *pi=(struct FD_PYTHON_INDEX *)o;
    return fd_index2lisp(pi->index);}
  else if ((PyObject_TypeCheck(o,&FramerDType)) ||
	   (PyObject_TypeCheck(o,&ChoiceType))) {
    pylisp *v=(struct FD_PYTHON_WRAPPER *)o;
    return fd_incref(v->lispval);}
  else {
    struct FD_PYTHON_OBJECT *pyo=u8_alloc(struct FD_PYTHON_OBJECT);
    FD_INIT_CONS(pyo,python_object_type);
    pyo->pyval=o; Py_INCREF(o);
    return (fdtype) pyo;}
}

/* This parses string args and is used for calling functions through
   Python. */
static fdtype py2lispx(PyObject *o)
{
  if (PyInt_Check(o))
    return FD_INT2DTYPE(PyInt_AS_LONG(o));
  else if (PyFloat_Check(o))
    return fd_init_double(NULL,PyFloat_AsDouble(o));
  else if (PyString_Check(o)) {
    PyObject *u8=PyString_AsEncodedObject(o,"utf8","none");
    u8_string sdata=(u8_string)PyString_AS_STRING(u8);
    fdtype v=((*sdata==':') ? (fd_parse(sdata+1)) : (fd_parse(sdata)));
    Py_DECREF(u8);
    return v;}
  else if (PyUnicode_Check(o)) {
    PyObject *u8=PyUnicode_AsEncodedString(o,"utf8","none");
    fdtype v=fd_parse((u8_string)PyString_AS_STRING(u8));
    Py_DECREF(u8);
    return v;}
  else if ((PyObject_TypeCheck(o,&PoolType))) {
    struct FD_PYTHON_POOL *pp=(struct FD_PYTHON_POOL *)o;
    return fd_pool2lisp(pp->pool);}
  else if ((PyObject_TypeCheck(o,&IndexType))) {
    struct FD_PYTHON_INDEX *pi=(struct FD_PYTHON_INDEX *)o;
    return fd_index2lisp(pi->index);}
  else if ((PyObject_TypeCheck(o,&FramerDType)) ||
	   (PyObject_TypeCheck(o,&ChoiceType))) {
    pylisp *v=(struct FD_PYTHON_WRAPPER *)o;
    return fd_incref(v->lispval);}
  else return FD_VOID;
}

static PyObject *lisp2py(fdtype o)
{
  if (FD_FIXNUMP(o))
    return PyInt_FromLong(FD_FIX2INT(o));
  else if (FD_STRINGP(o))
    return PyUnicode_DecodeUTF8
      ((char *)FD_STRING_DATA(o),FD_STRING_LENGTH(o),"none");
  else if (FD_ACHOICEP(o)) {
    pylisp *po=newpychoice();
    po->lispval=fd_simplify_choice(o);
    return (PyObject *)po;}
  else if (FD_CHOICEP(o)) {
    pylisp *po=newpychoice();
    po->lispval=fd_incref(o);
    return (PyObject *)po;}
  else if (FD_ABORTP(o)) 
    return pass_error();
  else {
    pylisp *po=newpylisp();
    po->lispval=fd_incref(o);
    return (PyObject *)po;}
}

/* Primitives for embedded Python */

static fdtype pyerr(u8_context cxt)
{
  PyObject *err=PyErr_Occurred();
  if (err) {
    PyErr_Clear();
    return py2lisp(err);}
  else return fd_err("Mysterious Python error",cxt,NULL,FD_VOID);
}

static fdtype pyexec(fdtype pystring)
{
  int result;
  PyGILState_STATE gstate;
  gstate=PyGILState_Ensure();
  result=PyRun_SimpleString(FD_STRDATA(pystring));
  PyGILState_Release(gstate);
  return FD_INT2DTYPE(result);
}

static fdtype pyapply(fdtype fcn,int n,fdtype *args)
{
  if (FD_PRIM_TYPEP(fcn,python_object_type)) {
    struct FD_PYTHON_OBJECT *pyo=(struct FD_PYTHON_OBJECT *)fcn;
    if (PyCallable_Check(pyo->pyval)) {
      fdtype result; PyObject *tuple=PyTuple_New(n), *pyresult;
      PyGILState_STATE gstate=PyGILState_Ensure();
      int i=0;
      while (i<n) {
	PyObject *elt=lisp2py(args[i]);
	if (elt) {PyTuple_SetItem(tuple,i,elt);}
	else {
	  Py_DECREF(tuple);
	  PyGILState_Release(gstate);
	  return pyerr("pyapply");}
	i++;}
      pyresult=PyObject_CallObject(pyo->pyval,tuple);
      Py_DECREF(tuple);
      result=py2lisp(pyresult);
      Py_DECREF(pyresult);
      PyGILState_Release(gstate);
      return result;}
    else return fd_type_error("python procedure","pyapply",fcn);}
  else return fd_type_error("python procedure","pyapply",fcn);
}

static fdtype pycall(int n,fdtype *args)
{
  return pyapply(args[0],n-1,args+1);
}

static fdtype pyfn(fdtype modname,fdtype fname)
{
  PyObject *pmodulename=lisp2py(modname);
  PyObject *pmodule=PyImport_Import(pmodulename);
  if (pmodule) {
    PyObject *pFunc=PyObject_GetAttrString(pmodule,FD_STRDATA(fname));
    if (pFunc) {
      fdtype wrapped=py2lisp(pFunc);
      Py_DECREF(pFunc); Py_DECREF(pmodule);
      return wrapped;}
    Py_DECREF(pmodule);
    return pyerr("pyfn");}
  else return pyerr("pyfn");
}

static fdtype pymodule=FD_VOID;
static int python_init_done=0;

static void initframerdmodule()
{
  if (!(FD_VOIDP(pymodule))) return;
#if (!(HAVE_CONSTRUCTOR_ATTRIBUTES))
  u8_initialize_u8stdio();
  u8_init_chardata_c();
#endif
  
  /* Initialize builtin scheme modules.
     These include all modules specified by (e.g.):
     configure --enable-fdweb */
#if ((HAVE_CONSTRUCTOR_ATTRIBUTES) && (!(FD_TESTCONFIG)))
  FD_INIT_SCHEME_BUILTINS();
#else
  /* If we're a "test" executable (FD_TESTCONFIG), we're
     statically linked, so we need to initialize some modules
     explicitly (since the "onload" initializers may not be invoked). */
  fd_init_schemeio();
  fd_init_texttools();
  fd_init_tagger();
  fd_init_fdweb();
#endif
  pymodule=fd_new_module("PARZELTOUNGE",0);
  python_object_type=fd_register_cons_type("python");
  default_env=fd_working_environment();
  fd_recyclers[python_object_type]=recycle_python_object;
  fd_unparsers[python_object_type]=unparse_python_object;  
  
  fd_applyfns[python_object_type]=pyapply;

  fd_defn(pymodule,fd_make_cprim1x("PYEXEC",pyexec,1,fd_string_type,FD_VOID));
  fd_defn(pymodule,fd_make_cprimn("PYCALL",pycall,1));
  fd_defn(pymodule,fd_make_cprim2x("PYMETHOD",pyfn,2,
				   fd_string_type,FD_VOID,
				   fd_string_type,FD_VOID));
  fd_finish_module(pymodule);

}

static void initpythonmodule()
{
  PyObject *m, *d;
  if (python_init_done) return;
  m=Py_InitModule("parzeltounge",framerd_methods);
  framerd_error=PyErr_NewException("parzeltounge.error",NULL,NULL);
  Py_INCREF(framerd_error);
  PyModule_AddObject(m,"error",framerd_error);
  python_init_done=1;
}

void fd_initialize_parzeltounge(void) FD_LIBINIT_FN;

void fd_initialize_parzeltounge() 
{
  if (!(Py_IsInitialized())) Py_Initialize();
  initframerdmodule();
  initpythonmodule();
}

void initparzeltounge(void)
{
  fd_initialize_parzeltounge();
}

