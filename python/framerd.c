#include <Python.h>
#include <structmember.h>
#include <eframerd/lisp.h>
#include <eframerd/fddb.h>
#include <eframerd/pools.h>
#include <eframerd/indices.h>

typedef struct FD_PYTHON_WRAPPER {
  PyObject_HEAD /* Header stuff */
  fd_lisp lispval;} pylisp;

typedef struct FD_PYTHON_POOL {
  PyObject_HEAD /* Header stuff */
  fd_pool pool;} pypool;

typedef struct FD_PYTHON_INDEX {
  PyObject_HEAD /* Header stuff */
  fd_index index;} pyindex;

static fd_lisp py2lisp(PyObject *o);
static fd_lisp py2lispx(PyObject *o);
static PyObject *lisp2py(fd_lisp x);

staticforward PyTypeObject ChoiceType;
staticforward PyTypeObject FramerDType;
static PyObject *FramerDError;

/* FramerD to Python errors */

static PyObject *framerd_error;

static PyObject *pass_error(fd_lisp exo)
{
  PyObject *err;
  fd_exception ex; u8_string details; fd_lisp irritant;
  U8_OUTPUT out;
  if (FD_VOIDP(exo))
    fd_poperr(&ex,&details,&irritant);
  else {
    struct FD_EXCEPTION_OBJECT *xo=
      FD_GET_CONS(exo,fd_exception_type,struct FD_EXCEPTION_OBJECT *);
    ex=xo->ex; details=xo->details; irritant=xo->irritant;}
  U8_INIT_OUTPUT(&out,64);
  if (FD_VOIDP(irritant))
    if (details)
      u8_printf(&out,"%m: (%m)",ex,details);
    else u8_printf(&out,"%m",ex);
  else if (details)
    u8_printf(&out,"%m: (%m) %q",ex,details,irritant);
  else u8_printf(&out,"%m: %q",ex,irritant);
  PyErr_SetString(framerd_error,out.bytes);
  fd_decref(irritant); fd_free(out.bytes);
  return (PyObject *)NULL;
}

/*****************************************************************************
 * BASIC TYPE-OPERATIONS
 *****************************************************************************/

static pylisp *             /* on "x = stacktype.Stack()" */
newpylisp()                 /* instance constructor function */    
{                                /* these don't get an 'args' input */
  pylisp *self;
  self = PyObject_NEW(pylisp, &FramerDType);  /* malloc, init, incref */
  if (self == NULL)
    return NULL;            /* raise exception */
  self->lispval=FD_VOID;
  return self;                /* a new type-instance object */
}

static pylisp *             /* on "x = stacktype.Stack()" */
newpychoice()                 /* instance constructor function */    
{                                /* these don't get an 'args' input */
  pylisp *self;
  self = PyObject_NEW(pylisp, &ChoiceType);  /* malloc, init, incref */
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
  PyMem_DEL(self);            /* same as 'free(self)' */
}

static int
pylisp_compare(v, w)
     pylisp *v, *w;
{
  int i, test;              /* compare objects and return -1, 0 or 1 */
  return (FD_LISP_COMPARE(v->lispval,w->lispval));
}

static PyObject *pylisp_str(PyObject *self)
{
  struct FD_PYTHON_WRAPPER *pw=(struct FD_PYTHON_WRAPPER *)self;
  char *exprstring=fd_lisp2string(pw->lispval); 
  return PyString_FromFormat("framerd.ref('%s')",exprstring);
}

static PyObject *choice_str(PyObject *self)
{
  struct FD_PYTHON_WRAPPER *pw=(struct FD_PYTHON_WRAPPER *)self;
  if ((FD_CHOICE_SIZE(pw->lispval))>8)
    return PyString_FromFormat("<%d results>",FD_CHOICE_SIZE(pw->lispval));
  else return pylisp_str(self);
}

static PyObject *pylisp_get(PyObject *self,PyObject *arg)
{
  struct FD_PYTHON_WRAPPER *pw=(struct FD_PYTHON_WRAPPER *)self;
  fd_lisp frames=py2lisp(self), slotids=py2lispx(arg), value=FD_EMPTY_CHOICE;
  PyObject *result;
  FD_DO_CHOICES(frame,frames) {
    FD_DO_CHOICES(slotid,slotids) {
      fd_lisp v;
      if (FD_OIDP(frame)) v=fd_frame_get(frame,slotid);
      else v=fd_get(frame,slotid,FD_EMPTY_CHOICE);
      FD_ADD_TO_CHOICE(value,v);}}
  result=lisp2py(value);
  fd_decref(value);
  return result;
}

static int pylisp_set(PyObject *self,PyObject *arg,PyObject *val)
{
  struct FD_PYTHON_WRAPPER *pw=(struct FD_PYTHON_WRAPPER *)self;
  fd_lisp frames=py2lisp(self), slotids=py2lisp(arg), values=py2lisp(val);
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
  char *exprstring=fd_lisp2string(pw->lispval); 
  fprintf(fp,"framerd.ref('%s')",exprstring);
  fd_free(exprstring);
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
  fd_lisp choices[2]; PyObject *result;
  choices[0]=py2lisp(pw1); choices[1]=py2lisp(pw2);
  fd_lisp combined=fd_union(choices,2);
  result=lisp2py(combined);
  fd_decref(combined); fd_decref(choices[0]); fd_decref(choices[1]);
  return result;
}

static PyObject *choice_item(struct FD_PYTHON_WRAPPER *pw,int i)
{                                   /* or repr or str */
  return lisp2py((FD_CHOICE_DATA(pw->lispval))[i]);
}

static PyMappingMethods table_methods = {  /* mapping type supplement */
        (inquiry)       NULL,         /* mp_length        'len(x)'  */
        (binaryfunc)    pylisp_get,      /* mp_subscript     'x[k]'    */
        (objobjargproc) pylisp_set,        /* mp_ass_subscript 'x[k] = v'*/
};

static PySequenceMethods choice_methods = {  /* sequence supplement     */
      (inquiry)       choice_length,    /* sq_length    "len(x)"   */
      (binaryfunc)    choice_merge,     /* sq_concat    "x + y"    */
      (intargfunc)    NULL,             /* sq_repeat    "x * n"    */
      (intargfunc)    choice_item,      /* sq_item      "x[i], in" */
      (intintargfunc) NULL,             /* sq_slice     "x[i:j]"   */
      (intobjargproc)     0,            /* sq_ass_item  "x[i] = v" */
      (intintobjargproc)  0,            /* sq_ass_slice "x[i:j]=v" */
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
  self = PyObject_NEW(pypool, &PoolType);  /* malloc, init, incref */
  if (self == NULL)
    return NULL;            /* raise exception */
  self->pool=NULL;
  return self;                /* a new type-instance object */
}

static void                     /* instance destructor function */
pool_dealloc(self)             /* when reference-count reaches zero */
    struct FD_PYTHON_POOL *self;
{                               /* do cleanup activity */
  PyMem_DEL(self);            /* same as 'free(self)' */
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
  self = PyObject_NEW(pyindex, &IndexType);  /* malloc, init, incref */
  if (self == NULL)
    return NULL;            /* raise exception */
  self->index=NULL;
  return self;                /* a new type-instance object */
}

static void                     /* instance destructor function */
index_dealloc(self)             /* when reference-count reaches zero */
    struct FD_PYTHON_INDEX *self;
{                               /* do cleanup activity */
  PyMem_DEL(self);            /* same as 'free(self)' */
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
    fd_lisp obj;
    PyObject *u8=PyString_AsEncodedObject(arg,"utf8","none");
    if (u8) obj=fd_parse(PyString_AS_STRING(u8));
    else return pass_error(FD_VOID);
    Py_DECREF(u8);
    return lisp2py(obj);}
  else return NULL;
}

static PyObject *usepool(PyObject *self,PyObject *arg)
{
  fd_pool p; char *poolid;
  if (PyString_Check(arg)) {
    PyObject *u8=PyString_AsEncodedObject(arg,"utf8","none");
    if (u8) p=fd_use_pool(PyString_AS_STRING(u8));
    else return pass_error(FD_VOID);
    Py_DECREF(u8);}
  else return NULL;
  if (p) {
    struct FD_PYTHON_POOL *pw=(struct FD_PYTHON_POOL *)newpool();
    pw->pool=p;
    return (PyObject *) pw;}
  else return pass_error(FD_VOID);
}

static PyObject *openindex(PyObject *self,PyObject *arg)
{
  fd_index ix; char *indexid;
  if (PyString_Check(arg)) {
    PyObject *u8=PyString_AsEncodedObject(arg,"utf8","none");
    if (u8) ix=fd_open_index(PyString_AS_STRING(u8));
    else return pass_error(FD_VOID);
    Py_DECREF(u8);}
  else return NULL;
  if (ix) {
    struct FD_PYTHON_INDEX *pw=(struct FD_PYTHON_INDEX *)newindex();
    pw->index=ix;
    return (PyObject *) pw;}
  else return pass_error(FD_VOID);
}

static PyObject *useindex(PyObject *self,PyObject *arg)
{
  fd_index ix; char *indexid;
  if (PyString_Check(arg)) {
    PyObject *u8=PyString_AsEncodedObject(arg,"utf8","none");
    if (u8) ix=fd_use_index(PyString_AS_STRING(u8));
    else return pass_error(FD_VOID);
    Py_DECREF(u8);}
  else return NULL;
  if (ix) {
    struct FD_PYTHON_INDEX *pw=(struct FD_PYTHON_INDEX *)newindex();
    pw->index=ix;
    return (PyObject *) pw;}
  else return pass_error(FD_VOID);
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
    fd_lisp frames=py2lispx(arg0), slotids=py2lispx(arg1);
    fd_lisp results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(f,frames) {
      FD_DO_CHOICES(slotid,slotids) {
	fd_lisp v=fd_frame_get(f,slotid);
	FD_ADD_TO_CHOICE(results,v);}}
    result=lisp2py(results);
    fd_decref(frames); fd_decref(slotids); fd_decref(results);
    return result;}
  else return NULL;
}

static PyObject *lispfind(PyObject *self,PyObject *args)
{
  fd_index ix; char *indexid;
  if ((PyTuple_Check(args)) && (PyTuple_GET_SIZE(args)==2)) {
    PyObject *arg0=PyTuple_GET_ITEM(args,0);
    PyObject *arg1=PyTuple_GET_ITEM(args,1), *result;
    fd_lisp slotids=py2lispx(arg0), values=py2lisp(arg1), results=FD_EMPTY_CHOICE;
    results=fd_bgfind(slotids,values,FD_VOID);
    result=lisp2py(results);
    fd_decref(values); fd_decref(slotids); fd_decref(results);
    return result;}
  else return NULL;
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
   {NULL,NULL}};

/* Python/LISP mapping */

static fd_lisp py2lisp(PyObject *o)
{
  if (PyInt_Check(o))
    return FD_LISPFIX(PyInt_AS_LONG(o));
  else if (PyFloat_Check(o))
    return fd_init_double(NULL,PyFloat_AsDouble(o));
  else if (PyString_Check(o)) {
    PyObject *u8=PyString_AsEncodedObject(o,"utf8","none");
    return fd_lisp_string(PyString_AS_STRING(u8));}
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

static fd_lisp py2lispx(PyObject *o)
{
  if (PyInt_Check(o))
    return FD_LISPFIX(PyInt_AS_LONG(o));
  else if (PyFloat_Check(o))
    return fd_init_double(NULL,PyFloat_AsDouble(o));
  else if (PyString_Check(o)) {
    PyObject *u8=PyString_AsEncodedObject(o,"utf8","none");
    fd_lisp v=fd_parse(PyString_AS_STRING(u8));
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

static PyObject *lisp2py(fd_lisp o)
{
  if (FD_FIXNUMP(o))
    return PyInt_FromLong(FD_FIXLISP(o));
  else if (FD_STRINGP(o))
    return PyString_Decode(FD_STRING_DATA(o),FD_STRING_LENGTH(o),"utf8","none");
  else if (FD_ACHOICEP(o)) {
    pylisp *po=newpychoice();
    po->lispval=fd_simplify_choice(o);
    return (PyObject *)po;}
  else if (FD_CHOICEP(o)) {
    pylisp *po=newpychoice();
    po->lispval=fd_incref(o);
    return (PyObject *)po;}
  else if (FD_EXCEPTIONP(o)) 
    return pass_error(o);
  else {
    pylisp *po=newpylisp();
    po->lispval=fd_incref(o);
    return (PyObject *)po;}
}

void initframerd(void)
{
  PyObject *m, *d;
  m=Py_InitModule("framerd",framerd_methods);
  framerd_error=PyErr_NewException("framerd.error",NULL,NULL);
  Py_INCREF(framerd_error);
  PyModule_AddObject(m,"error",framerd_error);
}

