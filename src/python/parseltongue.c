/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
   This file is part of beingmeta's KNO platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include <Python.h>
#include <structmember.h>

#ifdef HAVE_FSTAT
#undef HAVE_FSTAT
#endif

#include <kno/ptr.h>
#include <kno/cons.h>
#include <kno/storage.h>
#include <kno/pools.h>
#include <kno/indexes.h>
#include <kno/eval.h>
#include <kno/numbers.h>
#include <kno/bigints.h>
#include <kno/webtools.h>
#include <kno/cprims.h>

#include <libu8/libu8.h>
#include <libu8/u8strings.h>
#include <libu8/u8streamio.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

/* Compatibility */

#if PY_MAJOR_VERSION >= 3
#define PyText_Check(x) PyUnicode_Check(x)
#define PyString_Check(x) (0)
#define PyString_FromFormat PyUnicode_FromFormat
#define PyInt_FromLong PyLong_FromLong
#define PyInt PyLong
#define PyInt_Check PyLong_Check
#define PyInt_AS_LONG PyLong_AS_LONG
#define staticforward static
static u8_string pytext2utf8(PyObject *o,PyObject **tmpobj)
{
  PyObject *utf8 = PyUnicode_AsUTF8String(o);
  *tmpobj = utf8;
  if (utf8)
    return PyBytes_AS_STRING(utf8);
  else return NULL;
}
static PyObject *do_rich_compare(int cmp,int op)
{
  int rv = -1;
  switch (op) {
  case Py_LT: rv = (cmp<0); break;
  case Py_LE: rv = (cmp<=0); break;
  case Py_EQ: rv = (cmp==0); break;
  case Py_NE: rv = (cmp!=0); break;
  case Py_GT: rv = (cmp>0); break;
  case Py_GE: rv = (cmp>=0); break;}
  if (rv < 0)
    return Py_NotImplemented;
  else if (rv)
    return Py_True;
  else return Py_False;
}
#else
#define PyText_Check(x) PyUnicode_Check(x)
static u8_string pytext2utf8(PyObject *o,PyObject **tmpobj)
{
  PyObject *utf8 = PyString_AsEncodedObject(o,"utf8","none");
  *tmpobj = utf8;
  if (utf8)
    return PyString_AS_STRING(utf8);
  else return NULL;
}
#endif

void kno_init_parseltongue(void) KNO_LIBINIT_FN;

typedef struct KNO_PYTHON_WRAPPER {
  PyObject_HEAD /* Header stuff */
  lispval lval;} pylisp;

typedef struct KNO_PYTHON_POOL {
  PyObject_HEAD /* Header stuff */
  kno_pool pool;} pypool;

typedef struct KNO_PYTHON_INDEX {
  PyObject_HEAD /* Header stuff */
  kno_index index;} pyindex;

typedef struct KNO_PYTHON_FUNCTION {
  PyObject_HEAD /* Header stuff */
  lispval fnval;} pyfunction;

typedef struct KNO_PYTHON_OBJECT {
  KNO_CONS_HEADER; /* Header stuff */
  PyObject *pyval;} KNO_PYTHON_OBJECT;
typedef struct KNO_PYTHON_OBJECT *kno_python_object;

#define PYTHON_OBJECT_TYPE 0x27a830ULL
static kno_lisp_type python_object_type;
static kno_lexenv default_env;

static lispval py2lisp(PyObject *o);
static lispval py2lispx(PyObject *o);
static PyObject *lisp2py(lispval x);
static u8_string py2string(PyObject *o);

staticforward PyTypeObject ChoiceType;
staticforward PyTypeObject OIDType;
staticforward PyTypeObject KnoType;
staticforward PyTypeObject IndexType;
staticforward PyTypeObject PoolType;
staticforward PyTypeObject ApplicableType;

typedef void (*bigint_consumer)(void *,int);
typedef unsigned int (*bigint_producer)(void *);

static int negate_bigint(unsigned char *bytes,int len)
{
  int i=0, carry=0; while (i<len) {
    bytes[i]=~(bytes[i]); i++;}
  if (bytes[0]==255) {bytes[0]=0; carry=1;}
  else bytes[0]=bytes[0]+1;
  i=1; if (carry) while (i<len) {
      int sum=bytes[i]+carry;
      if (sum==256) {bytes[i]=0; carry=1;}
      else {bytes[i]=(unsigned char)sum; carry=0;}
      i++;}
  bytes[i]=carry;
  return len+carry;
}

/* Kno to Python errors */

static PyObject *kno_error;

static PyObject *pass_error()
{
  u8_condition ex; u8_context cxt; u8_string details; lispval irritant;
  U8_OUTPUT out;
  kno_poperr(&ex,&cxt,&details,&irritant);
  U8_INIT_OUTPUT(&out,64);
  if (cxt)
    u8_printf(&out,"%m(%s):",ex,cxt);
  u8_printf(&out,"%m:",ex);
  if (details) u8_printf(&out," (%s) ",details);
  if (!(KNO_VOIDP(irritant))) u8_printf(&out,"// %q",irritant);
  PyErr_SetString(kno_error,(char *)out.u8_outbuf);
  u8_free(details);
  kno_decref(irritant);
  u8_free(out.u8_outbuf);
  return (PyObject *)NULL;
}

static lispval translate_python_error(u8_context cxt)
{
  if (PyErr_Occurred()) {
    PyObject *type, *value, *stack;
    PyErr_Fetch(&type,&value,&stack);
    PyErr_Clear();
    if (cxt == NULL) cxt = "py2lisp";
    lispval etype = py2lisp(type), evalue = py2lisp(value);
    lispval estack = (stack) ? (py2lisp(stack)) : (KNO_EMPTY_LIST);
    lispval evec = (stack) ?
      (kno_make_nvector(3,etype,evalue,estack)) :
      (kno_make_nvector(2,etype,evalue));
    u8_string details = py2string(type);
    kno_seterr("PythonError",cxt,details,evec);
    if (details) u8_free(details);
    kno_decref(evec);
    if (type) Py_DECREF(type);
    if (value) Py_DECREF(value);
    if (stack) Py_DECREF(stack);
    return KNO_ERROR_VALUE;}
  else return KNO_VOID;
}

/*****************************************************************************
 * BASIC TYPE-OPERATIONS
 *****************************************************************************/

static pylisp *             /* on "x = stacktype.Stack()" */
newpylisp()                 /* instance constructor function */
{                                /* these don't get an 'args' input */
  pylisp *self;
  self = PyObject_New(pylisp, &KnoType);  /* malloc, init, incref */
  if (self == NULL)
    return NULL;            /* raise exception */
  self->lval=KNO_VOID;
  return self;                /* a new type-instance object */
}

static pylisp *             /* on "x = stacktype.Stack()" */
newpychoice()                 /* instance constructor function */
{                                /* these don't get an 'args' input */
  pylisp *self;
  self = PyObject_New(pylisp, &ChoiceType);  /* malloc, init, incref */
  if (self == NULL)
    return NULL;            /* raise exception */
  self->lval=KNO_VOID;
  return self;                /* a new type-instance object */
}

static pylisp *             /* on "x = stacktype.Stack()" */
newpyoid()                 /* instance constructor function */
{                                /* these don't get an 'args' input */
  pylisp *self;
  self = PyObject_New(pylisp, &OIDType);  /* malloc, init, incref */
  if (self == NULL)
    return NULL;            /* raise exception */
  self->lval=KNO_VOID;
  return self;                /* a new type-instance object */
}

static pylisp *             /* on "x = stacktype.Stack()" */
newpyapply(lispval fn)                 /* instance constructor function */
{                                /* these don't get an 'args' input */
  pylisp *self;
  self = PyObject_New(pylisp, &ApplicableType);  /* malloc, init, incref */
  if (self == NULL)
    return NULL;            /* raise exception */
  self->lval=fn;
  kno_incref(fn);
  return self;                /* a new type-instance object */
}

static void                     /* instance destructor function */
pylisp_dealloc(self)             /* when reference-count reaches zero */
    pylisp *self;
{                               /* do cleanup activity */
  kno_decref(self->lval);
  PyObject_Del(self);            /* same as 'free(self)' */
}

#if PY_MAJOR_VERSION >= 3
static PyObject *pylisp_compare(PyObject *v_arg,PyObject *w_arg,int op)
{
  if ( (PyObject_TypeCheck(v_arg,&KnoType)) &&
       (PyObject_TypeCheck(w_arg,&KnoType)) ) {
    pylisp *v = (pylisp *) v_arg;
    pylisp *w = (pylisp *) w_arg;
    int cmp = KNO_FULL_COMPARE(v->lval,w->lval);
    return do_rich_compare(cmp,op);}
  else return Py_NotImplemented;
}
#else
static int pylisp_compare(pylisp *v,pylisp *w)
{
  return (KNO_FULL_COMPARE(v->lval,w->lval));
}
#endif

static PyObject *
pylisp_apply(PyObject *self,PyObject *args,PyObject *kwargs)
{
  PyObject *pyresult;
  if (PyTuple_Check(args)) {
    lispval fn = py2lisp(self), result = KNO_VOID;
    if (! (KNO_APPLICABLEP(fn)) ) {
      u8_string s = kno_lisp2string(fn);
      PyErr_Format(PyExc_TypeError,"Not applicable: %s",s);
      u8_free(s);
      kno_decref(fn);
      return NULL;}
    int i=0, size=PyTuple_GET_SIZE(args);
    if (size == 0)
      result = kno_apply(fn,0,NULL);
    else {
      lispval argv[size];
      while (i<size) {
	PyObject *arg=PyTuple_GET_ITEM(args,i);
	argv[i]=py2lisp(arg);
	i++;}
      Py_BEGIN_ALLOW_THREADS {
	result=kno_apply(fn,size,argv);}
      Py_END_ALLOW_THREADS;
      kno_decref_elts(argv,size);}
    pyresult=lisp2py(result);
    return pyresult;}
  else return NULL;
}

static void recycle_python_object(struct KNO_RAW_CONS *obj)
{
  struct KNO_PYTHON_OBJECT *po=(struct KNO_PYTHON_OBJECT *)obj;
  Py_DECREF(po->pyval); u8_free(po);
}

#if PY_MAJOR_VERSION >= 3
static int unparse_python_object(u8_output out,lispval obj)
{
  struct KNO_PYTHON_OBJECT *pyo = (struct KNO_PYTHON_OBJECT *)obj;
  PyObject *pyval = pyo->pyval;
  PyObject *as_string = PyObject_Repr(pyo->pyval);
  if (as_string == NULL) {
    u8_printf(out,"#<PYTHON Weird #x%llx>",
	      (unsigned long long)((kno_ptrval)pyo->pyval));
    return 1;}
  PyObject *utf8 = PyUnicode_AsUTF8String(as_string);
  u8_string repr = PyBytes_AS_STRING(utf8);
  if (repr[0] == '<')
    u8_printf(out,"#🐍%s",repr);
  else {
    u8_puts(out,"#<PYTHON");
    if (PySequence_Check(pyval)) {
      Py_ssize_t len = PySequence_Length(pyval);
      u8_printf(out," len=%lld",(long long)len);}
    if (PyMapping_Check(pyval)) {
      Py_ssize_t n_keys = PyMapping_Size(pyval);
      u8_printf(out," keys=%lld",(long long)n_keys);}
    if (PyNumber_Check(pyval)) {
      u8_puts(out," number");}
    u8_printf(out," '%s'>",repr);}
  Py_DECREF(as_string);
  Py_DECREF(utf8);
  return 1;
}
#else
static int unparse_python_object(u8_output out,lispval obj)
{
  struct KNO_PYTHON_OBJECT *pyo = (struct KNO_PYTHON_OBJECT *)obj;
  PyObject *as_string = PyObject_Unicode(pyo->pyval);
  PyObject *u8        = PyUnicode_AsEncodedString(as_string,"utf8","none");
  u8_string repr      = PyString_AS_STRING(u8);
  if (repr[0] == '<')
    u8_printf(out,"#🐍%s",repr);
  else u8_printf(out,"#<PYTHON %s>",repr);
  Py_DECREF(as_string);
  Py_DECREF(u8);
  return 1;
}
#endif

/* Python/LISP mapping */

static int read_bigint_byte(unsigned char **data)
{
  int val=**data; (*data)++;
  return val;
}
static lispval py2lisp(PyObject *o)
{
  if (o==NULL)
    return translate_python_error("py2lisp");
  else if (o==Py_None)
    return KNO_VOID;
  else if (o==Py_False)
    return KNO_FALSE;
  else if (o==Py_True)
    return KNO_TRUE;
  else if (PyInt_Check(o))
    return KNO_INT2LISP(PyInt_AS_LONG(o));
  else if (PyFloat_Check(o))
    return kno_init_double(NULL,PyFloat_AsDouble(o));
  else if (PyLong_Check(o)) {
    int bitlen=_PyLong_NumBits(o);
    int bytelen=bitlen/8+1;
    int sign=_PyLong_Sign(o);
    if (bitlen<32) {
      long lval=PyLong_AsLong(o);
      return KNO_INT2LISP(lval);}
    else if (bitlen<64) {
      long long lval=PyLong_AsLongLong(o);
      return KNO_INT2LISP(lval);}
    else {
      PyLongObject *plo=(PyLongObject *)o;
      unsigned char bytes[bytelen];
      int retval =_PyLong_AsByteArray(plo,bytes,bytelen,1,1);
      if (retval < 0)
	return kno_err("PythonLongError","py2lisp/long",NULL,KNO_VOID);
      if (sign<0) { negate_bigint(bytes,bytelen);}
      return (lispval)
	kno_digit_stream_to_bigint(bytelen,(bigint_producer)read_bigint_byte,
				  (void *)bytes,256,(sign<0));}}
#if PY_MAJOR_VERSION >= 3
  else if (PyUnicode_Check(o)) {
    PyObject *utf8 = PyUnicode_AsUTF8String(o);
    if (utf8) {
      lispval v = knostring((u8_string)PyBytes_AS_STRING(utf8));
      Py_DECREF(utf8);
      return v;}
    else return kno_err("InvalidPythonString","py2lisp",NULL,KNO_VOID);}
  else if (PyBytes_Check(o)) {
    Py_ssize_t len = PyBytes_GET_SIZE(o);
    char *data = PyBytes_AS_STRING(o);
    return kno_make_packet(NULL,len,data);}
#endif
#if PY_MAJOR_VERSION < 3
  else if (PyString_Check(o)) {
    PyObject *u8=PyString_AsEncodedObject(o,"utf8","none");
    if (u8) {
      lispval v = knostring((u8_string)PyString_AS_STRING(u8));
      Py_DECREF(u8);
      return v;}
    else return kno_err("InvalidPythonString","py2lisp",NULL,KNO_VOID);}
  else if (PyUnicode_Check(o)) {
    PyObject *utf8 = PyUnicode_AsUTF8String(o);
    if (utf8) {
      lispval v = knostring((u8_string)PyBytes_AS_STRING(utf8));
      Py_DECREF(utf8);
      return v;}
    else return kno_err("InvalidPythonString","py2lisp",NULL,KNO_VOID);}
#endif
  else if (PyTuple_Check(o)) {
    int i=0, n=PyTuple_Size(o);
    lispval *data=u8_alloc_n(n,lispval);
    while (i<n) {
      PyObject *pelt=PyTuple_GetItem(o,i);
      lispval elt=py2lisp(pelt);
      data[i]=elt;
      i++;}
    return kno_init_vector(NULL,n,data);}
  else if (PyList_Check(o)) {
    int i=0, n=PyList_Size(o);
    lispval head = KNO_EMPTY_LIST, *tail=&head;
    while (i<n) {
      PyObject *pelt=PyList_GetItem(o,i);
      lispval elt=py2lisp(pelt);
      lispval pair = kno_init_pair(NULL,elt,KNO_EMPTY_LIST);
      struct KNO_PAIR *pair_struct = (kno_pair) pair;
      *tail=pair; tail=&(pair_struct->cdr);
      i++;}
    return head;}
  else if ((PyObject_TypeCheck(o,&PoolType))) {
    struct KNO_PYTHON_POOL *pp=(struct KNO_PYTHON_POOL *)o;
    return kno_pool2lisp(pp->pool);}
  else if ((PyObject_TypeCheck(o,&ApplicableType))) {
    struct KNO_PYTHON_FUNCTION *pf=(struct KNO_PYTHON_FUNCTION *)o;
    return kno_incref(pf->fnval);}
  else if ((PyObject_TypeCheck(o,&IndexType))) {
    struct KNO_PYTHON_INDEX *pi=(struct KNO_PYTHON_INDEX *)o;
    return kno_index2lisp(pi->index);}
  else if ((PyObject_TypeCheck(o,&KnoType)) ||
	   (PyObject_TypeCheck(o,&ChoiceType)) ||
	   (PyObject_TypeCheck(o,&OIDType))) {
    pylisp *v=(struct KNO_PYTHON_WRAPPER *)o;
    return kno_incref(v->lval);}
  else {
    struct KNO_PYTHON_OBJECT *pyo=u8_alloc(struct KNO_PYTHON_OBJECT);
    KNO_INIT_CONS(pyo,python_object_type);
    pyo->pyval=o; Py_INCREF(o);
    return (lispval) pyo;}
}

static u8_string py2string(PyObject *o)
{
  int free_temp = 0;
  if (! ( (PyString_Check(o)) || (PyUnicode_Check(o)) ) ) {
    o = PyObject_Str(o);
    free_temp = 1;}

  if (PyUnicode_Check(o)) {
    PyObject *utf8 = PyUnicode_AsUTF8String(o);
    if (free_temp) Py_DECREF(o);
    if (utf8) {
      u8_string s = u8_strdup((u8_string)PyBytes_AS_STRING(utf8));
      Py_DECREF(utf8);
      return s;}
    else {
      kno_seterr("InvalidPythonString","py2string",NULL,KNO_VOID);
      return NULL;}}
#if PY_MAJOR_VERSION < 3
  else if (PyString_Check(o)) {
    PyObject *u8=PyString_AsEncodedObject(o,"utf8","none");
    if (free_temp) Py_DECREF(o);
    if (u8) {
      u8_string s = u8_strdup((u8_string)PyString_AS_STRING(u8));
      Py_DECREF(u8);
      return s;}
    else {
      kno_seterr("InvalidPythonString","py2string",NULL,KNO_VOID);
      return NULL;}}
#endif
  else {
    if (free_temp) Py_DECREF(o);
    kno_seterr("InvalidPythonString","py2string",NULL,KNO_VOID);
    return NULL;}
}

static lispval py2sym(PyObject *o)
{
#if PY_MAJOR_VERSION >= 3
  if (PyUnicode_Check(o)) {
    PyObject *utf8 = PyUnicode_AsUTF8String(o);
    if (utf8) {
      lispval v = kno_intern((u8_string)PyBytes_AS_STRING(utf8));
      Py_DECREF(utf8);
      return v;}
    else return kno_err("InvalidPythonString","py2lisp",NULL,KNO_VOID);}
#endif
#if PY_MAJOR_VERSION < 3
  if (PyString_Check(o)) {
    PyObject *u8=PyString_AsEncodedObject(o,"utf8","none");
    if (u8) {
      lispval v = kno_intern((u8_string)PyString_AS_STRING(u8));
      Py_DECREF(u8);
      return v;}
    else return kno_err("InvalidPythonString","py2lisp",NULL,KNO_VOID);}
  else if (PyUnicode_Check(o)) {
    PyObject *utf8 = PyUnicode_AsUTF8String(o);
    if (utf8) {
      lispval v = kno_intern((u8_string)PyBytes_AS_STRING(utf8));
      Py_DECREF(utf8);
      return v;}
    else return kno_err("InvalidPythonString","py2lisp",NULL,KNO_VOID);}
#endif
  else return py2lisp(o);
}

/* This parses string args and is used for calling functions through
   Python. */
static lispval py2lispx(PyObject *o)
{
  if (o==Py_None)
    return KNO_VOID;
  else if (o==Py_False)
    return KNO_FALSE;
  else if (o==Py_True)
    return KNO_TRUE;
  else if (PyInt_Check(o))
    return KNO_INT2LISP(PyInt_AS_LONG(o));
  else if (PyFloat_Check(o))
    return kno_init_double(NULL,PyFloat_AsDouble(o));
  else if (PyLong_Check(o)) {
    int bitlen=_PyLong_NumBits(o);
    int bytelen=bitlen/8+1;
    int sign=_PyLong_Sign(o);
    if (bitlen<32) {
      long lval=PyLong_AsLong(o);
      return KNO_INT2LISP(lval);}
    else if (bitlen<64) {
      long long lval=PyLong_AsLongLong(o);
      return KNO_INT2LISP(lval);}
    else {
      PyLongObject *plo=(PyLongObject *)o;
      unsigned char bytes[bytelen];
      int retval =_PyLong_AsByteArray(plo,bytes,bytelen,1,1);
      if (retval<0)
	return kno_err("PythonLongError","py2lispx/long",NULL,KNO_VOID);
      if (sign<0) { negate_bigint(bytes,bytelen);}
      return (lispval) kno_digit_stream_to_bigint(bytelen,(bigint_producer)read_bigint_byte,
						 (void *)bytes,256,(sign<0));}}
#if PY_MAJOR_VERSION < 3
  else if (PyString_Check(o)) {
    PyObject *u8=PyString_AsEncodedObject(o,"utf8","none");
    if (u8) {
      u8_string sdata=(u8_string)PyString_AS_STRING(u8);
      lispval v=((*sdata==':') ? (kno_parse(sdata+1)) : (kno_parse(sdata)));
      Py_DECREF(u8);
      return v;}
    else {
      translate_python_error("py2lispx");
      return KNO_ERROR_VALUE;}}
  else if (PyUnicode_Check(o)) {
    PyObject *u8=PyUnicode_AsEncodedObject(o,"utf8","none");
    if (u8) {
      u8_string sdata=(u8_string)PyString_AS_STRING(u8);
      lispval v=((*sdata==':') ? (kno_parse(sdata+1)) : (kno_parse(sdata)));
      Py_DECREF(u8);
      return v;}
    else {
      translate_python_error("py2lispx");
      return KNO_ERROR_VALUE;}}
#else
  else if (PyUnicode_Check(o)) {
    PyObject *utf8 = PyUnicode_AsUTF8String(o);
    if (utf8) {
      lispval v=kno_parse((u8_string)PyBytes_AS_STRING(utf8));
      Py_DECREF(utf8);
      return v;}
    else {
      translate_python_error("py2lispx");
      return KNO_ERROR_VALUE;}}
#endif
  else if (PyTuple_Check(o)) {
    int i=0, n=PyTuple_Size(o);
    lispval *data=u8_alloc_n(n,lispval);
    while (i<n) {
      PyObject *pelt=PyTuple_GetItem(o,i);
      lispval elt=py2lispx(pelt);
      data[i]=elt;
      i++;}
    return kno_init_vector(NULL,n,data);}
  else if ((PyObject_TypeCheck(o,&PoolType))) {
    struct KNO_PYTHON_POOL *pp=(struct KNO_PYTHON_POOL *)o;
    return kno_pool2lisp(pp->pool);}
  else if ((PyObject_TypeCheck(o,&IndexType))) {
    struct KNO_PYTHON_INDEX *pi=(struct KNO_PYTHON_INDEX *)o;
    return kno_index2lisp(pi->index);}
  else if ((PyObject_TypeCheck(o,&KnoType)) ||
	   (PyObject_TypeCheck(o,&ChoiceType)) ||
	   (PyObject_TypeCheck(o,&OIDType))) {
    pylisp *v=(struct KNO_PYTHON_WRAPPER *)o;
    return kno_incref(v->lval);}
  else return KNO_VOID;
}

static void output_bigint_byte(unsigned char **scan,int digit)
{
  **scan=digit; (*scan)++;
}

static PyObject *lisp2py(lispval o)
{
  if (KNO_FIXNUMP(o)) {
    long lval=KNO_FIX2INT(o);
    return PyInt_FromLong(lval);}
  else if (KNO_IMMEDIATEP(o)) {
    if (KNO_VOIDP(o)) Py_RETURN_NONE;
    else if (KNO_TRUEP(o)) Py_RETURN_TRUE;
    else if (KNO_FALSEP(o)) Py_RETURN_FALSE;
    else if (KNO_EMPTY_CHOICEP(o)) {
      pylisp *po=newpychoice();
      po->lval=KNO_EMPTY_CHOICE;
      return (PyObject *)po;}
    else {
      pylisp *po=newpylisp();
      po->lval=kno_incref(o);
      return (PyObject *)po;}}
  else if (KNO_OIDP(o)) {
    pylisp *po=newpyoid();
    po->lval=kno_incref(o);
    return (PyObject *)po;}
  else if (KNO_STRINGP(o))
    return PyUnicode_DecodeUTF8
      ((char *)KNO_STRING_DATA(o),KNO_STRING_LENGTH(o),"none");
  else if (KNO_PRIM_TYPEP(o,python_object_type)) {
    struct KNO_PYTHON_OBJECT *fdpo=(kno_python_object)o;
    PyObject *o=fdpo->pyval;
    Py_INCREF(o);
    return o;}
  else if (KNO_FLONUMP(o)) {
    double dval=KNO_FLONUM(o);
    return PyFloat_FromDouble(dval);}
  else if (KNO_CHOICEP(o)) {
    pylisp *po=newpychoice();
    po->lval=kno_incref(o);
    return (PyObject *)po;}
  else if (KNO_VECTORP(o)) {
    int i=0, n=KNO_VECTOR_LENGTH(o);
    lispval *data=KNO_VECTOR_DATA(o);
    PyObject *tuple=PyTuple_New(n);
    if (tuple==NULL)
      return pass_error();
    else while (i<n) {
      PyObject *v=lisp2py(data[i]);
      if (v==NULL) {
	Py_DECREF(tuple);
	return pass_error();}
      PyTuple_SET_ITEM(tuple,i,v);
      i++;}
    return (PyObject *)tuple;}
  else if (KNO_PAIRP(o)) {
    int len = kno_list_length(o);
    if (len>=0) {
      PyObject *list=PyList_New(len);
      if (list==NULL)
	return pass_error();
      int i = 0;
      lispval scan = o;
      while (PAIRP(scan)) {
	lispval car = KNO_CAR(scan);
	PyObject *pycar = lisp2py(car);
	PyList_SetItem(list,i,pycar);
	scan=KNO_CDR(scan);
	i++;}
      return list;}
    else {
      pylisp *po=newpylisp();
      po->lval=kno_incref(o);
      return (PyObject *)po;}}
  else if (KNO_BIGINTP(o)) {
    kno_bigint big=(kno_bigint) o;
    if (kno_small_bigintp(big)) {
      long lval=kno_bigint_to_long(big);
      return PyInt_FromLong(lval);}
    else if (kno_modest_bigintp(big)) {
      long long llval=kno_bigint_to_long_long(big);
      return PyLong_FromLongLong(llval);}
    else {
      PyObject *pylong;
      int n_bytes=kno_bigint_length_in_bytes(big);
      int negativep=kno_bigint_negativep(big);
      unsigned char _bytes[64], *bytes, *scan;
      if (n_bytes>=63) scan=bytes=u8_malloc(n_bytes+1);
      else scan=bytes=_bytes;
      kno_bigint_to_digit_stream
	(big,256,output_bigint_byte,(void *)&scan);
      if (negativep) n_bytes=negate_bigint(bytes,n_bytes);
      pylong=_PyLong_FromByteArray(bytes,n_bytes,1,1);
      if (bytes!=_bytes) u8_free(bytes);
      return pylong;}}
  else if (KNO_PRECHOICEP(o)) {
    lispval simplified = kno_make_simple_choice(o);
    if (KNO_CHOICEP(simplified)) {
      pylisp *po=newpychoice();
      po->lval=simplified;
      return (PyObject *)po;}
    else {
      PyObject *po = lisp2py(simplified);
      kno_decref(simplified);
      return po;}}
  else if (KNO_ABORTP(o))
    return pass_error();
  else if (KNO_APPLICABLEP(o)) {
    pylisp *po=newpyapply(o);
    return (PyObject *)po;}
  else {
    pylisp *po=newpylisp();
    po->lval=kno_incref(o);
    return (PyObject *)po;}
}

lispval PySequence2Choice(PyObject *pyo,int decref)
{
  if (PySequence_Check(pyo)) {
    lispval results = KNO_EMPTY;
    Py_ssize_t i=0, n=PySequence_Size(pyo);
    while (i<n) {
      PyObject *pelt = PySequence_Fast_GET_ITEM(pyo,i);
      lispval elt = py2lisp(pelt);
      if (KNO_ABORTED(elt)) {
	if (decref) Py_DECREF(pyo);
	kno_decref(results);
	return elt;}
      else {KNO_ADD_TO_CHOICE(results,elt);}
      i++;}
    if (decref) Py_DECREF(pyo);
    return kno_simplify_choice(results);}
  else return kno_type_error("PythonSequence","PyList2Choice",KNO_VOID);
}

/* Printed representation */

#define PyUTF8String(s,len) PyUnicode_DecodeUTF8((char *)(s),len,"none")

static PyObject *pylisp_str(PyObject *self)
{
  struct KNO_PYTHON_WRAPPER *pw=(struct KNO_PYTHON_WRAPPER *)self;
  struct U8_OUTPUT out; u8_byte buf[64];
  PyObject *pystring;
  U8_INIT_OUTPUT_X(&out,32,buf,U8_OUTPUT_STREAM);
  u8_printf(&out,"kno.ref('%q')",pw->lval);
  pystring=PyUTF8String(out.u8_outbuf,out.u8_write-out.u8_outbuf);
  if (out.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(out.u8_outbuf);
  return pystring;
}

static PyObject *choice_str(PyObject *self)
{
  struct KNO_PYTHON_WRAPPER *pw=(struct KNO_PYTHON_WRAPPER *)self;
  struct U8_OUTPUT out; u8_byte buf[64];
  PyObject *pystring; int i=0;
  U8_INIT_OUTPUT_X(&out,32,buf,U8_OUTPUT_STREAM);
  u8_puts(&out,"kno.Choice(");
  {KNO_DO_CHOICES(elt,pw->lval) {
      if (i>0) u8_putc(&out,','); i++;
      if (KNO_STRINGP(elt)) {
	int c=KNO_STRDATA(elt)[0];
	u8_putc(&out,'\'');
	if (strchr("@(#",c))  u8_putc(&out,'\\');
	u8_puts(&out,KNO_STRDATA(elt));
	u8_putc(&out,'\'');}
      else if (KNO_SYMBOLP(elt))
	u8_printf(&out,"':%s'",KNO_SYMBOL_NAME(elt));
      else u8_printf(&out,"'%q'",elt);}}
  u8_putc(&out,')');
  pystring=PyUTF8String(out.u8_outbuf,out.u8_write-out.u8_outbuf);
  if (out.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(out.u8_outbuf);
  return pystring;
}

static PyObject *oid_str(PyObject *self)
{
  struct KNO_PYTHON_WRAPPER *pw=(struct KNO_PYTHON_WRAPPER *)self;
  struct U8_OUTPUT out; u8_byte buf[64];
  PyObject *pystring; int i=0;
  U8_INIT_OUTPUT_X(&out,32,buf,U8_OUTPUT_STREAM);
  u8_puts(&out,"kno.OID(");
  {KNO_DO_CHOICES(elt,pw->lval) {
      if (i>0) u8_putc(&out,','); i++;
      if (KNO_STRINGP(elt)) {
	int c=KNO_STRDATA(elt)[0];
	u8_putc(&out,'\'');
	if (strchr("@(#",c))  u8_putc(&out,'\\');
	u8_puts(&out,KNO_STRDATA(elt));
	u8_putc(&out,'\'');}
      else if (KNO_SYMBOLP(elt))
	u8_printf(&out,"':%s'",KNO_SYMBOL_NAME(elt));
      else u8_printf(&out,"'%q'",elt);}}
  u8_putc(&out,')');
  pystring=PyUTF8String(out.u8_outbuf,out.u8_write-out.u8_outbuf);
  if (out.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(out.u8_outbuf);
  return pystring;
}

static PyObject *pylisp_get(PyObject *self,PyObject *arg)
{
  lispval frames=py2lisp(self), slotids=py2lispx(arg), value=KNO_EMPTY_CHOICE;
  if ((KNO_CHOICEP(frames)) && (KNO_FIXNUMP(slotids))) {
    PyObject *elt=lisp2py((KNO_CHOICE_DATA(frames))[KNO_FIX2INT(slotids)]);
    kno_decref(frames);
    return elt;}
  else {
    PyObject *result;
    KNO_DO_CHOICES(frame,frames) {
      KNO_DO_CHOICES(slotid,slotids) {
	lispval v;
	if (KNO_OIDP(frame)) v=kno_frame_get(frame,slotid);
	else v=kno_get(frame,slotid,KNO_EMPTY_CHOICE);
	KNO_ADD_TO_CHOICE(value,v);}}
    result=lisp2py(value);
    kno_decref(frames);
    kno_decref(slotids);
    kno_decref(value);
    return result;}
}

static int pylisp_set(PyObject *self,PyObject *arg,PyObject *val)
{
  lispval frames=py2lisp(self), slotids=py2lisp(arg), values=py2lisp(val);
  KNO_DO_CHOICES(frame,frames) {
    KNO_DO_CHOICES(slotid,slotids) {
      kno_store(frame,slotid,values);}}
  kno_decref(frames); kno_decref(slotids); kno_decref(values);
  return 1;
}

static int choice_length(pw)
     struct KNO_PYTHON_WRAPPER *pw;
{                                   /* or repr or str */
  return KNO_CHOICE_SIZE(pw->lval);
}

static PyObject *choice_merge(pw1,pw2)
     PyObject *pw1, *pw2;
{                                   /* or repr or str */
  lispval choices[2]; PyObject *result;
  choices[0]=py2lisp(pw1); choices[1]=py2lisp(pw2);
  lispval combined=kno_union(choices,2);
  result=lisp2py(combined);
  kno_decref(combined); kno_decref(choices[0]); kno_decref(choices[1]);
  return result;
}

static PyObject *choice_item(struct KNO_PYTHON_WRAPPER *pw,int i)
{                                   /* or repr or str */
  if (i<(KNO_CHOICE_SIZE(pw->lval)))
    return lisp2py((KNO_CHOICE_DATA(pw->lval))[i]);
  else {
    PyErr_Format(PyExc_IndexError,"choice only has %d (<=%d) elements",
		 KNO_CHOICE_SIZE(pw->lval),i);
    return NULL;}
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

static PyTypeObject KnoType = {      /* main python type-descriptor */
  /* type header */                    /* shared by all instances */
  PyVarObject_HEAD_INIT(&PyType_Type,0)
  "kno",                         /* tp_name */
  sizeof(struct KNO_PYTHON_WRAPPER),/* tp_basicsize */
  0,                               /* tp_itemsize */

  /* standard methods */
  (destructor)  pylisp_dealloc,   /* tp_dealloc  ref-count==0  */
  (printfunc)   NULL,     /* tp_print    "print x"     */
  (getattrfunc) NULL,   /* tp_getattr  "x.attr"      */
  (setattrfunc) NULL,               /* tp_setattr  "x.attr=v"    */
#if PY_MAJOR_VERSION < 3
  (cmpfunc)     pylisp_compare,   /* tp_compare  "x > y"       */
#else
  NULL,
#endif
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

static PyTypeObject OIDType = {      /* main python type-descriptor */
  /* type header */                    /* shared by all instances */
  PyVarObject_HEAD_INIT(&PyType_Type,0)
  "kno",                         /* tp_name */
  sizeof(struct KNO_PYTHON_WRAPPER),/* tp_basicsize */
  0,                               /* tp_itemsize */

  /* standard methods */
  (destructor)  pylisp_dealloc,   /* tp_dealloc  ref-count==0  */
  (printfunc)   NULL,     /* tp_print    "print x"     */
  (getattrfunc) NULL,   /* tp_getattr  "x.attr"      */
  (setattrfunc) NULL,               /* tp_setattr  "x.attr=v"    */
#if PY_MAJOR_VERSION < 3
  (cmpfunc)     pylisp_compare,   /* tp_compare  "x > y"       */
#else
  NULL,
#endif
  (reprfunc)    oid_str,               /* tp_repr     `x`, print x  */

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
  PyVarObject_HEAD_INIT(&PyType_Type,0)
  "choice",                         /* tp_name */
  sizeof(struct KNO_PYTHON_WRAPPER),/* tp_basicsize */
  0,                               /* tp_itemsize */

  /* standard methods */
  (destructor)  pylisp_dealloc,   /* tp_dealloc  ref-count==0  */
  (printfunc)   NULL,     /* tp_print    "print x"     */
  (getattrfunc) NULL,   /* tp_getattr  "x.attr"      */
  (setattrfunc) NULL,               /* tp_setattr  "x.attr=v"    */
#if PY_MAJOR_VERSION < 3
  (cmpfunc)     pylisp_compare,   /* tp_compare  "x > y"       */
#else
  NULL,
#endif
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

static PyTypeObject ApplicableType = {      /* main python type-descriptor */
  /* type header */                    /* shared by all instances */
  PyVarObject_HEAD_INIT(&PyType_Type,0)
  "kno",                         /* tp_name */
  sizeof(struct KNO_PYTHON_WRAPPER),/* tp_basicsize */
  0,                               /* tp_itemsize */

  /* standard methods */
  (destructor)  pylisp_dealloc,   /* tp_dealloc  ref-count==0  */
  (printfunc)   NULL,     /* tp_print    "print x"     */
  (getattrfunc) NULL,   /* tp_getattr  "x.attr"      */
  (setattrfunc) NULL,               /* tp_setattr  "x.attr=v"    */
#if PY_MAJOR_VERSION < 3
  (cmpfunc)     pylisp_compare,   /* tp_compare  "x > y"       */
#else
  NULL,
#endif
  (reprfunc)    pylisp_str,               /* tp_repr     `x`, print x  */

  /* type categories */
  0,                             /* tp_as_number   +,-,*,/,%,&,>>,pow...*/
  0,            /* tp_as_sequence +,[i],[i:j],len, ...*/
  &table_methods, /* tp_as_mapping  [key], len, ...*/

  /* more methods */
  (hashfunc)     0,              /* tp_hash    "dict[x]" */
  (ternaryfunc)  pylisp_apply,              /* tp_call    "x()"     */
  (reprfunc)     0,              /* tp_str     "str(x)"  */

};  /* plus others: see Include/object.h */


static PyTypeObject PoolType;

/*****************************************************************************
 * BASIC TYPE-OPERATIONS
 *****************************************************************************/

static pypool *             /* on "x = stacktype.Stack()" */
newpool()                 /* instance constructor function */
{                                /* these don't get an 'args' input */
  struct KNO_PYTHON_POOL *self;
  self = PyObject_New(pypool, &PoolType);  /* malloc, init, incref */
  if (self == NULL)
    return NULL;            /* raise exception */
  self->pool=NULL;
  return self;                /* a new type-instance object */
}

static void                     /* instance destructor function */
pool_dealloc(self)             /* when reference-count reaches zero */
    struct KNO_PYTHON_POOL *self;
{                               /* do cleanup activity */
  PyObject_Del(self);            /* same as 'free(self)' */
}

#if PY_MAJOR_VERSION >= 3
static PyObject *pool_compare(PyObject *v_arg,PyObject *w_arg,int op)
{
  if ( (PyObject_TypeCheck(v_arg,&PoolType)) &&
       (PyObject_TypeCheck(w_arg,&PoolType)) ) {
    struct KNO_PYTHON_POOL *v = (struct KNO_PYTHON_POOL *) v_arg;
    struct KNO_PYTHON_POOL *w = (struct KNO_PYTHON_POOL *) w_arg;
    int cmp = (v->pool<w->pool) ? (-1) : (v->pool == w->pool) ? (0): (1);
    return do_rich_compare(cmp,op);}
  else return Py_NotImplemented;
}
#else
static int pool_compare(struct KNO_PYTHON_POOL *v,struct KNO_PYTHON_POOL *w,
			int op)
{
  int cmp = (v->pool<w->pool) ? (-1) :
    (v->pool == w->pool) ? (0): (1);
  return cmp;
}
#endif

static PyObject *pool_str(PyObject *self)
{
  struct KNO_PYTHON_POOL *pw=(struct KNO_PYTHON_POOL *)self;
  return PyString_FromFormat("kno.pool('%s')",pw->pool->pool_source);
}

static PyTypeObject PoolType = {      /* main python type-descriptor */
  /* type header */                    /* shared by all instances */
  PyVarObject_HEAD_INIT(&PyType_Type,0)
  "pool",                         /* tp_name */
  sizeof(struct KNO_PYTHON_POOL),/* tp_basicsize */
  0,                               /* tp_itemsize */

  /* standard methods */
  (destructor)  pool_dealloc,   /* tp_dealloc  ref-count==0  */
  (printfunc)   NULL,     /* tp_print    "print x"     */
  (getattrfunc) NULL,   /* tp_getattr  "x.attr"      */
  (setattrfunc) NULL,               /* tp_setattr  "x.attr=v"    */
#if PY_MAJOR_VERSION < 3
  (cmpfunc)     pool_compare,   /* tp_compare  "x > y"       */
#else
  NULL,
#endif
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

/*****************************************************************************
 * BASIC TYPE-OPERATIONS
 *****************************************************************************/

static pyindex *             /* on "x = stacktype.Stack()" */
newindex()                 /* instance constructor function */
{                                /* these don't get an 'args' input */
  struct KNO_PYTHON_INDEX *self;
  self = PyObject_New(pyindex, &IndexType);  /* malloc, init, incref */
  if (self == NULL)
    return NULL;            /* raise exception */
  self->index=NULL;
  return self;                /* a new type-instance object */
}

static void                     /* instance destructor function */
index_dealloc(self)             /* when reference-count reaches zero */
    struct KNO_PYTHON_INDEX *self;
{                               /* do cleanup activity */
  PyObject_Del(self);            /* same as 'free(self)' */
}

#if PY_MAJOR_VERSION >= 3
static PyObject *index_compare(PyObject *v_arg,PyObject *w_arg,int op)
{
  if ( (PyObject_TypeCheck(v_arg,&IndexType)) &&
       (PyObject_TypeCheck(w_arg,&IndexType)) ) {
    struct KNO_PYTHON_INDEX *v = (struct KNO_PYTHON_INDEX *) v_arg;
    struct KNO_PYTHON_INDEX *w = (struct KNO_PYTHON_INDEX *) w_arg;
    int cmp = (v->index<w->index) ? (-1) : (v->index == w->index) ? (0): (1);
    return do_rich_compare(cmp,op);}
  else return Py_NotImplemented;
}
#else
static int index_compare(struct KNO_PYTHON_INDEX *v,struct KNO_PYTHON_INDEX *w)
{
  if (v->index<w->index)
    return -1;
  else if (v->index == w->index)
    return 0;
  else return 1;
}
#endif

static PyObject *index_str(PyObject *self)
{
  struct KNO_PYTHON_INDEX *pw=(struct KNO_PYTHON_INDEX *)self;
  return PyString_FromFormat("kno.index('%s')",pw->index->index_source);
}

static PyTypeObject IndexType = {      /* main python type-descriptor */
  /* type header */                    /* shared by all instances */
  PyVarObject_HEAD_INIT(&PyType_Type,0)
  "index",                         /* tp_name */
  sizeof(struct KNO_PYTHON_INDEX),/* tp_basicsize */
  0,                               /* tp_itemsize */

  /* standard methods */
  (destructor)  index_dealloc,   /* tp_dealloc  ref-count==0  */
  (printfunc)   NULL,     /* tp_print    "print x"     */
  (getattrfunc) NULL,   /* tp_getattr  "x.attr"      */
  (setattrfunc) NULL,               /* tp_setattr  "x.attr=v"    */
#if PY_MAJOR_VERSION < 3
  (cmpfunc)     index_compare,   /* tp_compare  "x > y"       */
#else
  NULL,
#endif
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

#if PY_MAJOR_VERSION < 3
static PyObject *pylisp_ref(PyObject *self,PyObject *arg)
{
  if (PyString_Check(arg)) {
    lispval obj;
    PyObject *u8=PyString_AsEncodedObject(arg,"utf8","none");
    if (u8) obj=kno_parse((u8_string)(PyString_AS_STRING(u8)));
    else return pass_error();
    Py_DECREF(u8);
    return lisp2py(obj);}
  else return NULL;
}
#else
static PyObject *pylisp_ref(PyObject *self,PyObject *arg)
  {
  if (PyUnicode_Check(arg)) {
  lispval obj;
  PyObject *utf8 = PyUnicode_AsUTF8String(arg);
  if (utf8)
    obj=kno_parse((u8_string)(PyBytes_AS_STRING(utf8)));
  else return pass_error();
  Py_DECREF(utf8);
  return lisp2py(obj);}
  else return NULL;
  }
#endif

static PyObject *usepool(PyObject *self,PyObject *arg)
{
  kno_pool p;
  if (PyText_Check(arg)) {
    PyObject *utf8 = NULL;
    u8_string spec = pytext2utf8(arg,&utf8);
    if (spec)
      p=kno_use_pool(spec,-1,KNO_FALSE);
    else return pass_error();
    if (utf8) Py_DECREF(utf8);}
  else return NULL;
  if (p) {
    struct KNO_PYTHON_POOL *pw=(struct KNO_PYTHON_POOL *)newpool();
    pw->pool=p;
    return (PyObject *) pw;}
  else return pass_error();
}

static PyObject *openindex(PyObject *self,PyObject *arg)
{
  kno_index ix;
  if (PyText_Check(arg)) {
    PyObject *utf8 = NULL;
    u8_string spec = pytext2utf8(arg,&utf8);
    if (spec == NULL)
      return pass_error();
    else ix = kno_open_index(spec,-1,KNO_FALSE);
    if (utf8) Py_DECREF(utf8);}
  else return NULL;
  if (ix) {
    struct KNO_PYTHON_INDEX *pw=(struct KNO_PYTHON_INDEX *)newindex();
    pw->index=ix;
    return (PyObject *) pw;}
  else return pass_error();
}

static PyObject *useindex(PyObject *self,PyObject *arg)
{
  kno_index ix;
  if (PyText_Check(arg)) {
    PyObject *utf8; u8_string spec = pytext2utf8(arg,&utf8);
    if (spec == NULL)
      return pass_error();
    else ix = kno_use_index(spec,-1,KNO_FALSE);
    if (utf8) Py_DECREF(utf8);}
  else return NULL;
  if (ix) {
    struct KNO_PYTHON_INDEX *pw=(struct KNO_PYTHON_INDEX *)newindex();
    pw->index=ix;
    return (PyObject *) pw;}
  else return pass_error();
}

static PyObject *setcachelevel(PyObject *self,PyObject *arg)
{
  if (PyInt_Check(arg)) {
    long old_level=kno_default_cache_level;
    long val=PyInt_AS_LONG(arg);
    kno_default_cache_level=val;
    return PyInt_FromLong(old_level);}
  else return NULL;
}

static PyObject *lispget(PyObject *self,PyObject *args)
{
  if ((PyTuple_Check(args)) && (PyTuple_GET_SIZE(args)==2)) {
    PyObject *arg0=PyTuple_GET_ITEM(args,0);
    PyObject *arg1=PyTuple_GET_ITEM(args,1), *result;
    lispval frames=py2lispx(arg0), slotids=py2lispx(arg1);
    lispval results=KNO_EMPTY_CHOICE;
    Py_BEGIN_ALLOW_THREADS {
      KNO_DO_CHOICES(f,frames) {
	KNO_DO_CHOICES(slotid,slotids) {
	  lispval v=kno_frame_get(f,slotid);
	  KNO_ADD_TO_CHOICE(results,v);}}}
    Py_END_ALLOW_THREADS;
    result=lisp2py(results);
    kno_decref(frames); kno_decref(slotids); kno_decref(results);
    return result;}
  else return NULL;
}

static PyObject *lispfind(PyObject *self,PyObject *args)
{
  PyObject *result;
  if (PyTuple_Check(args)) {
    int i=0, size=PyTuple_GET_SIZE(args), sv_start=0, sv_size=size;
    lispval *slotvals, results, indices=KNO_VOID;
    if (size%2) {
      PyObject *arg0=PyTuple_GET_ITEM(args,0);
      indices=py2lispx(arg0);
      sv_size=size-1; sv_start=1;}
    slotvals=u8_alloc_n(sv_size,lispval);
    i=0; while (i<sv_size) {
      PyObject *arg0=PyTuple_GET_ITEM(args,sv_start+i);
      PyObject *arg1=PyTuple_GET_ITEM(args,sv_start+i+1);
      lispval slotids=py2lispx(arg0), values=py2lisp(arg1);
      slotvals[i++]=slotids; slotvals[i++]=values;}
    Py_BEGIN_ALLOW_THREADS {
      if (KNO_VOIDP(indices))
	results=kno_bgfinder(sv_size,slotvals);
      else results=kno_finder(indices,sv_size,slotvals);
      i=0; while (i<sv_size) {lispval v=slotvals[i++]; kno_decref(v);}
      u8_free(slotvals);}
    Py_END_ALLOW_THREADS;
    result=lisp2py(results);
    kno_decref(results);
    return result;}
  else return NULL;
}

#if 0
static PyObject *lispcall(PyObject *self,PyObject *args)
{
  PyObject *pyresult;
  if (PyTuple_Check(args)) {
    int size=PyTuple_GET_SIZE(args), n_args=size-1;
    if (size==0) {
      PyErr_Format(PyExc_IndexError,"Missing function arg");
      return NULL;}
    lispval fn = fn=py2lisp(PyTuple_GET_ITEM(args,0)), result = KNO_VOID;
    if (! (KNO_APPLICABLEP(fn)) ) {
      u8_string s = kno_lisp2string(fn);
      PyErr_Format(PyExc_TypeError,"Not applicable: %s",s);
      u8_free(s);
      kno_decref(fn);
      return NULL;}
    if (n_args == 0) {
      Py_BEGIN_ALLOW_THREADS {
	result = kno_apply(fn,0,NULL);}
      Py_END_ALLOW_THREADS;}
    else {
      lispval argv[size];
      int i=0; while (i<n_args) {
	PyObject *arg=PyTuple_GET_ITEM(args,i+1);
	argv[i]=py2lisp(arg);
	i++;}
      Py_BEGIN_ALLOW_THREADS {
	result=kno_apply(fn,size-1,argv);}
      Py_END_ALLOW_THREADS;
      i=0; while (i<n_args) {
	lispval v=argv[i++]; kno_decref(v);}}
    pyresult = lisp2py(result);
    return pyresult;}
  else return NULL;
}
#endif

static PyObject *lispeval(PyObject *self,PyObject *pyexpr)
{
  lispval expr=py2lispx(pyexpr), value;
  PyObject *pyvalue;
  Py_BEGIN_ALLOW_THREADS {
    value=kno_eval(expr,default_env,NULL);}
  Py_END_ALLOW_THREADS;
  pyvalue=lisp2py(value);
  kno_decref(expr); kno_decref(value);
  return pyvalue;
}

static PyObject *lispfn(PyObject *self,PyObject *args)
{
  if ((PyTuple_Check(args)) && (PyTuple_GET_SIZE(args)==2)) {
    lispval modname=py2lispx(PyTuple_GET_ITEM(args,0));
    lispval fname=py2lispx(PyTuple_GET_ITEM(args,1));
    lispval module=kno_find_module(modname,0);
    lispval fn=kno_get(module,fname,KNO_VOID);
    PyObject *result=((KNO_VOIDP(fn)) ? (NULL) : (lisp2py(fn)));
    kno_decref(modname); kno_decref(fname); kno_decref(module); kno_decref(fn);
    return result;}
  else return lispeval(self,args);
}

static PyObject *lispmod(PyObject *self,PyObject *args)
{
  if ((PyTuple_Check(args)) && (PyTuple_GET_SIZE(args)==1)) {
    lispval modname = py2lispx(PyTuple_GET_ITEM(args,0));
    lispval module  = kno_find_module(modname,0);
    lispval methods = KNO_VOID;
    if (KNO_HASHTABLEP(module))
      methods = module;
    else if (KNO_LEXENVP(module)) {
      kno_lexenv env = (kno_lexenv) module;
      methods = env->env_exports;}
    else NO_ELSE;
    kno_incref(methods);
    kno_decref(module);
    if (KNO_HASHTABLEP(methods))
      return lisp2py(methods);
    else return NULL;}
  else return lispeval(self,args);
}

#if PY_MAJOR_VERSION >= 3
static struct PyMethodDef kno_methods[]=
  {{"ref",pylisp_ref,METH_VARARGS,"Resolve a LISP reference"},
   {"pool",usepool,METH_VARARGS,"Resolve a LISP reference"},
   {"usepool",usepool,METH_VARARGS,"Resolve a LISP reference"},
   {"useindex",useindex,METH_VARARGS,"Resolve a LISP reference"},
   {"index",openindex,METH_VARARGS,"Resolve a LISP reference"},
   {"openindex",openindex,METH_VARARGS,"Resolve a LISP reference"},
   {"setcachelevel",setcachelevel,METH_VARARGS,"Resolve a LISP reference"},
   {"get",lispget,METH_VARARGS,"Resolve a LISP reference"},
   {"find",lispfind,METH_VARARGS,"Resolve a LISP reference"},
   /* {"call",lispcall}, */
   {"eval",lispeval,METH_VARARGS,"Resolve a LISP reference"},
   {"method",lispfn,METH_VARARGS,"Resolve a LISP reference"},
   {"module",lispmod,METH_VARARGS,"Resolve a LISP reference"},
   {NULL,NULL,0,NULL}};
#else
static struct PyMethodDef kno_methods[]=
  {{"ref",pylisp_ref},
   {"pool",usepool},
   {"usepool",usepool},
   {"useindex",useindex},
   {"index",openindex},
   {"openindex",openindex},
   {"setcachelevel",setcachelevel},
   {"get",lispget},
   {"find",lispfind},
   /* {"call",lispcall}, */
   {"eval",lispeval},
   {"method",lispfn},
   {"module",lispmod},
   {NULL,NULL}};
#endif

/* Table methods for Python objects */

DEFC_PRIM("PY/GET",pyget,MAX_ARGS(2)|MIN_ARGS(1),
	  "Gets a field from a python object",
	  {"pyobj",PYTHON_OBJECT_TYPE,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID})
static lispval pyget(lispval pyobj,lispval key)
{
  PyObject *o = lisp2py(pyobj), *v;
  if (KNO_STRINGP(key))
    v = PyObject_GetAttrString(o,KNO_STRDATA(key));
  else if (KNO_SYMBOLP(key)) {
    u8_string pname = KNO_SYMBOL_NAME(key);
    v = PyObject_GetAttrString(o,pname);}
  else {
    PyObject *k = lisp2py(key);
    v = PyObject_GetItem(o,k);
    Py_DECREF(k);}
  if (v == NULL) {
    PyErr_Clear();
    return KNO_EMPTY;}
  else {
    lispval r = py2lisp(v);
    Py_DECREF(v);
    return r;}
}

DEFC_PRIM("PY/HAS",pyhas,MAX_ARGS(2)|MIN_ARGS(2),
	  "Returns true if a python object has a field",
	  {"pyobj",PYTHON_OBJECT_TYPE,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID});
static lispval pyhas(lispval obj,lispval key)
{
  PyObject *o = lisp2py(obj); int has = -1;
  if (KNO_STRINGP(key))
    has = PyObject_HasAttrString(o,KNO_STRDATA(key));
  else if (KNO_SYMBOLP(key)) {
    u8_string pname = KNO_SYMBOL_NAME(key);
    has = PyObject_HasAttrString(o,pname);}
  else {
    PyObject *k = lisp2py(key);
    has = PyObject_HasAttr(o,k);
    Py_DECREF(k);}
  if (has<0) {
    PyErr_Clear();
    return KNO_FALSE;}
  else if (has)
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* Primitives for embedded Python */

static lispval pyerr(u8_context cxt)
{
  PyObject *err=PyErr_Occurred();
  if (err) {
    PyObject *type, *val, *tb;
    PyErr_Fetch(&type,&val,&tb);
    lispval v = kno_make_nvector(3,py2lisp(type),py2lisp(val),py2lisp(tb));
    if (type) Py_DECREF(type);
    if (val) Py_DECREF(val);
    if (tb) Py_DECREF(tb);
    PyErr_Clear();
    return v;}
  else return kno_err("Mysterious Python error",cxt,NULL,KNO_VOID);
}

static lispval pyerrobj(u8_context cxt)
{
  PyObject *err=PyErr_Occurred();
  if (err) {
    PyObject *type, *val, *tb;
    PyErr_Fetch(&type,&val,&tb);
    lispval v = kno_make_nvector(3,py2lisp(type),py2lisp(val),py2lisp(tb));
    if (type) Py_DECREF(type);
    if (val) Py_DECREF(val);
    if (tb) Py_DECREF(tb);
    PyErr_Clear();
    return v;}
  else return kno_intern("MysteriousPythonError");
}

/* This is the KNO apply method for Python functions */
static lispval pyapply(lispval fcn,int n,kno_argvec args)
{
  if (KNO_PRIM_TYPEP(fcn,python_object_type)) {
    struct KNO_PYTHON_OBJECT *pyo=(struct KNO_PYTHON_OBJECT *)fcn;
    if (PyCallable_Check(pyo->pyval)) {
      lispval result; PyObject *tuple=PyTuple_New(n), *pyresult;
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
      if (pyresult) {
	Py_DECREF(tuple);
	result=py2lisp(pyresult);
	Py_DECREF(pyresult);}
      else {
	result = translate_python_error("pyapply");
	Py_DECREF(tuple);}
      PyGILState_Release(gstate);
      return result;}
    else return kno_type_error("python procedure","pyapply",fcn);}
  else return kno_type_error("python procedure","pyapply",fcn);
}

DEFC_PRIM("PY/EXEC",pyexec,MIN_ARGS(1)|MAX_ARGS(1),
	  "Executes a string of Python code",
	  {"codestring",kno_string_type,KNO_VOID});
static lispval pyexec(lispval codestring)
{
  int result;
  PyGILState_STATE gstate;
  gstate=PyGILState_Ensure();
  result=PyRun_SimpleString(KNO_STRDATA(codestring));
  PyGILState_Release(gstate);
  return KNO_INT2LISP(result);
}

DEFC_PRIM("PY/STRING",pystring,MAX_ARGS(1)|MIN_ARGS(1),
	  "Returns a string containing the printed representation "
	  "of a Python object",
	  {"pyobj",PYTHON_OBJECT_TYPE,KNO_VOID});
static lispval pystring(lispval pyobj)
{
  if (KNO_PRIM_TYPEP(pyobj,python_object_type)) {
    struct KNO_PYTHON_OBJECT *po=(kno_python_object)pyobj;
    PyObject *o=po->pyval;
    u8_string s = py2string(o);
    lispval v = knostring(s);
    u8_free(s);
    return v;}
  else return kno_err("NotAPythonObject","pystring",NULL,pyobj);
}

static int pystore(lispval map,PyObject *obj,PyObject *field,
		   int symbolize,int onerr)
{
  PyObject *pyval = PyObject_GetAttr(obj,field);
  if (pyval == NULL) {
    if (onerr == 0) {
      PyErr_Clear();
      return 0;}
    else if (onerr < 0)
      return -1;
    else NO_ELSE;}
  if (pyval == Py_None) return 0;
  lispval slot = (symbolize) ? (py2sym(field)) : (py2lisp(field));
  lispval value = (pyval) ? (py2lisp(pyval)) : (SYMBOLP(slot)) ?
    (pyerrobj(KNO_SYMBOL_NAME(slot))) : (KNO_STRINGP(slot)) ?
    (pyerrobj(KNO_CSTRING(slot))) : (kno_intern("error"));
  kno_store(map,slot,value);
  kno_decref(slot); kno_decref(value);
  if (pyval) Py_DECREF(pyval);
  return 1;
}

static lispval pytable_helper(lispval obj,int symbolize,int onerr)
{
  if (KNO_PRIM_TYPEP(obj,python_object_type)) {
    struct KNO_PYTHON_OBJECT *po=(kno_python_object)obj;
    PyObject *o=po->pyval;
    PyObject *listing = PyObject_Dir(o);
    if (listing) {
      if (!(PySequence_Check(listing)))
	return pyerr("pytable: dir() listing not sequence");
      int i=0, n=PySequence_Length(listing);
      if (n<0) return pyerr("pytable: bad dir() listing");
      lispval map = kno_empty_slotmap();
      while (i<n) {
	PyObject *pelt=PySequence_GetItem(listing,i);
	if (pystore(map,o,pelt,symbolize,onerr)<0) {
	  kno_decref(map);
	  return pyerr("pytable");}
	i++;}
      Py_DECREF(listing);
      return map;}
    else return pyerr("py/table");}
  else return kno_err("NotAPythonObject","pytable",NULL,obj);
}

DEFC_PRIM("PY/TABLE",pytable,MAX_ARGS(1)|MIN_ARGS(1),
	  "Returns a table containing the fields and values of a Python object",
	  {"pyobj",PYTHON_OBJECT_TYPE,KNO_VOID});
static lispval pytable(lispval obj)
{
  return pytable_helper(obj,1,0);
}

DEFC_PRIM("PY/TYPE",pytype,MAX_ARGS(1)|MIN_ARGS(1),
	  "Returns the Python type object for a type",
	  {"pyobj",PYTHON_OBJECT_TYPE,KNO_VOID});
static lispval pytype(lispval pyobj)
{
  if (KNO_PRIM_TYPEP(pyobj,python_object_type)) {
    struct KNO_PYTHON_OBJECT *po=(kno_python_object)pyobj;
    PyObject *o=po->pyval;
    PyObject *ptype = PyObject_Type(o);
    if ( (ptype == NULL) || (!(PyType_Check(ptype))) )
      return pyerr("py/type");
    lispval wrapped = py2lisp(ptype);
    Py_DECREF(ptype);
    return wrapped;}
  else return kno_err("NotAPythonObject","pytable",NULL,pyobj);
}

DEFC_PRIM("PY/TYPENAME",pytypename,MAX_ARGS(1)|MIN_ARGS(1),
	  "Returns name of the Python type for an object",
	  {"pyobj",PYTHON_OBJECT_TYPE,KNO_VOID});
static lispval pytypename(lispval pyobj)
{
  if (KNO_PRIM_TYPEP(pyobj,python_object_type)) {
    struct KNO_PYTHON_OBJECT *po=(kno_python_object)pyobj;
    PyObject *o=po->pyval;
    PyObject *ptype = PyObject_Type(o);
    if ( (ptype == NULL) || (!(PyType_Check(ptype))) )
      return pyerr("py/typename");
    PyTypeObject *pto = (PyTypeObject *) ptype;
    lispval result = knostring(pto->tp_name);
    Py_DECREF(ptype);
    return result;}
  else return kno_err("NotAPythonObject","pytable",NULL,pyobj);
}

DEFC_PRIM("PY/IMPORT",pyimport,MIN_ARGS(1)|MAX_ARGS(1),
	  "Returns a python module object",
	  {"modname",kno_any_type,KNO_VOID});
static lispval pyimport(lispval modname)
{
  u8_string modstring = NULL; size_t len = -1;
  if (KNO_STRINGP(modname)) {
    modstring = KNO_CSTRING(modname);
    len = KNO_STRLEN(modname);}
  else if (KNO_SYMBOLP(modname)) {
    modstring = KNO_SYMBOL_NAME(modname);
    len = strlen(modstring);}
  else return kno_err("BadPythonModName","pyimport",NULL,modname);
  PyObject *pmodulename = PyUnicode_DecodeUTF8(modstring,len,"none");
  PyObject *o = PyImport_Import(pmodulename);
  if (o) {
    lispval wrapped=py2lisp(o);
    Py_DECREF(pmodulename);
    Py_DECREF(o);
    return wrapped;}
  else {
    Py_DECREF(pmodulename);
    return pyerr("pyimport");}
}

static lispval py_use_module_evalfn
(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval modname_expr = kno_get_arg(expr,1);
  lispval imports_expr = kno_get_arg(expr,2);
  PyObject *module = NULL;
  if ( (KNO_VOIDP(modname_expr)) || (KNO_VOIDP(imports_expr)) )
    return kno_err(kno_SyntaxError,"py_use_module_evalfn",NULL,expr);
  lispval modname = kno_eval(modname_expr,env,_stack);
  if (ABORTED(modname)) return modname;
  else if (KNO_STRINGP(modname)) {
    PyObject *pmodulename=lisp2py(modname);
    module = PyImport_Import(pmodulename);
    Py_DECREF(pmodulename);}
  else if (KNO_SYMBOLP(modname)) {
    u8_string pname = KNO_SYMBOL_NAME(modname);
    size_t len = strlen(pname);
    PyObject *pmodulename = PyUnicode_DecodeUTF8(pname,len,"none");
    module = PyImport_Import(pmodulename);
    Py_DECREF(pmodulename);}
  else if (KNO_TYPEP(modname,python_object_type))
    module = lisp2py(modname);
  else return kno_err("NotModuleOrName","py_use_module_evalfn",NULL,modname);
  if (module == NULL) {
    kno_decref(modname);
    return pyerr("py/use-module");}
  else if (KNO_VOIDP(imports_expr)) {
    if (!(KNO_STRINGP(modname))) {
      lispval rv =
	kno_type_error("PythonModuleName(String)","py_use_module_evalfn",
		       modname);
      Py_DECREF(module);
      kno_decref(modname);
      return rv;}
    lispval wrapped=py2lisp(module);
    lispval sym = kno_intern(KNO_CSTRING(modname));
    int bind_rv = kno_bind_value(sym,wrapped,env);
    Py_DECREF(module);
    kno_decref(modname);
    if (bind_rv < 0) {
      return KNO_ERROR;}
    else return KNO_VOID;}
  else NO_ELSE;
  lispval imports = kno_eval(imports_expr,env,_stack);
  if (ABORTED(imports)) {
    Py_DECREF(module);
    return imports;}
  lispval result = KNO_VOID;
  KNO_DO_CHOICES(spec,imports) {
    if (KNO_STRINGP(spec)) {
      PyObject *pFunc=PyObject_GetAttrString(module,KNO_CSTRING(spec));
      if (pFunc) {
	lispval wrapped=py2lisp(pFunc);
	Py_DECREF(pFunc);
	kno_bind_value(kno_getsym(KNO_CSTRING(spec)),wrapped,env);
	kno_decref(wrapped);}
      else result = pyerr("py/use");}
    else if (KNO_SYMBOLP(spec)) {
      PyObject *pFunc=PyObject_GetAttrString(module,KNO_SYMBOL_NAME(spec));
      if (pFunc) {
	lispval wrapped=py2lisp(pFunc);
	Py_DECREF(pFunc);
	kno_bind_value(spec,wrapped,env);
	kno_decref(wrapped);}
      else result = pyerr("py/use");}
    else if (KNO_TABLEP(spec)) {
      lispval keys = kno_getkeys(spec);
      KNO_DO_CHOICES(sym,keys) {
	if (!(SYMBOLP(sym))) {
	  result = kno_type_error("symbol","py_use_module_evalfn",sym);
	  break;}
	lispval val = kno_get(spec,sym,KNO_VOID);
	if (!(STRINGP(val)))
	  result = kno_type_error("string","py_use_module_evalfn",val);
	else {
	  PyObject *pFunc=PyObject_GetAttrString(module,KNO_SYMBOL_NAME(spec));
	  if (pFunc) {
	    lispval wrapped=py2lisp(pFunc);
	    Py_DECREF(pFunc);
	    kno_bind_value(sym,wrapped,env);
	    kno_decref(wrapped);}
	  else {
	    result = pyerr("py/use");
	    break;}}
	kno_decref(val);}
	kno_decref(keys);}
    else result = kno_err("BadPythonRefMap","py_use_module_evalfn",
			  KNO_CSTRING(modname),
			  spec);
    if (KNO_ABORTED(result)) break;}
  Py_DECREF(module);
  kno_decref(imports);
  return result;
}

DEFC_PRIMN("PY/CALL",pycall,KNO_VAR_ARGS|MIN_ARGS(1),
	   "Calls a python function on some arguments");
static lispval pycall(int n,kno_argvec args)
{
  return pyapply(args[0],n-1,args+1);
}

DEFC_PRIMN("PY/HANDLE",pyhandle,KNO_VAR_ARGS|MIN_ARGS(2),
	   "Calls the method of a Python object on some arguments");
static lispval pyhandle(int n,kno_argvec lisp_args)
{
  lispval obj = lisp_args[0], method = lisp_args[1];
  PyObject *name;
  if (! (KNO_PRIM_TYPEP(obj,python_object_type)) )
    return kno_type_error("python object","pyapply",obj);
  else if (n>8)
    return kno_err(kno_TooManyArgs,"pyhandle",NULL,KNO_VOID);
  else if (KNO_STRINGP(method)) {
    name = PyUnicode_DecodeUTF8
      ((char *)KNO_STRING_DATA(method),KNO_STRING_LENGTH(method),"none");}
  else if (KNO_SYMBOLP(method)) {
    u8_string pname = KNO_SYMBOL_NAME(method);
    name = PyUnicode_DecodeUTF8((char *)pname,strlen(pname),"none");}
  else return kno_type_error("method name","pyhandle",method);
  struct KNO_PYTHON_OBJECT *pyo=(struct KNO_PYTHON_OBJECT *) obj;
  PyObject *po = pyo->pyval;
  PyObject *args[n];
  PyObject *r = NULL;
  int i = 2; while (i<n) {
    args[i] = lisp2py(lisp_args[i]); i++;}
  switch (n) {
  case 0: case 1: return kno_err("BadCall","pyhandle",NULL,KNO_VOID);
  case 2:
    r = PyObject_CallMethodObjArgs(po,name,NULL); break;
  case 3:
    r = PyObject_CallMethodObjArgs(po,name,args[2],NULL); break;
  case 4:
    r = PyObject_CallMethodObjArgs(po,name,args[2],args[3],NULL);
    break;
  case 5:
    r = PyObject_CallMethodObjArgs(po,name,args[2],args[3],args[4],NULL);
    break;
  case 6:
    r = PyObject_CallMethodObjArgs
      (po,name,args[2],args[3],args[4],args[5],NULL);
    break;
  case 7:
    r = PyObject_CallMethodObjArgs
      (po,name,args[2],args[3],args[4],args[5],args[6],NULL);
  case 8:
    r = PyObject_CallMethodObjArgs
      (po,name,args[2],args[3],args[4],args[5],args[6],
       args[7],NULL);
    break;
  default:
    ;;}
  i=2; while (i<n) { PyObject *o = args[i]; Py_DECREF(o); i++;}
  Py_DECREF(name);
  if (r == NULL)
    return translate_python_error("pyhandle");
  else {
    lispval result = py2lisp(r);
    Py_DECREF(r);
    return result;}
}

DEFC_PRIMN("PY/TRY",pytry,KNO_VAR_ARGS|MIN_ARGS(2),
	   "Calls a method on a Python object, returning {} on error");
static lispval pytry(int n,kno_argvec lisp_args)
{
  lispval obj = lisp_args[0], method = lisp_args[1];
  PyObject *name;
  if (! (KNO_PRIM_TYPEP(obj,python_object_type)) )
    return kno_type_error("python object","pyapply",obj);
  else if (n>8)
    return kno_err(kno_TooManyArgs,"pyhandle",NULL,KNO_VOID);
  else if (KNO_STRINGP(method)) {
    name = PyUnicode_DecodeUTF8
      ((char *)KNO_STRING_DATA(method),KNO_STRING_LENGTH(method),"none");}
  else if (KNO_SYMBOLP(method)) {
    u8_string pname = KNO_SYMBOL_NAME(method);
    name = PyUnicode_DecodeUTF8((char *)pname,strlen(pname),"none");}
  else return kno_type_error("method name","pyhandle",method);
  struct KNO_PYTHON_OBJECT *pyo=(struct KNO_PYTHON_OBJECT *) obj;
  PyObject *po = pyo->pyval;
  PyObject *args[n];
  PyObject *r = NULL;
  int i = 2; while (i<n) {
    args[i] = lisp2py(lisp_args[i]); i++;}
  switch (n) {
  case 0: case 1: return kno_err("BadCall","pyhandle",NULL,KNO_VOID);
  case 2:
    r = PyObject_CallMethodObjArgs(po,name,NULL); break;
  case 3:
    r = PyObject_CallMethodObjArgs(po,name,args[2],NULL); break;
  case 4:
    r = PyObject_CallMethodObjArgs(po,name,args[2],args[3],NULL);
    break;
  case 5:
    r = PyObject_CallMethodObjArgs(po,name,args[2],args[3],args[4],NULL);
    break;
  case 6:
    r = PyObject_CallMethodObjArgs
      (po,name,args[2],args[3],args[4],args[5],NULL);
    break;
  case 7:
    r = PyObject_CallMethodObjArgs
      (po,name,args[2],args[3],args[4],args[5],args[6],NULL);
  case 8:
    r = PyObject_CallMethodObjArgs
      (po,name,args[2],args[3],args[4],args[5],args[6],
       args[7],NULL);
    break;
  default:
    ;;}
  i=2; while (i<n) { PyObject *o = args[i]; Py_DECREF(o); i++;}
  Py_DECREF(name);
  if (r == NULL) {
    PyErr_Clear();
    return KNO_EMPTY;}
  else {
    lispval result = py2lisp(r);
    Py_DECREF(r);
    return result;}
}

DEFC_PRIM("PY/FCN",pyfcn,MAX_ARGS(2)|MIN_ARGS(1),
	  "Returns a python method object",
	  {"modname",kno_any_type,KNO_VOID},
	  {"fname",kno_any_type,KNO_VOID});
static lispval pyfcn(lispval modname,lispval fname)
{
  PyObject *o;
  if (KNO_STRINGP(modname)) {
    PyObject *pmodulename=lisp2py(modname);
    o = PyImport_Import(pmodulename);
    Py_DECREF(pmodulename);}
  else if (KNO_TYPEP(modname,python_object_type))
    o = lisp2py(modname);
  else return kno_err("NotModuleOrObject","pymethod",NULL,modname);
  if (o) {
    PyObject *pFunc=PyObject_GetAttrString(o,KNO_STRDATA(fname));
    Py_DECREF(o);
    if (pFunc) {
      lispval wrapped=py2lisp(pFunc);
      Py_DECREF(pFunc);
      return wrapped;}
    return pyerr("pymethod");}
  else return pyerr("pymethod");
}

DEFC_PRIM("PY/LEN",pylen,MAX_ARGS(1)|MIN_ARGS(1),
	  "eturns the length of *obj* or #f "
	  "if *obj* doesn't have a length",
	  {"pyobj",PYTHON_OBJECT_TYPE,KNO_VOID});
static lispval pylen(lispval pyobj)
{
  struct KNO_PYTHON_OBJECT *po=(kno_python_object)pyobj;
  PyObject *o=po->pyval;
  Py_ssize_t len = PyObject_Length(o);
  if (len < 0) {
    PyErr_Clear();
    return KNO_FALSE;}
  else return KNO_INT(len);
}

DEFC_PRIM("PY/DIR",pydir,MAX_ARGS(1)|MIN_ARGS(1),
	  "Returns a vector of fields on a Python object "
	  "or #f if it isn't a map/dictionary",
	  {"pyobj",PYTHON_OBJECT_TYPE,KNO_VOID});
static lispval pydir(lispval pyobj)
{
  if (KNO_PRIM_TYPEP(pyobj,python_object_type)) {
    struct KNO_PYTHON_OBJECT *po=(kno_python_object)pyobj;
    PyObject *o=po->pyval;
    PyObject *listing = PyObject_Dir(o);
    if (listing) {
      lispval r = py2lisp(listing);
      Py_DECREF(listing);
      return r;}
    else return pyerr("py/table");}
  else return kno_err("NotAPythonObject","pydir",NULL,pyobj);
}

DEFC_PRIM("PY/DIR*",pydirstar,MAX_ARGS(1)|MIN_ARGS(1),
	  "Returns a choice of fields on a Python object "
	  "or {} if it isn't a map/dictionary",
	  {"pyobj",PYTHON_OBJECT_TYPE,KNO_VOID});
static lispval pydirstar(lispval pyobj)
{
  if (KNO_PRIM_TYPEP(pyobj,python_object_type)) {
    struct KNO_PYTHON_OBJECT *po=(kno_python_object)pyobj;
    PyObject *o=po->pyval;
    PyObject *listing = PyObject_Dir(o);
    if (listing) {
      if (!(PySequence_Check(listing)))
	return pyerr("pytable: dir() listing not sequence");
      int i=0, n=PySequence_Length(listing);
      if (n<0) return pyerr("pytable: bad dir() listing");
      lispval results = KNO_EMPTY;
      while (i<n) {
	PyObject *pelt=PySequence_GetItem(listing,i);
	lispval elt=py2lisp(pelt);
	Py_DECREF(pelt);
	if (KNO_ABORTED(elt)) {
	  kno_decref(results);
	  Py_DECREF(listing);
	  return elt;}
	else {
	  KNO_ADD_TO_CHOICE(results,elt);
	  i++;}}
      Py_DECREF(listing);
      return results;}
    else return pyerr("py/dir");}
  else return KNO_EMPTY;
}

DEFC_PRIM("PY/NEXT",pynext,MAX_ARGS(2)|MIN_ARGS(1),
	  "Advances an iterator",
	  {"iterator",kno_any_type,KNO_VOID},
	  {"termval",kno_any_type,KNO_VOID});
static lispval pynext(lispval iterator,lispval termval)
{
  struct KNO_PYTHON_OBJECT *po=(kno_python_object)iterator;
  PyObject *o=po->pyval;
  if (PyIter_Check(o)) {
    PyObject *next = PyIter_Next(o);
    if (next)
      return py2lisp(next);
    else return kno_incref(termval);}
  else return kno_type_error("PythonIterator","pynext",iterator);
}

/* PyPATH configuration */

static int pypath_config_set(lispval var,lispval val,void * data)
{
  PyObject* sysPath = PySys_GetObject((char*)"path");
  if (!(KNO_STRINGP(val))) {
    kno_seterr("NotAString","pypath_config_set",NULL,val);
    return -1;}
  PyObject* dirname = lisp2py(val);
  PyList_Append(sysPath, dirname);
  Py_DECREF(dirname);
  return 1;
}

static lispval pypath_config_get(lispval var,void * data)
{
  PyObject* sysPath = PySys_GetObject((char*)"path");
  return py2lisp(sysPath);
}

/* KNO sequence handlers for python objects */

static int python_sequence_length(lispval x)
{
  if (KNO_TYPEP(x,python_object_type)) {
    struct KNO_PYTHON_OBJECT *pyo = (kno_python_object) x;
    if (PySequence_Check(pyo->pyval)) {
      Py_ssize_t len = PySequence_Length(pyo->pyval);
      if (len<0)
	return translate_python_error("python_sequence_length");
      else return len;}
    else return kno_type_error("python sequence","python_sequence_length",x);}
  else return kno_type_error("python object","python_sequence_length",x);
}

static lispval python_sequence_elt(lispval x,int i)
{
  if (KNO_TYPEP(x,python_object_type)) {
    struct KNO_PYTHON_OBJECT *pyo = (kno_python_object) x;
    if (PySequence_Check(pyo->pyval)) {
      Py_ssize_t len = PySequence_Length(pyo->pyval);
      if (len<0)
	return translate_python_error("python_sequence_elt");
      else if (i >= len) {
	u8_byte buf[64];
	return kno_err(kno_RangeError,"python_sequence_elt",
		       u8_bprintf(buf,"%d",i),x);}
      else {
	PyObject *item = PySequence_GetItem(pyo->pyval,(Py_ssize_t)i);
	return py2lisp(item);}}
    else return kno_type_error("python sequence","python_sequence_elt",x);}
  else return kno_type_error("python object","python_sequence_elt",x);
}

static lispval python_sequence_slice(lispval x,int start,int end)
{
  if (KNO_TYPEP(x,python_object_type)) {
    struct KNO_PYTHON_OBJECT *pyo = (kno_python_object) x;
    u8_byte numbuf[64];
    if (PySequence_Check(pyo->pyval)) {
      Py_ssize_t len = PySequence_Length(pyo->pyval);
      if (len<0)
	return translate_python_error("python_sequence_slice");
      else {
	Py_ssize_t use_start = (start<0) ? (len+start) : (start);
	Py_ssize_t use_end = (end<0) ? (len+end) : (end);
	if ( (use_end >= len) || (use_end < 0) ) {
	  return kno_err(kno_RangeError,"python_sequence_slice",
			 u8_bprintf(numbuf,"%d",start),x);}
	else if ( (use_start >= len) || (use_start < 0) ) {
	  return kno_err(kno_RangeError,"python_sequence_slice",
			 u8_bprintf(numbuf,"%d",start),x);}
	else {
	  PyObject *item = PySequence_GetSlice
	    (pyo->pyval,(Py_ssize_t)use_start,(Py_ssize_t)use_end);
	  lispval result = py2lisp(item);
	  Py_DECREF(item);
	  return result;}}}
    else return kno_type_error("python sequence","python_sequence_slice",x);}
  else return kno_type_error("python object","python_sequence_slice",x);
}

static int python_sequencep(lispval x)
{
  if (KNO_TYPEP(x,python_object_type)) {
    struct KNO_PYTHON_OBJECT *pyo = (kno_python_object) x;
    if (PySequence_Check(pyo->pyval))
      return 1;
    else return 0;}
  else return 0;
}

static struct KNO_SEQFNS python_sequence_fns = {
  python_sequence_length, /* int (*len)(lispval x); */
  python_sequence_elt, /* lispval (*elt)(lispval x,int i); */
  python_sequence_slice, /* lispval (*slice)(lispval x,int i,int j); */
  NULL, /* int (*position)(lispval key,lispval x,int i,int j); */
  NULL, /* int (*search)(lispval key,lispval x,int i,int j); */
  NULL, /* lispval *(*elts)(lispval x,int *); */
  NULL, /* lispval (*make)(int,kno_argvec); */
  python_sequencep, /* int (*sequencep)(lispval); */
};

/* KNO table handlers for python objects */

static lispval python_table_get(lispval x,lispval key,lispval dflt)
{
  if (KNO_TYPEP(x,python_object_type)) {
    struct KNO_PYTHON_OBJECT *pyo = (kno_python_object) x;
    if (PyMapping_Check(pyo->pyval)) {
      u8_string keystring = (KNO_SYMBOLP(key)) ? (KNO_SYMBOL_NAME(key)) :
	(KNO_STRINGP(key)) ? (KNO_CSTRING(key)) : (NULL);
      if (keystring == NULL)
	return kno_type_error("python mapping key","python_table_get",key);
      else {
	PyObject *pyresult =
	  PyMapping_GetItemString(pyo->pyval,(char *)keystring);
	if (pyresult == NULL)
	  return kno_incref(dflt);
	else {
	  lispval result = py2lisp(pyresult);
	  Py_DECREF(pyresult);
	  return result;}}}
    else return kno_type_error("python mapping object","python_table_get",x);}
  else return kno_type_error("python object","python_table_get",x);
}

static int python_table_store(lispval x,lispval key,lispval value)
{
  if (KNO_TYPEP(x,python_object_type)) {
    struct KNO_PYTHON_OBJECT *pyo = (kno_python_object) x;
    if (PyMapping_Check(pyo->pyval)) {
      u8_string keystring = (KNO_SYMBOLP(key)) ? (KNO_SYMBOL_NAME(key)) :
	(KNO_STRINGP(key)) ? (KNO_CSTRING(key)) : (NULL);
      if (keystring == NULL)
	return kno_type_error("python mapping key","python_table_get",key);
      else {
	PyObject *v = lisp2py(value);
	int rv = PyMapping_SetItemString(pyo->pyval,(char *)keystring,v);
	Py_DECREF(v);
	if (rv<0)
	  return translate_python_error("python_table_store");
	else return KNO_TRUE;}}
    else return kno_type_error("python mapping object","python_table_store",x);}
  else return kno_type_error("python object","python_table_store",x);
}

static int python_table_getsize(lispval x)
{
  if (KNO_TYPEP(x,python_object_type)) {
    struct KNO_PYTHON_OBJECT *pyo = (kno_python_object) x;
    if (PyMapping_Check(pyo->pyval)) {
      Py_ssize_t sz = PyMapping_Size(pyo->pyval);
      if (sz < 0) {
	translate_python_error("python_table_getsize");
	return -1;}
      else return (int) sz;}
    else {
      kno_type_error("PythonMapping","python_table_getsize",x);
      return -1;}}
  else {
    kno_type_error("PythonObject","python_table_getsize",x);
    return -1;}
}

static lispval python_table_getkeys(lispval x)
{
  if (KNO_TYPEP(x,python_object_type)) {
    struct KNO_PYTHON_OBJECT *pyo = (kno_python_object) x;
    if (PyMapping_Check(pyo->pyval)) {
      PyObject *keys = PyMapping_Keys(pyo->pyval);
      if (keys == NULL)
	return translate_python_error("python_table_getkeys");
      else return PySequence2Choice(keys,1);}
    else return kno_type_error("PythonMap","python_table_getkeys",x);}
  else return kno_type_error("PythonObject","python_table_getkeys",x);
}

static int python_tablep(lispval x)
{
  if (KNO_TYPEP(x,python_object_type)) {
    struct KNO_PYTHON_OBJECT *pyo = (kno_python_object) x;
    if (PyMapping_Check(pyo->pyval))
      return 1;
    else return 0;}
  else return 0;
}

static struct KNO_TABLEFNS python_table_fns = {
  python_table_get, /* lispval (*get)(lispval obj,lispval kno_key,lispval dflt) */
  python_table_store, /* int (*store)(lispval obj,lispval kno_key,lispval value) */
  NULL, /* int (*add)(lispval obj,lispval kno_key,lispval value) */
  NULL, /* int (*drop)(lispval obj,lispval kno_key,lispval value) */
  NULL, /* int (*test)(lispval obj,lispval kno_key,lispval value) */
  NULL, /* int (*readonly)(lispval obj,int op) */
  NULL, /* int (*modified)(lispval obj,int op) */
  NULL, /* int (*finished)(lispval obj,int op) */
  python_table_getsize, /* int (*getsize)(lispval obj) */
  python_table_getkeys, /* lispval (*keys)(lispval obj) */
  NULL, /* lispval *(*keyvec)(lispval obj,int *len) */
  NULL, /* struct KNO_KEYVAL (*keyvals)(lispval obj,int *) */
  python_tablep /* int (*tablep)(lispval obj) */
};

/* Initializations */

static lispval pymodule=KNO_VOID;
static int python_init_done=0;

static void init_kno_module()
{
  if (!(KNO_VOIDP(pymodule))) return;
#if (!(HAVE_CONSTRUCTOR_ATTRIBUTES))
  u8_initialize_u8stdio();
  u8_init_chardata_c();
#endif

  /* Initialize builtin scheme modules.
     These include all modules specified by (e.g.):
     configure --enable-fdweb */
#if ((HAVE_CONSTRUCTOR_ATTRIBUTES) && (!(KNO_TESTCONFIG)))
  KNO_INIT_SCHEME_BUILTINS();
#else
  /* If we're a "test" executable (KNO_TESTCONFIG), we're
     statically linked, so we need to initialize some modules
     explicitly (since the "onload" initializers may not be invoked). */
  kno_init_texttools();
  kno_init_fdweb();
#endif
#if PY_MAJOR_VERSION >= 3
  pymodule=kno_new_module("PARSELTONGUE",0);
#else
  pymodule=kno_new_module("PARSELTONGUE2",0);
#endif
  python_object_type=kno_register_cons_type("python",PYTHON_OBJECT_TYPE);
  default_env=kno_working_lexenv();
  kno_recyclers[python_object_type]=recycle_python_object;
  kno_unparsers[python_object_type]=unparse_python_object;

  kno_applyfns[python_object_type]=pyapply;

  kno_seqfns[python_object_type] = &python_sequence_fns;
  kno_tablefns[python_object_type] = &python_table_fns;

  link_local_cprims();

  kno_def_evalfn(pymodule,"PY/USE-MODULE",py_use_module_evalfn,
		 "imports bindings "
		 "from a Python module to the current environment (which "
		 "should be a Scheme module's top level environments). "
		 "Each *spec* can be a symbol, a string, or a table. "
		 "Symbols use their pname to find the corresponding "
		 "Python function; strings are lowercased to generate "
		 "symbol names. If *spec* is a table, it should map "
		 "symbols to Python names.");

  kno_finish_cmodule(pymodule);

  kno_register_config("PYPATH","The search path used by Python",
		     pypath_config_get,pypath_config_set,NULL);

  /* Try to handle a virtual env declarations */
  lispval virtual_env = kno_config_get("PYTHON:VENV");
  if (KNO_STRINGP(virtual_env)) {
    PyObject* vinfo = PySys_GetObject((char*)"version_info");
    Py_ssize_t major = -1, minor = -1;
    u8_setenv("VIRTUAL_ENV",KNO_CSTRING(virtual_env),1);
    u8_string curpath = u8_getenv("PATH");
    u8_string vbin_path = u8_mkpath(KNO_CSTRING(virtual_env),"bin");
    u8_string new_path = u8_string_append(vbin_path,":",curpath,NULL);
    u8_setenv("PATH",new_path,1);
    u8_free(new_path); u8_free(vbin_path); u8_free(curpath);
    if (vinfo) {
      PyObject *major_val = PyObject_GetAttrString(vinfo,"major");
      PyObject *minor_val = PyObject_GetAttrString(vinfo,"minor");
      if ( (major_val) && (PyNumber_Check(major_val)) )
	major = PyNumber_AsSsize_t(major_val,NULL);
      if ( (minor_val) && (PyNumber_Check(minor_val)) )
	minor = PyNumber_AsSsize_t(minor_val,NULL);
      Py_DECREF(major_val); Py_DECREF(minor_val);
      if ( (major>1) && (minor>=0) ) {
	u8_string mod_subdir = u8_mkstring("lib/python%d.%d/site-packages/",major,minor);
	u8_string path_entry = u8_mkpath(KNO_CSTRING(virtual_env),mod_subdir);
	lispval add_path = knostring(path_entry);
	kno_set_config("PYPATH",add_path);
	kno_decref(add_path);
	u8_free(mod_subdir);
	u8_free(path_entry);}
      else {
	u8_log(LOGERR,"BadPythonVersionInfo",
	       "Invalid version info in sys.version_info, not setting PYPATH "
	       "for %s",KNO_CSTRING(virtual_env));}
      Py_DECREF(vinfo);}
    else u8_log(LOGERR,"NoPythonVersionInfo",
		"No version info sys.version_info to set PYPATH");}
  kno_decref(virtual_env);
}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("PY/EXEC",pyexec,1,pymodule);
  KNO_LINK_CPRIM("PY/IMPORT",pyimport,1,pymodule);
  KNO_LINK_CPRIMN("PY/CALL",pycall,pymodule);
  KNO_LINK_CPRIMN("PY/HANDLE",pyhandle,pymodule);
  KNO_LINK_CPRIMN("PY/TRY",pytry,pymodule);

#if 0
  KNO_LINK_TYPED("PY/FCN",pyfcn,2,pymodule,
		 "module",kno_any_type,KNO_VOID,
		 "name",kno_string_type,KNO_VOID);
  KNO_LINK_TYPED("PY/NEXT",pynext,2,pymodule,
		 "iterator",python_object_type,KNO_VOID,
		 "termval",kno_any_type,KNO_VOID);
  KNO_LINK_TYPED("PY/LEN",pylen,1,pymodule,
		 "pseq",python_object_type,KNO_VOID);
  KNO_LINK_TYPED("PY/STRING",pystring,1,pymodule,
		 "pyobj",python_object_type,KNO_VOID);
  KNO_LINK_TYPED("PY/TYPE",pytype,1,pymodule,
		 "pyobj",python_object_type,KNO_VOID);
  KNO_LINK_TYPED("PY/DIR",pydir,1,pymodule,
		 "pyobj",python_object_type,KNO_VOID);
  KNO_LINK_TYPED("PY/TYPENAME",pytypename,1,pymodule,
		 "pyobj",python_object_type,KNO_VOID);
  KNO_LINK_TYPED("PY/DIR*",pydirstar,1,pymodule,
		 "pyobj",python_object_type,KNO_VOID);
  KNO_LINK_TYPED("PY/TABLE",pytable,1,pymodule,
		 "pyobj",python_object_type,KNO_VOID);
  KNO_LINK_TYPED("PY/GET",pyget,2,pymodule,
		 "pyobj",python_object_type,KNO_VOID,
		 "key",kno_any_type,KNO_VOID);
  KNO_LINK_TYPED("PY/HAS",pyhas,2,pymodule,
		 "pyobj",python_object_type,KNO_VOID,
		 "key",kno_any_type,KNO_VOID);
#endif

  KNO_LINK_CPRIM("PY/FCN",pyfcn,2,pymodule);
  KNO_LINK_CPRIM("PY/NEXT",pynext,2,pymodule);
  KNO_LINK_CPRIM("PY/LEN",pylen,1,pymodule);
  KNO_LINK_CPRIM("PY/STRING",pystring,1,pymodule);
  KNO_LINK_CPRIM("PY/TYPE",pytype,1,pymodule);
  KNO_LINK_CPRIM("PY/DIR",pydir,1,pymodule);
  KNO_LINK_CPRIM("PY/TYPENAME",pytypename,1,pymodule);
  KNO_LINK_CPRIM("PY/DIR*",pydirstar,1,pymodule);
  KNO_LINK_CPRIM("PY/TABLE",pytable,1,pymodule);
  KNO_LINK_CPRIM("PY/GET",pyget,2,pymodule);
  KNO_LINK_CPRIM("PY/HAS",pyhas,2,pymodule);

}

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef kno_mod = {
  PyModuleDef_HEAD_INIT,
  "parseltongue",
  "Provides a bridge between KNO and Python",
  -1,
  kno_methods,
  NULL,
  NULL,
  NULL,
  NULL};

static PyObject *python_kno_module = NULL;

PyObject *init_python_module()
{
  if (python_kno_module)
    return python_kno_module;
  PyObject *m = PyModule_Create(&kno_mod);
  kno_error=PyErr_NewException("kno.error",NULL,NULL);
  Py_INCREF(kno_error);
  PyModule_AddObject(m,"error",kno_error);
  python_init_done=1;
  python_kno_module=m;
  return m;
}
#else
void init_python_module()
{
  PyObject *m;
  if (python_init_done) return;
  m=Py_InitModule("kno",kno_methods);
  kno_error=PyErr_NewException("kno.error",NULL,NULL);
  Py_INCREF(kno_error);
  PyModule_AddObject(m,"error",kno_error);
  python_init_done=1;
}
#endif

#if PY_MAJOR_VERSION >= 3
static wchar_t *py_argv[256]={L"",NULL};
static int py_argc=1;
#else
static char *py_argv[256]={"",NULL};
static int py_argc=1;
#endif

static void setup_rich_compare_methods()
{
#if PY_MAJOR_VERSION >= 3
  KnoType.tp_richcompare = pylisp_compare;
  PoolType.tp_richcompare = pool_compare;
  IndexType.tp_richcompare = index_compare;
#endif
}

void kno_init_parseltongue()
{
  if (!(Py_IsInitialized())) Py_Initialize();
  PySys_SetArgvEx(py_argc,py_argv,0);
  // fprintf(stderr,"Setup compare methods\n");
  setup_rich_compare_methods();
  // fprintf(stderr,"Init kno module\n");
  init_kno_module();
  // fprintf(stderr,"Init python module\n");
}

#if PY_MAJOR_VERSION >= 3
PyMODINIT_FUNC PyInit_kno(void)
{
  kno_init_parseltongue();
  return init_python_module();
}
#else
PyMODINIT_FUNC initparseltongue(void)
{
  kno_init_parseltongue();
}
#endif


