/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include <Python.h>
#include <structmember.h>
#include <framerd/ptr.h>
#include <framerd/cons.h>
#include <framerd/storage.h>
#include <framerd/pools.h>
#include <framerd/indexes.h>
#include <framerd/eval.h>
#include <framerd/numbers.h>
#include <framerd/bigints.h>
#include <framerd/fdweb.h>

#include <libu8/libu8.h>
#include <libu8/u8strings.h>
#include <libu8/u8streamio.h>
#include <libu8/u8printf.h>

typedef struct FD_PYTHON_WRAPPER {
  PyObject_HEAD /* Header stuff */
  lispval lval;} pylisp;

typedef struct FD_PYTHON_POOL {
  PyObject_HEAD /* Header stuff */
  fd_pool pool;} pypool;

typedef struct FD_PYTHON_INDEX {
  PyObject_HEAD /* Header stuff */
  fd_index index;} pyindex;

typedef struct FD_PYTHON_FUNCTION {
  PyObject_HEAD /* Header stuff */
  lispval fnval;} pyfunction;

typedef struct FD_PYTHON_OBJECT {
  FD_CONS_HEADER; /* Header stuff */
  PyObject *pyval;} FD_PYTHON_OBJECT;
typedef struct FD_PYTHON_OBJECT *fd_python_object;
static fd_ptr_type python_object_type;
static fd_lexenv default_env;

static lispval py2lisp(PyObject *o);
static lispval py2lispx(PyObject *o);
static PyObject *lisp2py(lispval x);
static u8_string py2string(PyObject *o);

staticforward PyTypeObject ChoiceType;
staticforward PyTypeObject OIDType;
staticforward PyTypeObject FramerDType;
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

/* FramerD to Python errors */

static PyObject *framerd_error;

static PyObject *pass_error()
{
  u8_condition ex; u8_context cxt; u8_string details; lispval irritant;
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

static lispval translate_python_error(u8_context cxt)
{
  if (PyErr_Occurred()) {
    PyObject *type, *value, *stack;
    PyErr_Fetch(&type,&value,&stack);
    PyErr_Clear();
    if (cxt == NULL) cxt = "py2lisp";
    lispval etype = py2lisp(type), evalue = py2lisp(value);
    lispval estack = (stack) ? (py2lisp(stack)) : (FD_EMPTY_LIST);
    lispval evec = (stack) ?
      (fd_make_nvector(3,etype,evalue,estack)) :
      (fd_make_nvector(2,etype,evalue));
    u8_string details = py2string(type);
    fd_seterr("PythonError",cxt,details,evec);
    if (details) u8_free(details);
    fd_decref(evec);
    if (type) Py_DECREF(type);
    if (value) Py_DECREF(value);
    if (stack) Py_DECREF(stack);
    return FD_ERROR_VALUE;}
  else return FD_VOID;
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
  self->lval=FD_VOID;
  return self;                /* a new type-instance object */
}

static pylisp *             /* on "x = stacktype.Stack()" */
newpychoice()                 /* instance constructor function */
{                                /* these don't get an 'args' input */
  pylisp *self;
  self = PyObject_New(pylisp, &ChoiceType);  /* malloc, init, incref */
  if (self == NULL)
    return NULL;            /* raise exception */
  self->lval=FD_VOID;
  return self;                /* a new type-instance object */
}

static pylisp *             /* on "x = stacktype.Stack()" */
newpyoid()                 /* instance constructor function */
{                                /* these don't get an 'args' input */
  pylisp *self;
  self = PyObject_New(pylisp, &OIDType);  /* malloc, init, incref */
  if (self == NULL)
    return NULL;            /* raise exception */
  self->lval=FD_VOID;
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
  fd_incref(fn);
  return self;                /* a new type-instance object */
}

static void                     /* instance destructor function */
pylisp_dealloc(self)             /* when reference-count reaches zero */
    pylisp *self;
{                               /* do cleanup activity */
  fd_decref(self->lval);
  PyObject_Del(self);            /* same as 'free(self)' */
}

static int
pylisp_compare(v, w)
     pylisp *v, *w;
{
  return (FD_FULL_COMPARE(v->lval,w->lval));
}

static PyObject *
pylisp_apply(PyObject *self,PyObject *args,PyObject *kwargs)
{
  PyObject *pyresult;
  if (PyTuple_Check(args)) {
    lispval fn = py2lisp(self), result = FD_VOID;
    if (! (FD_APPLICABLEP(fn)) ) {
      u8_string s = fd_lisp2string(fn);
      PyErr_Format(PyExc_TypeError,"Not applicable: %s",s);
      u8_free(s);
      fd_decref(fn);
      return NULL;}
    int i=0, size=PyTuple_GET_SIZE(args);
    if (size == 0)
      result = fd_apply(fn,0,NULL);
    else {
      lispval argv[size];
      while (i<size) {
	PyObject *arg=PyTuple_GET_ITEM(args,i);
	argv[i]=py2lisp(arg);
	i++;}
      Py_BEGIN_ALLOW_THREADS {
	result=fd_apply(fn,size,argv);}
      Py_END_ALLOW_THREADS;
      fd_decref_vec(argv,size);}
    pyresult=lisp2py(result);
    return pyresult;}
  else return NULL;
}

static void recycle_python_object(struct FD_RAW_CONS *obj)
{
  struct FD_PYTHON_OBJECT *po=(struct FD_PYTHON_OBJECT *)obj;
  Py_DECREF(po->pyval); u8_free(po);
}

static int unparse_python_object(u8_output out,lispval obj)
{
  struct FD_PYTHON_OBJECT *pyo = (struct FD_PYTHON_OBJECT *)obj;
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
    return FD_VOID;
  else if (o==Py_False)
    return FD_FALSE;
  else if (o==Py_True)
    return FD_TRUE;
  else if (PyInt_Check(o))
    return FD_INT2DTYPE(PyInt_AS_LONG(o));
  else if (PyFloat_Check(o))
    return fd_init_double(NULL,PyFloat_AsDouble(o));
  else if (PyLong_Check(o)) {
    int bitlen=_PyLong_NumBits(o);
    int bytelen=bitlen/8+1;
    int sign=_PyLong_Sign(o);
    if (bitlen<32) {
      long lval=PyLong_AsLong(o);
      return FD_INT2DTYPE(lval);}
    else if (bitlen<64) {
      long long lval=PyLong_AsLongLong(o);
      return FD_INT2DTYPE(lval);}
    else {
      PyLongObject *plo=(PyLongObject *)o;
      unsigned char bytes[bytelen];
      int retval =_PyLong_AsByteArray(plo,bytes,bytelen,1,1);
      if (retval < 0)
	return fd_err("PythonLongError","py2lisp/long",NULL,FD_VOID);
      if (sign<0) { negate_bigint(bytes,bytelen);}
      return (lispval)
	fd_digit_stream_to_bigint(bytelen,(bigint_producer)read_bigint_byte,
				  (void *)bytes,256,(sign<0));}}
  else if (PyString_Check(o)) {
    PyObject *u8=PyString_AsEncodedObject(o,"utf8","none");
    if (u8) {
      lispval v = lispval_string((u8_string)PyString_AS_STRING(u8));
      Py_DECREF(u8);
      return v;}
    else return fd_err("InvalidPythonString","py2lisp",NULL,FD_VOID);}
  else if (PyUnicode_Check(o)) {
    PyObject *u8=PyUnicode_AsEncodedString(o,"utf8","none");
    if (u8) {
      lispval v = lispval_string((u8_string)PyString_AS_STRING(u8));
      Py_DECREF(u8);
      return v;}
    else return fd_err("InvalidPythonString","py2lisp",NULL,FD_VOID);}
  else if (PyTuple_Check(o)) {
    int i=0, n=PyTuple_Size(o);
    lispval *data=u8_alloc_n(n,lispval);
    while (i<n) {
      PyObject *pelt=PyTuple_GetItem(o,i);
      lispval elt=py2lisp(pelt);
      data[i]=elt;
      i++;}
    return fd_init_code(NULL,n,data);}
  else if (PyList_Check(o)) {
    int i=0, n=PyList_Size(o);
    lispval *data=u8_alloc_n(n,lispval);
    while (i<n) {
      PyObject *pelt=PyList_GetItem(o,i);
      lispval elt=py2lisp(pelt);
      data[i]=elt;
      i++;}
    return fd_init_vector(NULL,n,data);}
  else if ((PyObject_TypeCheck(o,&PoolType))) {
    struct FD_PYTHON_POOL *pp=(struct FD_PYTHON_POOL *)o;
    return fd_pool2lisp(pp->pool);}
  else if ((PyObject_TypeCheck(o,&ApplicableType))) {
    struct FD_PYTHON_FUNCTION *pf=(struct FD_PYTHON_FUNCTION *)o;
    return fd_incref(pf->fnval);}
  else if ((PyObject_TypeCheck(o,&IndexType))) {
    struct FD_PYTHON_INDEX *pi=(struct FD_PYTHON_INDEX *)o;
    return fd_index2lisp(pi->index);}
  else if ((PyObject_TypeCheck(o,&FramerDType)) ||
	   (PyObject_TypeCheck(o,&ChoiceType)) ||
	   (PyObject_TypeCheck(o,&OIDType))) {
    pylisp *v=(struct FD_PYTHON_WRAPPER *)o;
    return fd_incref(v->lval);}
  else {
    struct FD_PYTHON_OBJECT *pyo=u8_alloc(struct FD_PYTHON_OBJECT);
    FD_INIT_CONS(pyo,python_object_type);
    pyo->pyval=o; Py_INCREF(o);
    return (lispval) pyo;}
}

static u8_string py2string(PyObject *o)
{
  int free_temp = 0;
  if (! ( (PyString_Check(o)) || (PyUnicode_Check(o)) ) ) {
    o = PyObject_Str(o);
    free_temp = 1;}

  if (PyString_Check(o)) {
    PyObject *u8=PyString_AsEncodedObject(o,"utf8","none");
    if (free_temp) Py_DECREF(o);
    if (u8) {
      u8_string s = u8_strdup((u8_string)PyString_AS_STRING(u8));
      Py_DECREF(u8);
      return s;}
    else {
      fd_seterr("InvalidPythonString","py2string",NULL,FD_VOID);
      return NULL;}}
  else if (PyUnicode_Check(o)) {
    PyObject *u8=PyUnicode_AsEncodedString(o,"utf8","none");
    if (free_temp) Py_DECREF(o);
    if (u8) {
      u8_string s = u8_strdup((u8_string)PyString_AS_STRING(u8));
      Py_DECREF(u8);
      return s;}
    else {
      fd_seterr("InvalidPythonString","py2string",NULL,FD_VOID);
      return NULL;}}
  else {
    if (free_temp) Py_DECREF(o);
    fd_seterr("InvalidPythonString","py2string",NULL,FD_VOID);
    return NULL;}
}

/* This parses string args and is used for calling functions through
   Python. */
static lispval py2lispx(PyObject *o)
{
  if (o==Py_None)
    return FD_VOID;
  else if (o==Py_False)
    return FD_FALSE;
  else if (o==Py_True)
    return FD_TRUE;
  else if (PyInt_Check(o))
    return FD_INT2DTYPE(PyInt_AS_LONG(o));
  else if (PyFloat_Check(o))
    return fd_init_double(NULL,PyFloat_AsDouble(o));
  else if (PyLong_Check(o)) {
    int bitlen=_PyLong_NumBits(o);
    int bytelen=bitlen/8+1;
    int sign=_PyLong_Sign(o);
    if (bitlen<32) {
      long lval=PyLong_AsLong(o);
      return FD_INT2DTYPE(lval);}
    else if (bitlen<64) {
      long long lval=PyLong_AsLongLong(o);
      return FD_INT2DTYPE(lval);}
    else {
      PyLongObject *plo=(PyLongObject *)o;
      unsigned char bytes[bytelen];
      int retval =_PyLong_AsByteArray(plo,bytes,bytelen,1,1);
      if (retval<0)
	return fd_err("PythonLongError","py2lispx/long",NULL,FD_VOID);
      if (sign<0) { negate_bigint(bytes,bytelen);}
      return (lispval) fd_digit_stream_to_bigint(bytelen,(bigint_producer)read_bigint_byte,
						 (void *)bytes,256,(sign<0));}}
  else if (PyString_Check(o)) {
    PyObject *u8=PyString_AsEncodedObject(o,"utf8","none");
    u8_string sdata=(u8_string)PyString_AS_STRING(u8);
    lispval v=((*sdata==':') ? (fd_parse(sdata+1)) : (fd_parse(sdata)));
    Py_DECREF(u8);
    return v;}
  else if (PyUnicode_Check(o)) {
    PyObject *u8=PyUnicode_AsEncodedString(o,"utf8","none");
    lispval v=fd_parse((u8_string)PyString_AS_STRING(u8));
    Py_DECREF(u8);
    return v;}
  else if (PyTuple_Check(o)) {
    int i=0, n=PyTuple_Size(o);
    lispval *data=u8_alloc_n(n,lispval);
    while (i<n) {
      PyObject *pelt=PyTuple_GetItem(o,i);
      lispval elt=py2lispx(pelt);
      data[i]=elt;
      i++;}
    return fd_init_vector(NULL,n,data);}
  else if ((PyObject_TypeCheck(o,&PoolType))) {
    struct FD_PYTHON_POOL *pp=(struct FD_PYTHON_POOL *)o;
    return fd_pool2lisp(pp->pool);}
  else if ((PyObject_TypeCheck(o,&IndexType))) {
    struct FD_PYTHON_INDEX *pi=(struct FD_PYTHON_INDEX *)o;
    return fd_index2lisp(pi->index);}
  else if ((PyObject_TypeCheck(o,&FramerDType)) ||
	   (PyObject_TypeCheck(o,&ChoiceType)) ||
	   (PyObject_TypeCheck(o,&OIDType))) {
    pylisp *v=(struct FD_PYTHON_WRAPPER *)o;
    return fd_incref(v->lval);}
  else return FD_VOID;
}

static void output_bigint_byte(unsigned char **scan,int digit)
{
  **scan=digit; (*scan)++;
}

static PyObject *lisp2py(lispval o)
{
  if (FD_FIXNUMP(o)) {
    long lval=FD_FIX2INT(o);
    return PyInt_FromLong(lval);}
  else if (FD_IMMEDIATEP(o))
    if (FD_VOIDP(o)) Py_RETURN_NONE;
    else if (FD_TRUEP(o)) Py_RETURN_TRUE;
    else if (FD_FALSEP(o)) Py_RETURN_FALSE;
    else if (FD_EMPTY_CHOICEP(o)) {
      pylisp *po=newpychoice();
      po->lval=FD_EMPTY_CHOICE;
      return (PyObject *)po;}
    else {
      pylisp *po=newpylisp();
      po->lval=fd_incref(o);
      return (PyObject *)po;}
  else if (FD_STRINGP(o))
    return PyUnicode_DecodeUTF8
      ((char *)FD_STRING_DATA(o),FD_STRING_LENGTH(o),"none");
  else if (FD_PRIM_TYPEP(o,python_object_type)) {
    struct FD_PYTHON_OBJECT *fdpo=(fd_python_object)o;
    PyObject *o=fdpo->pyval;
    Py_INCREF(o);
    return o;}
  else if (FD_BIGINTP(o)) {
    fd_bigint big=(fd_bigint) o;
    if (fd_small_bigintp(big)) {
      long lval=fd_bigint_to_long(big);
      return PyInt_FromLong(lval);}
    else if (fd_modest_bigintp(big)) {
      long long llval=fd_bigint_to_long_long(big);
      return PyLong_FromLongLong(llval);}
    else {
      PyObject *pylong;
      int n_bytes=fd_bigint_length_in_bytes(big);
      int negativep=fd_bigint_negativep(big);
      unsigned char _bytes[64], *bytes, *scan;
      if (n_bytes>=63) scan=bytes=u8_malloc(n_bytes+1);
      else scan=bytes=_bytes;
      fd_bigint_to_digit_stream
	(big,256,output_bigint_byte,(void *)&scan);
      if (negativep) n_bytes=negate_bigint(bytes,n_bytes);
      pylong=_PyLong_FromByteArray(bytes,n_bytes,1,1);
      if (bytes!=_bytes) u8_free(bytes);
      return pylong;}}
  else if (FD_FLONUMP(o)) {
    double dval=FD_FLONUM(o);
    return PyFloat_FromDouble(dval);}
  else if (FD_PRECHOICEP(o)) {
    lispval simplified = fd_make_simple_choice(o);
    if (FD_CHOICEP(simplified)) {
      pylisp *po=newpychoice();
      po->lval=simplified;
      return (PyObject *)po;}
    else {
      PyObject *po = lisp2py(simplified);
      fd_decref(simplified);
      return po;}}
  else if (FD_CHOICEP(o)) {
    pylisp *po=newpychoice();
    po->lval=fd_incref(o);
    return (PyObject *)po;}
  else if (FD_OIDP(o)) {
    pylisp *po=newpyoid();
    po->lval=fd_incref(o);
    return (PyObject *)po;}
  else if (FD_VECTORP(o)) {
    int i=0, n=FD_VECTOR_LENGTH(o);
    lispval *data=FD_VECTOR_DATA(o);
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
  else if (FD_ABORTP(o))
    return pass_error();
  else if (FD_APPLICABLEP(o)) {
    pylisp *po=newpyapply(o);
    return (PyObject *)po;}
  else {
    pylisp *po=newpylisp();
    po->lval=fd_incref(o);
    return (PyObject *)po;}
}

/* Printed representation */

#define PyUTF8String(s,len) PyUnicode_DecodeUTF8((char *)(s),len,"none")

static PyObject *pylisp_str(PyObject *self)
{
  struct FD_PYTHON_WRAPPER *pw=(struct FD_PYTHON_WRAPPER *)self;
  struct U8_OUTPUT out; u8_byte buf[64];
  PyObject *pystring;
  U8_INIT_OUTPUT_X(&out,32,buf,U8_OUTPUT_STREAM);
  u8_printf(&out,"framerd.ref('%q')",pw->lval);
  pystring=PyUTF8String(out.u8_outbuf,out.u8_write-out.u8_outbuf);
  if (out.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(out.u8_outbuf);
  return pystring;
}

static PyObject *choice_str(PyObject *self)
{
  struct FD_PYTHON_WRAPPER *pw=(struct FD_PYTHON_WRAPPER *)self;
  struct U8_OUTPUT out; u8_byte buf[64];
  PyObject *pystring; int i=0;
  U8_INIT_OUTPUT_X(&out,32,buf,U8_OUTPUT_STREAM);
  u8_puts(&out,"framerd.Choice(");
  {FD_DO_CHOICES(elt,pw->lval) {
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
  pystring=PyUTF8String(out.u8_outbuf,out.u8_write-out.u8_outbuf);
  if (out.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(out.u8_outbuf);
  return pystring;
}

static PyObject *oid_str(PyObject *self)
{
  struct FD_PYTHON_WRAPPER *pw=(struct FD_PYTHON_WRAPPER *)self;
  struct U8_OUTPUT out; u8_byte buf[64];
  PyObject *pystring; int i=0;
  U8_INIT_OUTPUT_X(&out,32,buf,U8_OUTPUT_STREAM);
  u8_puts(&out,"framerd.OID(");
  {FD_DO_CHOICES(elt,pw->lval) {
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
  pystring=PyUTF8String(out.u8_outbuf,out.u8_write-out.u8_outbuf);
  if (out.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(out.u8_outbuf);
  return pystring;
}

static PyObject *pylisp_get(PyObject *self,PyObject *arg)
{
  lispval frames=py2lisp(self), slotids=py2lispx(arg), value=FD_EMPTY_CHOICE;
  if ((FD_CHOICEP(frames)) && (FD_FIXNUMP(slotids))) {
    PyObject *elt=lisp2py((FD_CHOICE_DATA(frames))[FD_FIX2INT(slotids)]);
    fd_decref(frames);
    return elt;}
  else {
    PyObject *result;
    FD_DO_CHOICES(frame,frames) {
      FD_DO_CHOICES(slotid,slotids) {
	lispval v;
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
  lispval frames=py2lisp(self), slotids=py2lisp(arg), values=py2lisp(val);
  FD_DO_CHOICES(frame,frames) {
    FD_DO_CHOICES(slotid,slotids) {
      fd_store(frame,slotid,values);}}
  fd_decref(frames); fd_decref(slotids); fd_decref(values);
  return 1;
}

static int choice_length(pw)
     struct FD_PYTHON_WRAPPER *pw;
{                                   /* or repr or str */
  return FD_CHOICE_SIZE(pw->lval);
}

static PyObject *choice_merge(pw1,pw2)
     PyObject *pw1, *pw2;
{                                   /* or repr or str */
  lispval choices[2]; PyObject *result;
  choices[0]=py2lisp(pw1); choices[1]=py2lisp(pw2);
  lispval combined=fd_union(choices,2);
  result=lisp2py(combined);
  fd_decref(combined); fd_decref(choices[0]); fd_decref(choices[1]);
  return result;
}

static PyObject *choice_item(struct FD_PYTHON_WRAPPER *pw,int i)
{                                   /* or repr or str */
  if (i<(FD_CHOICE_SIZE(pw->lval)))
    return lisp2py((FD_CHOICE_DATA(pw->lval))[i]);
  else {
    PyErr_Format(PyExc_IndexError,"choice only has %d (<=%d) elements",
		 FD_CHOICE_SIZE(pw->lval),i);
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

static PyTypeObject OIDType = {      /* main python type-descriptor */
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

static PyTypeObject ApplicableType = {      /* main python type-descriptor */
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
  if (v->pool<w->pool) return -1;
  else if (v->pool == w->pool) return 0;
  else return 1;
}

static PyObject *pool_str(PyObject *self)
{
  struct FD_PYTHON_POOL *pw=(struct FD_PYTHON_POOL *)self;
  return PyString_FromFormat("framerd.pool('%s')",pw->pool->pool_source);
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
  if (v->index<w->index) return -1;
  else if (v->index == w->index) return 0;
  else return 1;
}

static PyObject *index_str(PyObject *self)
{
  struct FD_PYTHON_INDEX *pw=(struct FD_PYTHON_INDEX *)self;
  return PyString_FromFormat("framerd.index('%s')",pw->index->index_source);
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
    lispval obj;
    PyObject *u8=PyString_AsEncodedObject(arg,"utf8","none");
    if (u8) obj=fd_parse((u8_string)(PyString_AS_STRING(u8)));
    else return pass_error();
    Py_DECREF(u8);
    return lisp2py(obj);}
  else return NULL;
}

static PyObject *usepool(PyObject *self,PyObject *arg)
{
  fd_pool p;
  if (PyString_Check(arg)) {
    PyObject *u8=PyString_AsEncodedObject(arg,"utf8","none");
    if (u8) p=fd_use_pool((u8_string)PyString_AS_STRING(u8),-1,FD_FALSE);
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
  fd_index ix;
  if (PyString_Check(arg)) {
    PyObject *u8=PyString_AsEncodedObject(arg,"utf8","none");
    if (u8) ix=fd_open_index((u8_string)PyString_AS_STRING(u8),-1,FD_FALSE);
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
  fd_index ix;
  if (PyString_Check(arg)) {
    PyObject *u8=PyString_AsEncodedObject(arg,"utf8","none");
    if (u8) ix=fd_use_index((u8_string)PyString_AS_STRING(u8),-1,FD_FALSE);
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
  if ((PyTuple_Check(args)) && (PyTuple_GET_SIZE(args)==2)) {
    PyObject *arg0=PyTuple_GET_ITEM(args,0);
    PyObject *arg1=PyTuple_GET_ITEM(args,1), *result;
    lispval frames=py2lispx(arg0), slotids=py2lispx(arg1);
    lispval results=FD_EMPTY_CHOICE;
    Py_BEGIN_ALLOW_THREADS {
      FD_DO_CHOICES(f,frames) {
	FD_DO_CHOICES(slotid,slotids) {
	  lispval v=fd_frame_get(f,slotid);
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
  if (PyTuple_Check(args)) {
    int i=0, size=PyTuple_GET_SIZE(args), sv_start=0, sv_size=size;
    lispval *slotvals, results, indices=FD_VOID;
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
      if (FD_VOIDP(indices))
	results=fd_bgfinder(sv_size,slotvals);
      else results=fd_finder(indices,sv_size,slotvals);
      i=0; while (i<sv_size) {lispval v=slotvals[i++]; fd_decref(v);}
      u8_free(slotvals);}
    Py_END_ALLOW_THREADS;
    result=lisp2py(results);
    fd_decref(results);
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
    lispval fn = fn=py2lisp(PyTuple_GET_ITEM(args,0)), result = FD_VOID;
    if (! (FD_APPLICABLEP(fn)) ) {
      u8_string s = fd_lisp2string(fn);
      PyErr_Format(PyExc_TypeError,"Not applicable: %s",s);
      u8_free(s);
      fd_decref(fn);
      return NULL;}
    if (n_args == 0) {
      Py_BEGIN_ALLOW_THREADS {
	result = fd_apply(fn,0,NULL);}
      Py_END_ALLOW_THREADS;}
    else {
      lispval argv[size];
      int i=0; while (i<n_args) {
	PyObject *arg=PyTuple_GET_ITEM(args,i+1);
	argv[i]=py2lisp(arg);
	i++;}
      Py_BEGIN_ALLOW_THREADS {
	result=fd_apply(fn,size-1,argv);}
      Py_END_ALLOW_THREADS;
      i=0; while (i<n_args) {
	lispval v=argv[i++]; fd_decref(v);}}
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
    value=fd_eval(expr,default_env);}
  Py_END_ALLOW_THREADS;
  pyvalue=lisp2py(value);
  fd_decref(expr); fd_decref(value);
  return pyvalue;
}

static PyObject *lispfn(PyObject *self,PyObject *args)
{
  if ((PyTuple_Check(args)) && (PyTuple_GET_SIZE(args)==2)) {
    lispval modname=py2lispx(PyTuple_GET_ITEM(args,0));
    lispval fname=py2lispx(PyTuple_GET_ITEM(args,1));
    lispval module=fd_find_module(modname,0,0);
    lispval fn=fd_get(module,fname,FD_VOID);
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
   /* {"call",lispcall}, */
   {"eval",lispeval},
   {"method",lispfn},
   {NULL,NULL}};

/* Table methods for Python objects */

static lispval pyget(lispval obj,lispval key)
{
  PyObject *o = lisp2py(obj), *v;
  if (FD_STRINGP(key))
    v = PyObject_GetAttrString(o,FD_STRDATA(key));
  else if (FD_SYMBOLP(key)) {
    u8_string pname = FD_SYMBOL_NAME(key), scan = pname;
    size_t buflen = strlen(pname)*2;
    U8_STATIC_OUTPUT(keystring,buflen); int c;
    while ( (c=u8_sgetc(&scan)) > 0) {
      u8_putc(&keystring,u8_tolower(c));}
    /* Should probably lowercase it or something smarter */
    v = PyObject_GetAttrString(o,keystring.u8_outbuf);}
  else {
    PyObject *k = lisp2py(key);
    v = PyObject_GetItem(o,k);
    Py_DECREF(k);}
  if (v == NULL) {
    PyErr_Clear();
    return FD_EMPTY;}
  else {
    lispval r = py2lisp(v);
    Py_DECREF(v);
    return r;}
}

static lispval pyhas(lispval obj,lispval key)
{
  PyObject *o = lisp2py(obj); int has = -1;
  if (FD_STRINGP(key))
    has = PyObject_HasAttrString(o,FD_STRDATA(key));
  else if (FD_SYMBOLP(key)) {
    u8_string pname = FD_SYMBOL_NAME(key), scan = pname;
    size_t buflen = strlen(pname)*2;
    U8_STATIC_OUTPUT(keystring,buflen); int c;
    while ( (c=u8_sgetc(&scan)) > 0) {
      u8_putc(&keystring,u8_tolower(c));}
    /* Should probably lowercase it or something smarter */
    has = PyObject_HasAttrString(o,keystring.u8_outbuf);}
  else {
    PyObject *k = lisp2py(key);
    has = PyObject_HasAttr(o,k);
    Py_DECREF(k);}
  if (has<0)
  if (has<0) {
    PyErr_Clear();
    return FD_FALSE;}
  else if (has)
    return FD_TRUE;
  else return FD_FALSE;
}

/* Primitives for embedded Python */

static lispval pyerr(u8_context cxt)
{
  PyObject *err=PyErr_Occurred();
  if (err) {
    PyObject *type, *val, *tb;
    PyErr_Fetch(&type,&val,&tb);
    lispval v = fd_make_nvector(3,py2lisp(type),py2lisp(val),py2lisp(tb));
    if (type) Py_DECREF(type);
    if (val) Py_DECREF(val);
    if (tb) Py_DECREF(tb);
    PyErr_Clear();
    return v;}
  else return fd_err("Mysterious Python error",cxt,NULL,FD_VOID);
}

static lispval pyexec(lispval pystring)
{
  int result;
  PyGILState_STATE gstate;
  gstate=PyGILState_Ensure();
  result=PyRun_SimpleString(FD_STRDATA(pystring));
  PyGILState_Release(gstate);
  return FD_INT2DTYPE(result);
}

static lispval pystring(lispval obj)
{
  if (FD_PRIM_TYPEP(obj,python_object_type)) {
    struct FD_PYTHON_OBJECT *po=(fd_python_object)obj;
    PyObject *o=po->pyval;
    u8_string s = py2string(o);
    lispval v = fdstring(s);
    u8_free(s);
    return v;}
  else return fd_err("NotAPythonObject","pystring",NULL,obj);
}

static lispval pyapply(lispval fcn,int n,lispval *args)
{
  if (FD_PRIM_TYPEP(fcn,python_object_type)) {
    struct FD_PYTHON_OBJECT *pyo=(struct FD_PYTHON_OBJECT *)fcn;
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
    else return fd_type_error("python procedure","pyapply",fcn);}
  else return fd_type_error("python procedure","pyapply",fcn);
}

static lispval pyhandle(int n,lispval *lisp_args)
{
  lispval obj = lisp_args[0], method = lisp_args[1];
  PyObject *name;
  if (! (FD_PRIM_TYPEP(obj,python_object_type)) )
    return fd_type_error("python object","pyapply",obj);
  else if (n>8)
    return fd_err(fd_TooManyArgs,"pyhandle",NULL,FD_VOID);
  else if (FD_STRINGP(method)) {
    name = PyUnicode_DecodeUTF8
      ((char *)FD_STRING_DATA(method),FD_STRING_LENGTH(method),"none");}
  else if (FD_SYMBOLP(method)) {
    u8_string pname = FD_SYMBOL_NAME(method), scan = pname;
    size_t buflen = strlen(pname)*2;
    U8_STATIC_OUTPUT(keystring,buflen); int c;
    while ( (c=u8_sgetc(&scan)) > 0) {
      u8_putc(&keystring,u8_tolower(c));}
    name = PyUnicode_DecodeUTF8
      ((char *)keystring.u8_outbuf,
       keystring.u8_write-keystring.u8_outbuf,
       "none");}
  else return fd_type_error("method name","pyhandle",method);
  struct FD_PYTHON_OBJECT *pyo=(struct FD_PYTHON_OBJECT *) obj;
  PyObject *po = pyo->pyval;
  PyObject *args[n];
  PyObject *r;
  int i = 2; while (i<n) {
    args[i] = lisp2py(lisp_args[i]); i++;}
  switch (n) {
  case 0: case 1: return fd_err("BadCall","pyhandle",NULL,FD_VOID);
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

static lispval pytry(int n,lispval *lisp_args)
{
  lispval obj = lisp_args[0], method = lisp_args[1];
  PyObject *name;
  if (! (FD_PRIM_TYPEP(obj,python_object_type)) )
    return fd_type_error("python object","pyapply",obj);
  else if (n>8)
    return fd_err(fd_TooManyArgs,"pyhandle",NULL,FD_VOID);
  else if (FD_STRINGP(method)) {
    name = PyUnicode_DecodeUTF8
      ((char *)FD_STRING_DATA(method),FD_STRING_LENGTH(method),"none");}
  else if (FD_SYMBOLP(method)) {
    u8_string pname = FD_SYMBOL_NAME(method), scan = pname;
    size_t buflen = strlen(pname)*2;
    U8_STATIC_OUTPUT(keystring,buflen); int c;
    while ( (c=u8_sgetc(&scan)) > 0) {
      u8_putc(&keystring,u8_tolower(c));}
    name = PyUnicode_DecodeUTF8
      ((char *)keystring.u8_outbuf,
       keystring.u8_write-keystring.u8_outbuf,
       "none");}
  else return fd_type_error("method name","pyhandle",method);
  struct FD_PYTHON_OBJECT *pyo=(struct FD_PYTHON_OBJECT *) obj;
  PyObject *po = pyo->pyval;
  PyObject *args[n];
  PyObject *r;
  int i = 2; while (i<n) {
    args[i] = lisp2py(lisp_args[i]); i++;}
  switch (n) {
  case 0: case 1: return fd_err("BadCall","pyhandle",NULL,FD_VOID);
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
    return FD_EMPTY;}
  else {
    lispval result = py2lisp(r);
    Py_DECREF(r);
    return result;}
}

static lispval pydir(lispval obj)
{
  if (FD_PRIM_TYPEP(obj,python_object_type)) {
    struct FD_PYTHON_OBJECT *po=(fd_python_object)obj;
    PyObject *o=po->pyval;
    PyObject *listing = PyObject_Dir(o);
    if (listing) {
      lispval r = py2lisp(listing);
      Py_DECREF(listing);
      return r;}
    else {
      PyErr_Clear();
      return FD_FALSE;}}
  else return fd_err("NotAPythonObject","pydir",NULL,obj);
}

static lispval pydirstar(lispval obj)
{
  if (FD_PRIM_TYPEP(obj,python_object_type)) {
    struct FD_PYTHON_OBJECT *po=(fd_python_object)obj;
    PyObject *o=po->pyval;
    PyObject *listing = PyObject_Dir(o);
    if (listing) {
      if (PyTuple_Check(listing)) {
	lispval results = FD_EMPTY;
	int i=0, n=PyTuple_Size(listing);
	while (i<n) {
	  PyObject *pelt=PyTuple_GetItem(o,i);
	  lispval elt=py2lisp(pelt);
	  if (FD_ABORTP(elt)) {
	    fd_decref(results);
	    return elt;}
	  else {
	    FD_ADD_TO_CHOICE(results,elt);
	    i++;}}
	return results;}
      else {
	lispval r = py2lisp(listing);
	Py_DECREF(listing);
	return r;}}
    else {
      PyErr_Clear();
      return FD_EMPTY;}}
  else return FD_EMPTY;
}

static lispval pycall(int n,lispval *args)
{
  return pyapply(args[0],n-1,args+1);
}

static lispval pylen(lispval pyobj)
{
  struct FD_PYTHON_OBJECT *po=(fd_python_object)pyobj;
  PyObject *o=po->pyval;
  Py_ssize_t len = PyObject_Length(o);
  if (len < 0) {
    PyErr_Clear();
    return FD_FALSE;}
  else return FD_INT(len);
}

static lispval pynext(lispval pyobj,lispval termval)
{
  struct FD_PYTHON_OBJECT *po=(fd_python_object)pyobj;
  PyObject *o=po->pyval;
  if (PyIter_Check(o)) {
    PyObject *next = PyIter_Next(o);
    if (next)
      return py2lisp(next);
    else return fd_incref(termval);}
  else return fd_type_error("PythonIterator","pynext",pyobj);
}

static lispval pyfcn(lispval modname,lispval fname)
{
  PyObject *o;
  if (FD_STRINGP(modname)) {
    PyObject *pmodulename=lisp2py(modname);
    o = PyImport_Import(pmodulename);
    Py_DECREF(pmodulename);}
  else if (FD_TYPEP(modname,python_object_type))
    o = lisp2py(modname);
  else return fd_err("NotModuleOrObject","pymethod",NULL,modname);
  if (o) {
    PyObject *pFunc=PyObject_GetAttrString(o,FD_STRDATA(fname));
    Py_DECREF(o);
    if (pFunc) {
      lispval wrapped=py2lisp(pFunc);
      Py_DECREF(pFunc);
      return wrapped;}
    return pyerr("pymethod");}
  else return pyerr("pymethod");
}

static lispval pyimport(lispval modname)
{
  PyObject *o;
  if (FD_STRINGP(modname)) {
    PyObject *pmodulename=lisp2py(modname);
    o = PyImport_Import(pmodulename);
    if (o) {
      lispval wrapped=py2lisp(o);
      Py_DECREF(o);
      return wrapped;}
    else return pyerr("pyimport");}
  else return FD_FALSE;
}

static lispval pymodule=FD_VOID;
static int python_init_done=0;

static int pypath_config_set(lispval var,lispval val,void * data)
{
  PyObject* sysPath = PySys_GetObject((char*)"path");
  if (!(FD_STRINGP(val))) {
    fd_seterr("NotAString","pypath_config_set",NULL,val);
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
  fd_init_fdweb();
#endif
  pymodule=fd_new_module("PARSELTONGUE",0);
  python_object_type=fd_register_cons_type("python");
  default_env=fd_working_lexenv();
  fd_recyclers[python_object_type]=recycle_python_object;
  fd_unparsers[python_object_type]=unparse_python_object;

  fd_applyfns[python_object_type]=pyapply;

  fd_idefn1(pymodule,"PY/EXEC",pyexec,1,
	    "Executes a Python expression",
	    fd_string_type,FD_VOID);
  fd_idefn1(pymodule,"PY/IMPORT",pyimport,1,
	    "Imports a python module/file",
	    fd_string_type,FD_VOID);
  fd_idefnN(pymodule,"PY/CALL",pycall,1,
	    "Calls a python function on some arguments");
  fd_idefnN(pymodule,"PY/HANDLE",pyhandle,2,
	    "Calls a method on a Python object");
  fd_idefnN(pymodule,"PY/TRY",pytry,2,
	    "Calls a method on a Python object, returning {} on error");
  fd_idefn2(pymodule,"PY/NEXT",pynext,1,
	    "Advances an iterator",
	    python_object_type,FD_VOID,-1,FD_EMPTY);
  fd_idefn2(pymodule,"PY/FCN",pyfcn,2,
	    "Returns a python method object",
	    -1,FD_VOID,
	    fd_string_type,FD_VOID);
  fd_idefn1(pymodule,"PY/LEN",pylen,1,
	    "`(PY/LEN *obj* *rv*) Returns the length of *obj* or #f "
	    "if *obj* doesn't have a length",
	    python_object_type,FD_VOID);
  fd_idefn1(pymodule,"PY/STRING",pystring,1,
	    "Returns a string containing the printed representation "
	    "of a Python object",
	    python_object_type,FD_VOID);
  fd_idefn1(pymodule,"PY/DIR",pydir,1,
	    "Returns a vector of fields on a Python object "
	    "or #F if it isn't a map",
	    python_object_type,FD_VOID);
  fd_idefn1(pymodule,"PY/DIR*",pydirstar,1,
	    "Returns a choice of the fields on a Python object "
	    "or {} if it isn't a map",
	    python_object_type,FD_VOID);
  fd_idefn2(pymodule,"PY/GET",pyget,2,
	    "Gets a field from a python object",
	    python_object_type,FD_VOID,-1,FD_VOID);
  fd_idefn2(pymodule,"PY/HAS",pyhas,2,
	    "Returns true if a python object has a field",
	    python_object_type,FD_VOID,-1,FD_VOID);
  fd_finish_module(pymodule);

  fd_register_config("PYPATH","The search path used by Python",
		     pypath_config_get,pypath_config_set,NULL);

}

static void initpythonmodule()
{
  PyObject *m;
  if (python_init_done) return;
  m=Py_InitModule("framerd",framerd_methods);
  framerd_error=PyErr_NewException("framerd.error",NULL,NULL);
  Py_INCREF(framerd_error);
  PyModule_AddObject(m,"error",framerd_error);
  python_init_done=1;
}

void fd_init_parseltongue(void) FD_LIBINIT_FN;

static char *py_argv[256]={"",NULL};
static int py_argc=1;

void fd_init_parseltongue()
{
  if (!(Py_IsInitialized())) Py_Initialize();
  PySys_SetArgvEx(py_argc,py_argv,0);
  initframerdmodule();
  initpythonmodule();
}

void initparseltongue(void)
{
  fd_init_parseltongue();
}

