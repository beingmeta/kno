/* Various utilities for manipulating the dom */

function $(eltarg)
{
  if (typeof eltarg == 'string')
    return document.getElementById(eltarg);
  else return eltarg;
}

function fdbLog(string)
{
  if ((console) && (console.log)) console.log(string);
}

function fdbWarn(string)
{
  if ((console) && (console.log))
    console.log(string);
  else alert(string);
}

function fdbNeedElt(arg,name)
{
  if (typeof arg == 'string') {
    var elt=document.getElementById(arg);
    if (elt) return elt;
    else if (name)
      fdbWarn("Invalid element ("+elt+") reference: "+arg);
    else fdbWarn("Invalid element reference: "+arg);
    return null;}
  else if (arg) return arg;
  else {
    if (name)
      fdbWarn("Invalid element ("+elt+") reference: "+arg);
    else fdbWarn("Invalid element reference: "+arg);
    return null;}
}

/* Adding elements */

function fdbAddElements(elt,elts,i)
{
  while (i<elts.length) {
    var arg=elts[i++];
    if (typeof arg == 'string')
      elt.appendChild(document.createTextNode(arg));
    else elt.appendChild(arg);}
  return elt;
}

function fdbInsertElementsBefore(elt,before,args,i)
{
  while (i<args.length) {
    var arg=args[i++];
    if (typeof arg == 'string')
      elt.insertBefore(before,document.createTextNode(arg));
    else elt.insertBefore(arg,before);}
  return elt;
}

function fdbAddAttributes(elt,attribs)
{
  if (attribs) {
    for (key in attribs) {
      if (key=='title')
	elt.title=attribs[key];
      else if (key=='name')
	elt.name=attribs[key];
      else if (key=='id')
	elt.id=attribs[key];
      else if (key=='value')
	elt.value=attribs[key];
      else elt.setAttribute(key,attribs[key]);}
    return elt;}
  else return elt;
}

/* Higher level functions, use lexpr/rest/var args */

function fdbAppend(elt_arg)
{
  var elt=null;
  if (typeof elt_arg == 'string')
    elt=document.getElementById(elt_arg);
  else if (elt_arg) elt=elt_arg;
  if (elt) return fdbAddElements(elt,args,1);
  else fdbWarn("Invalid DOM argument: "+elt_arg);
}

function fdbInsert(elt_arg,after_arg)
{
  var elt=null, after=null;
  if (typeof elt_arg == 'string')
    elt=document.getElementById(elt_arg);
  else if (elt_arg) elt=elt_arg;
  if (typeof after_arg == 'string') {
    after=document.getElementById(after_arg);
    if (after==null) {
      fdbWarn("Invalid DOM after argument: "+after_arg);
      return;}}
  if (elt==null)
    if (elt.firstChild)
      return fdbInsertElementsBefore(elt,elt.firstChild,args,2);
    else return fdbAddElements(elt,args,2);
  else if (after==elt)
    return fdbAddElements(elt,args,2);
  else if (after.nextSibling)
    return fdbInsertElements(elt,after.nextSibling,args,2);
  else return fdbAddElements(elt,args,2);
}

function fdbNewElement(tag,classname)
{
  var elt=document.createElement(tag);
  elt.className=classname;
  fdbAddElements(elt,arguments,2);
  return elt;
}

function fdbNewElementW(tag,classname,attribs)
{
  var elt=document.createElement(tag);
  elt.className=classname;
  fdbAddAttributes(elt,attribs);
  fdbAddElements(elt,arguments,3);
  return elt;
}

function fdbSpan(classname)
{
  var elt=document.createElement('span');
  elt.className=classname;
  fdbAddElements(elt,arguments,1);
  return elt;
}

function fdbSpanW(classname,attribs)
{
  var elt=document.createElement('span');
  elt.className=classname;
  fdbAddAttributes(elt,attribs);
  fdbAddElements(elt,arguments,2);
  return elt;
}

function fdbDiv(classname)
{
  var elt=document.createElement('div');
  elt.className=classname;
  fdbAddElements(elt,arguments,1);
  return elt;
}

function fdbDivW(classname,attribs)
{
  var elt=document.createElement('div');
  elt.className=classname;
  fdbAddAttributes(elt,attribs);
  fdbAddElements(elt,arguments,2);
  return elt;
}

function fdbInput(type,name,value,classname)
{
  var elt=document.createElement('input');
  elt.type=type; elt.name=name; elt.value=value;
  if (classname) elt.className=classname;
  return elt;
}

function fdbCheckbox(name,value,checked)
{
  var elt=document.createElement('input');
  elt.type='checkbox'; elt.name=name; elt.value=value;
  if (checked) elt.checked=true;
  else elt.checked=false;
  return elt;
}

