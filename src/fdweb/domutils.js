/* Various utilities for manipulating the dom */

function $(eltarg)
{
  if (typeof eltarg == 'string')
    return document.getElementById(eltarg);
  else return eltarg;
}

function fdbLog(string)
{
  if ((typeof console != 'undefined') && (console) && (console.log))
    console.log(string);
}

function fdbWarn(string)
{
  if ((typeof console != 'undefined') && (console) && (console.log))
    console.log(string);
  else alert(string);
}

/* This outputs a warning if a given elements if not found. */
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

/* Various search functions */

function fdbGetElementsByClassName(classname,under_arg)
{
  var under;
  if (typeof under_arg == 'undefined') under=null;
  else if (typeof under_arg == 'string')
    under=document.getElementById(under_arg);
  else under=under_arg;
  if (document.getElementsByClassName)
    if (under)
      under.getElementsByClassName(classname);
    else document.getElementsByClassName(classname);
  else if ((under) ? (under.all) : (document.all)) {
    var nodes=((under) ? (under.all) : (document.all))
    var results=[];
    var i=0; while (i<nodes.length) {
      var node=nodes[i++];
      if (node.nodeType==1)
	if (node.className==classname)
	  results.push(node);}
    return results;}
}

function fdbHasAttrib(elt,attribname)
{
  if (elt.hasAttribute)
    return elt.hasAttribute(attribname);
  else if (elt.getAttribute)
    if (elt.getAttribute(attribname))
      return true;
    else return false;
  else return false;
}

/* Adding/Inserting nodes */

function fdbAddElements(elt,elts,i)
{
  while (i<elts.length) {
    var arg=elts[i++];
    if (typeof arg == 'string')
      elt.appendChild(document.createTextNode(arg));
    else elt.appendChild(arg);}
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

function fdbInsertElementsBefore(elt,before,elts,i)
{
  while (i<elts.length) {
    var arg=elts[i++];
    if (typeof arg == 'string')
      elt.insertBefore(document.createTextNode(arg),before);
    else elt.insertBefore(arg,before);}
  return elt;
}

/* Higher level functions, use lexpr/rest/var args */

function fdbAppend(elt_arg)
{
  var elt=null;
  if (typeof elt_arg == 'string')
    elt=document.getElementById(elt_arg);
  else if (elt_arg) elt=elt_arg;
  if (elt) return fdbAddElements(elt,arguments,1);
  else fdbWarn("Invalid DOM argument: "+elt_arg);
}

function fdbPrepend(elt_arg)
{
  var elt=null;
  if (typeof elt_arg == 'string')
    elt=document.getElementById(elt_arg);
  else if (elt_arg) elt=elt_arg;
  if (elt)
    if (elt.firstChild)
      return fdbInsertElementsBefore(elt.firstChild,arguments,1);
    else return fdbAddElements(elt,arguments,1);
  else fdbWarn("Invalid DOM argument: "+elt_arg);
}

function fdbInsertBefore(before_arg)
{
  var parent=null, before=null;
  if (typeof before_arg == 'string') {
    before=document.getElementById(after_arg);
    if (after==null) {
      fdbWarn("Invalid DOM before argument: "+before_arg);
      return;}}
  else before=before_arg;
  if ((before) && (before.parentNode))
    elt=before.parentNode;
  else {
    if (before===before_arg)
      fdbWarn("Invalid DOM before argument: "+before_arg);
    else fdbWarn("Invalid DOM before argument: "+before_arg+"="+before);
    return;}
  return fdbInsertElementsBefore(elt,before,arguments,1);
}

function fdbInsertAfter(after_arg)
{
  var parent=null, after=null;
  if (typeof after_arg == 'string') {
    after=document.getElementById(after_arg);
    if (after==null) {
      fdbWarn("Invalid DOM after argument: "+after_arg);
      return;}}
  else after=after_arg;
  if ((after) && (after.parentNode))
    elt=after.parentNode;
  else {
    if (after===after_arg)
      fdbWarn("Invalid DOM after argument: "+after_arg);
    else fdbWarn("Invalid DOM after argument: "+after_arg+"="+after);
    return;}
  if (after.nextSibling)
    return fdbInsertElementsBefore(elt,after.nextSibling,arguments,1);
  else return fdbAddElements(elt,arguments,1);
}

function fdbReplace(cur_arg,newnode)
{
  var cur=null;
  if (typeof cur_arg == string)
    cur=document.getElementById(cur_arg);
  else cur=cur_arg;
  if (cur) {
    var parent=cur.parentNode;
    parent.replaceChild(newnode,cur);
    if ((cur.id) && (newnode.id==null))
      newnode.id=cur.id;
    return newnode;}
  else {
    fdbWarn("Invalid DOM replace argument: "+cur_arg);
    return;}
}

/* Element Creation */

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

