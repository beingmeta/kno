/* Various utilities for manipulating the dom */

var _debug_domedits=false;

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

function fdb_block_eltp(elt)
{
  var name=elt.tagName;
  return ((name==='DIV') || (name==='P') || (name==='LI') || (name==='UL'));
}

function fdbHasAttrib(elt,attribname,attribval)
{
  if (typeof attribval == 'undefined')
    if (elt.hasAttribute)
      return elt.hasAttribute(attribname);
    else if (elt.getAttribute)
      if (elt.getAttribute(attribname))
	return true;
      else return false;
    else return false;
  else if ((elt.getAttribute) &&
	   (elt.getAttribute(attribname)==attribval))
    return true;
  else return false;
}

/* Searching by tag name */

function fdbGetParentByTagName(node,tagname)
{
  var scan;
  if (typeof node == "string")
    scan=document.getElementById(node);
  else scan=node;
  tagname=tagname.toUpperCase();
  while ((scan) && (scan.parentNode)) 
    if (scan.tagName===tagname) return scan;
    else scan=scan.parentNode;
  if ((scan) && (scan.tagName===tagname)) return scan;
  else return null;
}

function fdbGetChildrenByTagName(under,tagname)
{
  if (typeof under == 'string') {
    under=document.getElementById(under);
    if (under===null) return new Array();}
  tagname=tagname.toUpperCase();
  if (under===null)
    return _fdbGetChildrenByTagName(document,tagname,new Array())
  else return _fdbGetChildrenByTagName(under,tagname,new Array())
}
function _fdbGetChildrenByTagName(under,tagname,results)
{
  if ((under.nodeType===1) && (under.tagName===tagname))
    results.push(under);
  var children=under.childNodes;
  var i=0; while (i<children.length)
    if (children[i].nodeType==1)
      _fdbGetChildrenByTagName(children[i++],tagname,results);
    else i++;
  return results;
}

/* Searching by class name */

function fdbGetParentByClassName(node,classname)
{
  var scan;
  if (typeof node === "string") scan=document.getElementById(node);
  else scan=node;
  while ((scan) && (scan.parentNode))
    if (scan.className===classname) return scan;
    else scan=scan.parentNode;
  if ((scan) && (scan.className===classname)) return scan;
  else return null;
}

function fdbGetChildrenByClassName(under,classname)
{
  if (typeof under == 'string')
    under=document.getElementById(under);
  if ((under) && (under.getElementsByClassName))
    return under.getElementsByClassName(classname);
  else if ((under===null) && (document.getElementsByClassName))
    return document.getElementsByClassName(classname);
  else if (under===null)
    return _fdbGetChildrenByClassName(document,classname,new Array());
  else return _fdbGetChildrenByClassName(under,classname,new Array());
}
function _fdbGetChildrenByClassName(under,classname)
{
  if ((under.nodeType===1) && (under.className===classname))
    results.push(under);
  var children=under.childNodes;
  var i=0; while (i<children.length)
    if (children[i].nodeType===1)
      _fdbGetChildrenByTagName(children[i++],tagname,results);
    else i++;
  return results;
}     

/* Kind of legacy */

function fdbGetElementsByClassName(classname,under_arg)
{
  var under;
  if (typeof under_arg === 'undefined') under=null;
  else if (typeof under_arg === 'string')
    under=document.getElementById(under_arg);
  else under=under_arg;
  if (under===null) return new Array();
  else return fdbGetChildrenByClassName(under,classname);
}

/* Searching by attribute */

function fdbGetParentByAttrib(node,attribName,attribValue)
{
  var scan;
  if (typeof node === "string") scan=document.getElementById(node);
  else node=scan;
  if (attribValue)
    while ((scan) && (scan.parentNode))
      if (scan.getAttribute(attribName)==attribValue)
	return scan;
      else parent=scan.parentNode;
  else while ((scan) && (scan.parentNode))
    if ((scan.hasAttribute) ? (scan.hasAttribute(attribName)) :
	(!(!(scan.getAttribute(attribName)))))
      return scan;
    else scan=scan.parentNode;
}

function fdbGetChildrenByAttrib(under,attribName,attribValue)
{
  if (typeof under === 'string')
    under=document.getElementById(under);
  if (attribValue)
    return _fdbGetChildrenByAttribValue
      (under,attribName,attribValue,new Array());
  else return _fdbGetChildrenByAttrib(under,attribName,new Array());
}
function _fdbGetChildrenByAttrib(under,attribname,results)
{
  if ((under.nodeType==1) &&
      ((under.hasAttribute) ? (under.hasAttribute(attribname)) :
       (under.getAttribute(attribname))))
    results.push(under);
  var children=under.childNodes;
  var i=0; while (i<children.length)
    if (children[i].nodeType==1)
      _fdbGetChildrenByAttrib(children[i++],attribname,results);
    else i++;
  return results;
}
function _fdbGetChildrenByAttribValue(under,attribname,attribval,results)
{
  if ((under.nodeType==1) &&
      (under.getAttribute(attribname)==attribval))
    results.push(under);
  var children=under.childNodes;
  var i=0; while (i<children.length)
    if (children[i].nodeType==1)
      _fdbGetChildrenByAttrib(children[i++],attribname,attribval,results);
    else i++;
  return results;
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
  if ((_debug) || (_debug_domedits))
    fdbLog("Inserting "+elts+" elements "
	   +"into "+elt
	   +" before "+before
	   +" starting with "+elts[0]);
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
      return fdbInsertElementsBefore(elt,elt.firstChild,arguments,1);
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
  if (typeof cur_arg === 'string')
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

function fdbRemove(cur_arg)
{
  var cur=null;
  if (typeof cur_arg == "string")
    cur=document.getElementById(cur_arg);
  else cur=cur_arg;
  if (cur) {
    var parent=cur.parentNode;
    parent.removeChild(cur);
    return null;}
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

/* Dealing with selections */

function fdbGetSelection(elt)
{
  if ((elt.tagName==='INPUT') || (elt.tagName==='TEXTAREA')) {
    // fdbLog('start='+elt.selectionStart+'; end='+elt.selectionEnd+
    //   '; value='+elt.value);
    if ((elt.value) && (elt.selectionStart) && (elt.selectionEnd) &&
	(elt.selectionStart!=elt.selectionEnd)) 
      return elt.value.slice(elt.selectionStart,elt.selectionEnd);
    else return null;}
  else if (window.getSelection)
    return window.getSelection();
  else return null;
}
