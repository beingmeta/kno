/* Various utilities for manipulating the dom */

var domutils_version="$Id:$";
var _debug=false;
var _debug_domedits=false;

function $(eltarg)
{
  if (typeof eltarg == 'string')
    return document.getElementById(eltarg);
  else return eltarg;
}

function fdbResolveHash(eltarg)
{
  if (typeof eltarg == 'string') {
    var elt=document.getElementById(eltarg);
    if ((typeof elt != "undefined") && (elt)) return elt;
    else {
      var elts=document.getElementsByName(eltarg);
      if ((elts) && (elts.length>0)) return elts[0];
      else return false;}}
  else return eltarg;
}

function fdbLog(string)
{
  if ((typeof console != 'undefined') && (console) && (console.log))
    console.log.apply(console,arguments);
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

/* Manipluating class names */

var _fdb_whitespace_pat=/\b/;

function _fdb_get_class_regex(name)
{
  var rx=new RegExp("\b"+name+"\b");
  return rx;
}

function fdbHasClass(elt,classname)
{
  var classinfo=elt.className;
  if ((classinfo) &&
      ((classinfo==classname) ||
       (classinfo.search(_fdb_get_class_regex(classname))>=0)))
    return true;
  else return false;
}

function fdbAddClass(elt,classname)
{
  var classinfo=elt.className;
  if ((classinfo===null) || (classinfo=="")) {
    elt.className=classname;
    return true;}
  else if (classinfo===classname)
    return false;
  else if (classinfo.search(_fdb_get_class_regex(classname))>=0)
    return false;
  else {
    elt.className=classname+" "+classinfo;
    return true;}
}

function fdbDropClass(elt,classname)
{
  var classinfo=elt.className, classpat;
  if ((classinfo===null) || (classinfo=="")) return false;
  else if (classinfo===classname) {
    elt.className=null;
    return true;}
  else if (classinfo.search(classpat=_fdb_get_class_regex(classname))) {
    elt.className=
      classinfo.replace(classpat,"").replace(_fdb_whitespace_pat," ");
    return true;}
  else return false;
}

function fdbSwapClass(elt,classname,newclass)
{
  var classinfo=elt.className, classpat=_fdb_get_class_regex(classname);
  if ((classinfo) && ((classinfo.search(classpat))>=0)) {
    elt.className=
      classinfo.replace(classpat,newclass).replace(_fdb_whitespace_pat," ");
    return true;}
  else return false;
}

/* Next and previous elements */

function fdbNextElement(node)
{
  if (node.nextElementSibling)
    return node.nextElementSibling;
  else {
    var scan=node;
    while (scan=scan.nextSibling) {
      if (typeof scan === "undefined") return null;
      else if (scan.nodeType==1) break;
      else {}}
    return scan;}
}

function fdbPreviousElement(node)
{
  if (node.previousElementSibling)
    return node.previousElementSibling;
  else {
    var scan=node;
    while (scan=scan.previousSibling) 
      if (typeof scan === "undefined") return null;
      else if (scan.nodeType==1) break;
      else {}
    return scan;}
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
    if (document.getElementsByTagName)
      return document.getElementsByTagName(tagname);
    else return _fdbGetChildrenByTagName(document,tagname,new Array());
  else if (under.getElementsByTagName)
    return under.getElementsByTagName(tagname);
  else return _fdbGetChildrenByTagName(under,tagname,new Array());
}
function _fdbGetChildrenByTagName(under,tagname,results)
{
  if ((under.nodeType===1) && (under.tagName==tagname))
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
function _fdbGetChildrenByClassName(under,classname,results)
{
  if ((under.nodeType===1) && (under.className===classname))
    results.push(under);
  var children=under.childNodes;
  var i=0; while (i<children.length)
    if (children[i].nodeType===1)
      _fdbGetChildrenByClassName(children[i++],tagname,results);
    else i++;
  return results;
}     

/* Kind of legacy */

function fdbGetElementsByClassName(classname,under_arg)
{
  if (typeof under_arg === 'undefined')
    return fdbGetChildrenByClassName(null,classname);
  else if (typeof under_arg === 'string') {
    var under=document.getElementById(under_arg);
    if (under==null) return new Array();
    else return fdbGetChildrenByClassName(under,classname);}
  else return fdbGetChildrenByClassName(under_arg,classname);
}

function fdbGetElementsByTagName(tagname,under_arg)
{
  if (typeof under_arg === 'undefined')
    return fdbGetChildrenByTagName(null,tagname.toUpperCase());
  else if (typeof under_arg === 'string') {
    var under=document.getElementById(under_arg);
    if (under==null) return new Array();
    else return fdbGetChildrenByTagName(under,tagname.toUpperCase());}
  else return fdbGetChildrenByTagName(under_arg,tagname);
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

/* Searching by selector */

function fdbParseSelector(spec)
{
  var tagname=null, classname=null, idname=null;
  var dotpos=spec.indexOf('.'), hashpos=spec.indexOf('#');
  if ((dotpos<0) && (hashpos<0))
    tagname=spec.toUpperCase();
  else if (dotpos>=0) {
    classname=spec.slice(dotpos+1);
    if (dotpos>0) tagname=spec.slice(0,dotpos).toUpperCase();}
  else if (hashpos>=0) {
    idname=spec.slice(hashpos+1);
    if (hashpos>0) tagname=spec.slice(0,hashpos).toUpperCase();}
  else if (dotpos<hashpos) {
    if (dotpos>0) tagname=spec.slice(0,dotpos);
    classname=spec.slice(dotpos+1,hashpos);
    idname=spec[hashpos];}
  else {
    if (hashpos>0) tagname=spec.slice(0,hashpos).toUpperCase();
    classname=spec.slice(dotpos);
    idname=spec.slice(hashpos+1,dotpos);}
  if ((tagname==="") || (tagname==="*")) tagname=null;
  if ((classname==="") || (classname==="*")) tagname=null;
  if ((idname==="") || (idname==="*")) tagname=null;
  return new Array(tagname,classname,idname);
}

function fdbElementMatches(elt,selector)
{
  if (selector instanceof Array) {
    var i=0; while (i<spec.length)
	       if (fdbElementMatches(elt,selector[i]))
		 return true;
	       else i++;
    return false;}
  else {
    var spec=fdbParseSelector(selector);
    return (((spec[0]===null) || (elt.tagName===spec[0])) &&
	    ((spec[1]===null) || (elt.className===spec[1])) &&
	    ((spec[2]===null) || (elt.id===spec[2])));}
}

function fdbElementMatchesSpec(elt,spec)
{
  return (((spec[0]===null) || (elt.tagName===spec[0])) &&
	  ((spec[1]===null) || (elt.className===spec[1])) &&
	  ((spec[2]===null) || (elt.id===spec[2])));
}

function fdbGetParents(elt,selector,results)
{
  if (typeof results === "undefined") results=new Array();
  if (selector instanceof Array) {
    var i=0; while (i<selector.length) 
      fdbGetParents(elt,selector[i++],results);
    return results;}
  else {
    var scan=elt;
    while (scan) {
      if (results.indexOf(scan)>=0) {}
      else if (fdbElementMatchesSpec(scan,spec)) 
	results.push(scan);
      else {}
      scan=scan.parentNode;}
    return results;}
}

function fdbGetChildren(elt,selector,results)
{
  if (typeof results === "undefined") results=new Array();
  if (selector instanceof Array) {
    var i=0; while (i<selector.length)
	       fdbGetChildren(elt,selector[i++],results);
    return results;}
  else {
    var spec=fdbParseSelector(selector);
    if (spec[2]) {
      var candidate=document.getElementById(spec[2]);
      if (candidate)
	if (fdbElemenMatchesSpec(candidate,spec)) {
	  var scan=candidate;
	  while (scan)
	    if (scan===elt) break;
	    else scan=scan.parentNode;
	  if (scan===elt) results.push(candidate);
	  return results;}
	else return results;}
    else if (spec[1]) {
      var candidates=fdbGetChildrenByClassName(elt,spec[1]);
      var i=0; while (i<candidates.length) {
	var candidate=candidates[i++];
	if (fdbElementMatchesSpec(candidate,spec))
	  results.push(candidate);}
      return results;}
    else if (spec[0]) {
      var candidates=fdbGetChildrenByTagName(elt,spec[0]);
      results.concat(candidates);
      return results;}
    else return results;}
}

function $$(selector,cxt) 
{
  var elt=((typeof cxt === "undefined") ? (document) : (cxt));
  return fdbGetChildren(cxt,selector,new Array());
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
    if ((cur.id) && (newnode.id==null)) {
      newnode.id=cur.id; cur.id=null;}
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
  if (typeof classname === "string")
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
  if (typeof classname == 'undefined') classname=null;
  var elt=document.createElement('span');
  if (classname) elt.className=classname;
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
  if (typeof classname == 'undefined') classname=null;
  var elt=document.createElement('div');
  if (classname) elt.className=classname;
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

function fdbImage(url,classname,alt)
{
  if (typeof classname == 'undefined') classname=null;
  var elt=document.createElement('img');
  if (classname) elt.className=classname;
  elt.src=url;
  if (typeof alt == "string") elt.alt=alt;
  return elt;
}

function fdbImageW(url,attribs)
{
  if (typeof attribs == 'undefined') attribs=false;
  var elt=document.createElement('img');
  elt.src=url;
  if (attribs) fdbAddAttributes(elt,attribs);
  return elt;
}

function fdbAnchor(url)
{
  var elt=document.createElement('a');
  elt.href=url;
  fdbAddElements(elt,arguments,1);
  return elt;
}

function fdbAnchorW(url,attribs)
{
  var elt=document.createElement('a');
  elt.href=url;
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

/* Forcing IDs */

var _fdb_idcounter=0, fdb_idbase=false;

function fdbForceId(about)
{
  if ((typeof about.id != "undefined") && (about.id))
    return about.id;
  else {
    if (!(fdb_idbase))
      fdb_idbase="TMPID"+(1000000+(Math.floor((1000000-1)*Math.random())))+"S";
    var tmpid=fdb_idbase+_fdb_idcounter++;
    while (document.getElementById(tmpid))
      tmpid=fdb_idbase+_fdb_idcounter++;
    about.id=tmpid;
    return tmpid;}
}

/* Checking element visibility */

function fdbIsVisible(elt,partial)
{
  if (typeof partial === "undefined") partial=false;
  var top = elt.offsetTop;
  var left = elt.offsetLeft;
  var width = elt.offsetWidth;
  var height = elt.offsetHeight;
  var winx=window.pageXOffset;
  var winy=window.pageYOffset;
  var winxedge=winx+window.innerWidth;
  var winyedge=winy+window.innerHeight;
  
  while(elt.offsetParent) {
    elt = elt.offsetParent;
    top += elt.offsetTop;
    left += elt.offsetLeft;}

  if (partial)
    // There are three cases we check for:
    return (
	    // top of element in window
	    ((top > winy) && (top < winyedge) &&
	     (left > winx) && (left < winxedge)) ||
	    // bottom of element in window
	    ((top+height > winy) && (top+height < winyedge) &&
	     (left+width > winx) && (left+width < winxedge)) ||
	    // top above/left of window, bottom below/right of window
	    (((top < winy) || (left < winx)) &&
	     ((top+height > winyedge) && (left+width > winxedge))));
  else return (top >= window.pageYOffset &&
	       left >= window.pageXOffset &&
	       (top + height) <= (window.pageYOffset + window.innerHeight) &&
	       (left + width) <= (window.pageXOffset + window.innerWidth));
}

/* Computing "flat width" of a node */

var __fdb_vertical_flat_width=8;
var __fdb_vertical_tags=["P","DIV","BR","UL","LI","BLOCKQUOTE",
			 "H1","H2","H3","H4","H5","H6"];
var __fdb_flat_width_fns={};

function _fdb_compute_flat_width(node,sofar)
{
  if (node.nodeType===Node.ELEMENT_NODE) {
    if (typeof __fdb_flat_width_fns[node.tagName] == "function")
      sofar=sofar+__fdb_flat_width_fns[node.tagName];
    else if (__fdb_vertical_tags.indexOf(node.tagName)>=0)
      sofar=sofar+__fdb_vertical_flat_width;
    else if (fdbHasAttrib(node,"flatwidth")) {
      var fw=node.getAttribute("flatwidth");
      if (typeof fw == "string") {
	if ((fw.length>0) && (fw[0]=='+'))
	  sofar=sofar+parseInt(fw.slice(1));
	else {
	  var fwnum=parseInt(fw);
	  if (typeof fwnum==="number") return sofar+fwnum;}}
      else if (typeof fw == "number") return sofar+fw;}
    if (node.hasChildNodes()) {
      var children=node.childNodes;
      var i=0; while (i<children.length) {
	var child=children[i++];
	if (child.nodeType===Node.TEXT_NODE)
	  sofar=sofar+child.nodeValue.length;
	else if (child.nodeType===Node.ELEMENT_NODE)
	  sofar=_fdb_compute_flat_width(child,sofar);
	else {}}
      return sofar;}
    else if (node.offsetWidth)
      return sofar+Math.ceil(node.offsetWidth/10)+
	Math.floor(node.offsetHeight/16)*__fdb_vertical_flat_width;
    else if (node.width)
      return sofar+Math.ceil(node.width/10)+
	Math.floor(node.height/16)*__fdb_vertical_flat_width;
    else return sofar;}
  else if (node.nodeType===Node.TEXT_NODE)
    return sofar+node.nodeValue.length;
  else return sofar;
}

function fdbFlatWidth(node,sofar)
{
  if (typeof sofar === "undefined") sofar=0;
  return _fdb_compute_flat_width(node,sofar);
}

/* CLeaning up markup */


/* Cleaning up headers for the HUD */

var fdb_cleanup_tags=["A","BR","HR"];
var fdb_cleanup_classes=[];

function _fdb_cleanup_tags(elt,name)
{
  var toremove=fdbGetChildrenByTagName(name);
  var i=0; while (i<toremove.length) fdbRemove(toremove[i++]);
}

function _fdb_cleanup_classes(elt,classname)
{
  var toremove=fdbGetChildrenByClassName(classname);
  var i=0; while (i<toremove.length) fdbRemove(toremove[i++]);
}

function _fdb_cleanup_content(elt)
{
  var tagname=elt.tagName, classname=elt.className;
  if (fdb_cleanup_tags.indexOf(tagname)>=0)
    return false;
  else if ((classname) &&
	   ((fdb_cleanup_classes.indexOf(classname))>=0))
    return false;
  else {
    var i=0; while (i<fdb_cleanup_tags.length)
	       _fdb_cleanup_tags(elt,fdb_cleanup_tags[i++]);
    i=0; while (i<fdb_cleanup_classes.length)
	   _fdb_cleanup_classes(elt,fdb_cleanup_classes[i++]);
    return elt;}
}

function fdb_cleanup_content(elt)
{
  var contents=new Array();
  var children=elt.childNodes;
  var i=0; while (i<children.length) {
    var child=children[i++];
    if (child.nodeType===1) {
      var converted=_fdb_cleanup_content(child.cloneNode(true));
      if (converted) contents.push(converted);}
    else contents.push(child.cloneNode(true));}
  return contents;
}

/* Getting simple text */

function _fdb_extract_strings(node,strings)
{
  if (node.nodeType===Node.TEXT_NODE) {
    var value=node.nodeValue;
    if (typeof value === "string") 
      strings.push(node.nodeValue);}
  else if (node.nodeType===Node.ELEMENT_NODE)
    if (fdb_cleanup_tags.indexOf(node.tagName)>=0) {}
    else if ((node.className) &&
	     (fdb_cleanup_classes.indexOf(node.className)>=0)) {}
    else if (node.hasChildNodes()) {
      var children=node.childNodes;
      var i=0; while (i<children.length) {
	var child=children[i++];
	if (child.nodeType===Node.TEXT_NODE) {
	  var value=child.nodeValue;
	  if (typeof value === "string") strings.push(value);}
	else if (child.nodeType===Node.ELEMENT_NODE)
	  _fdb_extract_strings(child,strings);
	else {}}}
    else  {}
}

function fdbJustText(node)
{
  var strings=new Array();
  _fdb_extract_strings(node,strings);
  return strings.join("");
}

/* Looking up elements, CSS-style, in tables */

function fdbLookupElement(table,elt)
{
  var tagname=elt.tagName;
  var classname=elt.className;
  var idname=elt.id;
  var probe;
  if ((idname) && (classname))
    probe=table[tagname+"."+classname+"#"+idname];
  else if (idname)
    probe=table[tagname+"#"+idname];
  else if (classname)
    probe=table[tagname+"."+classname];
  if ((typeof probe != "undefined") && (probe)) return probe;
  if ((idname) || (classname))
    if (idname)
      probe=table[tagname+"#"+idname];
    else if (classname)
      probe=table[tagname+"."+classname];
  if ((typeof probe != "undefined") && (probe)) return probe;
  if (idname) probe=table["#"+idname];
  if ((typeof probe != "undefined") && (probe)) return probe;
  if (classname) probe=table["."+classname];
  if ((typeof probe != "undefined") && (probe)) return probe;
  else probe=table[tagname];
  if ((typeof probe != "undefined") && (probe)) return probe;
  else return false;
}

/* Guessing IDs to use from the DOM */

function _fdb_get_node_id(node)
{
  // console.log('Checking '+node+' w/id '+node.id+' w/name '+node.name);
  if (node===null) return false;
  else if (node.id) return node.id;
  else if ((node.tagName=='A') && (node.name))
    return node.name;
  else return false;
}

function _fdb_get_parent_name(node)
{
  var parent=node.parentNode;
  if ((parent) && (parent.tagName==='A') && (node.name))
    return node.name;
  else return false;
}

function fdbGuessAnchor(about)
{
  /* This looks around a DOM element to try to find an ID to use as a
     target for a URI.  It especially catches the case where named
     anchors are used. */
  // console.log('Guessing anchors for '+about+' '+about.tagName);
  var probe=_fdb_get_node_id(about);
  if (probe) return probe;
  else if (probe=_fdb_get_parent_name(about)) return probe;
  else if (probe=_fdb_get_node_id(fdbNextElement(about))) return probe;
  else if (probe=_fdb_get_node_id(fdbPreviousElement(about))) return probe;
  else {
    var embedded_anchors=fdbGetChildrenByTagName(about,'A');
    if (embedded_anchors==null) return null;
    var i=0;
    while (i<embedded_anchors.length) 
      if (probe=_fdb_get_node_id(embedded_anchors[i])) return probe;
      else i++;
    return null;}
}

var _fdb_idcounter=0, fdb_idbase=false;

function fdbGetAnchor(about)
{
  /* This does a get anchor and creates an id if neccessary */
  var probe=fdbGuessAnchor(about);
  if (probe) return probe;
  else {
    if (!(fdb_idbase))
      fdb_idbase="TMPID"+(1000000+(Math.floor((1000000-1)*Math.random())))+"S";
    var tmpid=fdb_idbase+_fdb_idcounter++;
    while (document.getElementById(tmpid))
      tmpid=fdb_idbase+_fdb_idcounter++;
    about.id=tmpid;
    return tmpid;}
}

function fdbGetAnchor(about)
{
  /* This does a get anchor and creates an id if neccessary */
  var probe=fdbGuessAnchor(about);
  if (probe) return probe;
  else return fdbForceId(about);
}

