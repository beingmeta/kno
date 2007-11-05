/* -*- Mode: C; -*- */

/* General utility functions */

var xhtmlprefix=false;
var xhtmlnamespace="http://www.w3.org/1999/xhtml";

function fdbText(string)
{
  return document.createTextNode(string);
}

function fdbAddWord(elt,string)
{
  var sep=string.indexOf('$'), space=string.indexOf(' ');
  if (sep<0)
    if (space<0)
      return fdbAppend(elt,document.createTextNode(string));
    else return fdbAppend(elt,document.createTextNode('"'+string+'"'));
  else if (space<0) {
    var wordspan=fdbAppend(elt,string.slice(sep+1));
    fdbAppend(elt,fdbSpan('langid',
			  ((sep==0) ? 'en' : (string.slice(0,sep)))));
    return wordspan;}
  else {
    var wordspan=fdbAppend(elt,'"'+string.slice(sep+1)+'"');
    fdbAppend(elt,fdbSpan('langid',
			  ((sep==0) ? 'en' : (string.slice(0,sep)))));
    return wordspan;}
}

function fdbSpan(classname,content)
{
  var span=
    ((xhtmlprefix)?
     (document.createElementNS(xhtmlnamespace,xhtmlprefix+':span')) :
     (document.createElement('span')));
  span.className=classname;
  if (content)
    if (typeof content == 'string')
      span.appendChild(document.createTextNode(content));
    else span.appendChild(content);
  return span;
}

function fdbNarrative(content,hidden)
{
  var span=
    ((xhtmlprefix)?
     (document.createElementNS(xhtmlnamespace,xhtmlprefix+':span')) :
     (document.createElement('span')));
  if (hidden)
    span.className='hidden_narrative';
  else span.className='narrative';
  if (typeof content == 'string')
    span.appendChild(document.createTextNode(content));
  else span.appendChild(content);
  return span;
}

function fdbDiv(classname)
{
  var div=
    ((xhtmlprefix)?
     (document.createElementNS(xhtmlnamespace,xhtmlprefix+':div')) :
     (document.createElement('div')));
  div.className=classname;
  return div;
}

function fdbAnchor(href,classname,target,content)
{
  var a=
    ((xhtmlprefix)?
     (document.createElementNS(xhtmlnamespace,xhtmlprefix+':a')) :
     (document.createElement('a')));
  a.href=href;
  if (classname) a.className=classname;
  if (target) a.target=target;
  if (content)
    if (typeof content == 'string')
      a.appendChild(document.createTextNode(content));
    else a.appendChild(content);
  return a;
}

function fdbInput(type,varname,val)
{
  var input=document.createElement('input');
  input.type=type; input.name=varname; input.value=val;
  return input;
}

function fdbImg(src,alt,classname)
{
  var span=
    ((xhtmlprefix)?
     (document.createElementNS(xhtmlnamespace,xhtmlprefix+':img')) :
     (document.createElement('img')));
  if (classname) span.className=classname;
  span.src=src; span.alt=alt;
  return span;
}

function fdbNewElt(tagname,namespace)
{
  if (namespace)
    return document.createElementNS(namespace,tagname);
  else return document.createElement(tagname);
}

function fdbXMLtoHTML(node,nsprefix)
{
  if (node.nodeType!=1)
    return document.importNode(node.cloneNode(false),false);
  else {
    var children=node.childNodes;
    var copy=fdbNewElt(((nsprefix)? (nsprefix+node.tagName) : (node.tagName)),
		       xhtmlnamespace);
    var i=0; while (i<children.length)
      fdbAppend(copy,fdbXMLtoHTML(children[i++]));
    return copy;}
}

function fdbByID(id)
{
  return document.getElementById(id);
}

function fdbReplacement(id)
{
  var elt=document.getElementById(id);
  if (elt) 
    return elt.cloneNode(false);
  else return null;
}

function fdbAppend(parent,newchild)
{
  if (typeof newchild == 'string')
    newchild=document.createTextNode(newchild);
  if (newchild instanceof Array) {
    var i=0; while (i<newchild.length) {
      var child=child[i++];
      if (typeof child == 'string')
	child=document.createTextNode(child);
      parent.appendChild(child);}}
  else if (parent)
    parent.appendChild(newchild);
  return newchild;
}

function fdbPrepend(parent,newchild,current)
{
  if (typeof newchild == 'string')
    newchild=document.createTextNode(newchild);
  if (current)
    parent.insertBefore(newchild,current);
  else parent.insertBefore(newchild,parent.firstChild);
  return newchild;
}

function fdbReplace(oldchild,newchild)
{
  if (typeof newchild == 'string')
    newchild=document.createTextNode(newchild);
  oldchild.parentNode.replaceChild(newchild,oldchild);
  return newchild;
}

function fdbParent(node)
{
  return node.parentNode;
}

function fdbAllChildren(node)
{
  return node.childNodes;
}

function fdbChildren(node,attrib,val)
{
  var results=[], i=0;
  var children=node.childNodes;
  if ((!(children)) || (children.length==0)) return results;
  if (arguments.length==1)
    while (i<children.length)
      if (children[i].nodeType==1)
	results.push(children[i++]);
      else i++;
  else if (arguments.length==2)
    while (i<children.length)
      if ((children[i].nodeType==1) &&
	  (children[i].hasAttribute(attrib)))
	results.push(children[i++]);
      else i++;
  else while (i<children.length)
    if ((children[i].nodeType==1) &&
	(children[i].getAttribute(attrib)==val))
      results.push(children[i++]);
    else i++;
  return results;
}

function fdbGetAttrib(node,attrib)
{
  return node.getAttribute(attrib);
}
function fdbSetAttrib(node,attrib,value)
{
  return node.setAttribute(attrib,value);
}
function fdbRemoveAttrib(node,attrib)
{
  return node.removeAttribute(attrib);
}
function fdbCopyAttribs(to,from)
{
  var i=2; while (i<arguments.length) {
    var attrib=arguments[i++];
    var value=from.getAttribute(attrib);
    to.setAttribute(attrib,value);}
  return to;
}

function fdbFindAttrib(node,attrib)
{
  var val=false;
  while (node)
    if ((node.nodeType==1) &&
	(node.getAttribute) &&
	(val=node.getAttribute(attrib)))
      return val;
    else node=node.parentNode;
  return false;
}

function fdbFindClass(node,classname)
{
  if (typeof classname == 'string') {
    while (node)
      if ((node.nodeType==1) && (node.className==classname))
	return node;
      else node=node.parentNode;
    return node;}
  else if (classname instanceof Array) {
    while (node) {
      var i=0; while (i<classname.length)
	if (node.className==classname[i++])
	  return node;
      node=node.parentNode;}
    return node;}
  else return false;
}

/* Generic functions */

var oidvalues=[], slottables=[];

function fdbGet(f,s)
{
  if (!(f)) return f;
  else if (!(s)) return s;
  else if (f instanceof Array) {
    var results=[], i=0;
    while (i<f.length) {
      var item=f[i++];
      var val=fdbGet(item,s);
      if (val instanceof Array)
	results.concat(val);
      else results.push(val);}
    return results;}
  else if (s instanceof Array) {
    var results=[];
    var j=0; while (j<s.length) {
      var val=fdbGet(f,s[j++]);
      if (val instanceof Array)
	results.concat(val);
      else results.push(val);}
    return results;}
  else if (typeof s == 'string')
    if (typeof f == 'object')
      return f.getAttribute(s);
    else if (typeof f == 'string') {
      var slotvals=slottables[s];
      if (slotvals) {
	var cached=slotvals[f];
	if (cached) return cached;
	cached=_fdb_slotget(f,s);
	slotvals[f]=cached;
	return cached;}
      else {
	var oidval=oidvals[f];
	if (oidval)
	  return oidval[s];
	else {
	  oidval=_fdb_oidget(f);
	  oidvals[f]=oidval;
	  return oidval[s];}}}
    else return false;
  else if (typeof s == 'function')
    return s(f);
  else return false;
}

/* Utility functions for dealing with generated XHTML */

function fdbOid2Id(oid,tag)
{
  if (oid.slice(0,2)==':@') {
    var slash=oid.indexOf('/',2);
    if (slash)
      return tag+'_'+oid.slice(2,slash)+'_'+oid.slice(slash+1);
    else return false;}
  else return false;
}

function fdbGetOid(elt)
{
  var oid;
  while (elt) 
    if (elt.nodeType!=1) elt=elt.parentNode;
    else if (elt.oid) return elt.oid;
    else if ((elt.getAttribute) &&
	     (oid=elt.getAttribute('oid')))
      return oid;
    else elt=elt.parentNode;
  return null;
}

function fdbGetOidElt(elt)
{
  while (elt) 
    if (elt.nodeType!=1) elt=elt.parentNode;
    else if (elt.oid) return elt;
    else if ((elt.getAttribute) &&
	     (elt.getAttribute('oid')))
      return elt;
    else elt=elt.parentNode;
  return null;
}

function fdbOid2Elt(oid,tag)
{
  if (oid.slice(0,2)==':@') {
    var slash=oid.indexOf('/',2);
    if (slash) {
      var eltid=tag+'_'+oid.slice(2,slash)+'_'+oid.slice(slash+1);
      return fdbByID(eltid);}
    else return false;}
  else return false;
}

/* Legacy */

function get_elt_ref(elt,attribute)
{
  var id=fdbGet(elt,attribute);
  if (id) return fdbByID(id);
  else return false;
}

function lispval(string)
{
  if (string.charAt)
    if (string.charAt(0)==':') return string.slice(1);
    else return string;
  else return string;
}

function findChild(elt,classname)
{
  if ((elt) && (elt.childNodes)) {
    var nodes=elt.childNodes;
    var i=0; while (i<nodes.length) {
      var node=nodes[i++];
      if ((node.nodeType==1) &&
	  (node.className==classname))
	return node;}
    i=0; while (i<nodes.length) {
      var node=nodes[i++];
      if (node.nodeType==1) {
	var found=findChild(node,classname);
	if (found) return found;}}}
  return null;
}

/* Other utilities */

function trimString(string,start,end)
{
  if (!(start)) start=0;
  if (!(end)) end=string.length;
  while ((start<end) &&
	 (string.charAt(start)==' '))
    start++;
  while ((end>start) &&
	 ((string.charAt(end-1)==' ')||
	  (string.charAt(end-1)=='.')||
	  (string.charAt(end-1)==','))) end--;
  return string.slice(start,end);
}

/* Clickit commands */

var selected_image=null;

function _fdb_clickit_command(name)
{
  var elt=fdbByID('command_elt');
  elt.value=name;
  document.forms[0].submit();
}

function _fdb_clickit_action(url)
{
  document.forms[0].action=url;
  document.forms[0].submit();
}

function _fdb_clickit_action_command(url,name)
{
  var elt=fdbByID('command_elt');
  elt.value=name;
  document.forms[0].action=url;
  document.forms[0].submit();
}

function _fdb_clickit_mouseover(evt)
{
  var elt=evt.target;
  if (elt) elt.style.color='blue';
}

function _fdb_clickit_mouseout(evt)
{
  var elt=evt.target;
  if (elt) elt.style.color=null;
}

/* tab functions */

function _fdb_tab_click(evt)
{
  var elt=evt.target;
  if (elt) {
    var select_var=fdbGet(elt,'selectvar');
    var content_id=fdbGet(elt,'contentid');
    var content=fdbByID(content_id);
    var select_elt=fdbByID(select_var+'_INPUT');
    var parent=elt.parentNode;
    var others=parent.childNodes;
    var toggle_on=true;
    evt.preventDefault(); evt.cancelBubble=true;
    if (select_elt) select_elt.value=content_id;
    var i=0; while (i<others.length) {
      var node=others[i++];
      if ((node.nodeType==1) &&
	  ((node.className=='tab') ||
	   (node.className=='selected_tab'))) {
	var cid=fdbGet(node,'contentid');
	var cdoc=fdbByID(cid);
	node.className='tab';
	if (cdoc==content)
	  if ((cdoc.style.display) &&
	      (cdoc.style.display!='none'))
	    toggle_on=false;
	if (cdoc) cdoc.style.display='none';}}
    if (toggle_on) {
      elt.className='selected_tab';
      elt.style.textDecoration='none';
      content.style.display='block';}
    else {
      elt.className='tab';
      elt.style.textDecoration='underline';
      content.style.display='none';}
  return false;}
}

function _fdb_tab_mouseover(evt)
{
  var elt=evt.target;
  if ((elt) &&
      ((elt.className=='tab') || (elt.className=='selected_tab'))) 
    elt.style.textDecoration='underline';
}

function _fdb_tab_mouseout(evt)
{
  var elt=evt.target;
  if (elt) 
    elt.style.textDecoration='none';
}

function fdb_tab_select(container_id,content_id)
{
  var container=fdbByID(container_id);
  var selected=false;
  if (container) {
    var others=container.childNodes;
    var i=0; while (i<others.length) {
      var node=others[i++];
      if ((node.nodeType==1) &&
	  ((node.className=='tab') ||
	   (node.className=='selected_tab'))) {
	var cid=fdbGet(node,'contentid');
	var cdoc=fdbByID(cid);
	if (cid==content_id) {
	  node.className='selected_tab';
	  if (cdoc) cdoc.style.display='block';}
	else {
	  node.className='tab';
	  if (cdoc) cdoc.style.display='none';}}}}
}

/* input help functions */

function _fdb_input_help_focus(evt)
{
  var elt=evt.target;
  if ((elt) && (elt.getAttribute)) {
    var help_id=fdbGet(elt,'INPUTHELP');
    if (help_id) {
      var help_elt=fdbByID(help_id);
      if ((help_elt) && (help_elt.style))
	help_elt.style.display='block';}}
}

function _fdb_input_help_blur(evt)
{
  var elt=evt.target;
  if ((elt) && (elt.getAttribute)) {
    var help_id=fdbGet(elt,'INPUTHELP');
    if (help_id) {
      var help_elt=fdbByID(help_id);
      if ((help_elt) && (help_elt.style))
	help_elt.style.display='none';}}
}

function _fdb_autoprompt_focus(evt)
{
  var elt=evt.target;
  if (elt.className=='autoprompt_empty') {
    elt.className='autoprompt'; elt.value='';}
  _fdb_input_help_focus(evt);
}

function _fdb_autoprompt_blur(evt)
{
  var elt=evt.target;
  if ((elt.className=='autoprompt') && (elt.value=='')) {
    elt.className='autoprompt_empty';
    elt.value=fdbGet(elt,'prompt');}
  _fdb_input_help_blur(evt);
}

function autoprompt_setup()
{
  var elements=document.getElementsByTagName('INPUT');
  var i=0; if (elements) while (i<elements.length) {
    var elt=elements[i++];
    if ((elt.type=='text') &&
	(elt.className=='autoprompt'))
      if (elt.value=='') {
	elt.value=fdbGet(elt,'prompt');
	elt.className='autoprompt_empty';}
      else if (elt.value==fdbGet(elt,'prompt'))
	elt.className='autoprompt_empty';}
}

// Removes autoprompt text from empty fields
function autoprompt_cleanup()
{
  var elements=document.getElementsByTagName('INPUT');
  var i=0; if (elements) while (i<elements.length) {
    var elt=elements[i++];
    if (elt.className=='autoprompt_empty')
      elt.value="";}
}

/* hide/show toggles */

function _fdb_hideshow_toggle(evt)
{
  var elt=evt.target, content_id=null, content;
  var reveal;
  while (elt) {
    content_id=fdbGet(elt,'content');
    if (content_id) break;
    else elt=elt.parentNode;}
  if (!(elt)) return true;
  if (content_id)
    if (content_id=='_next')
      content=elt.nextSibling;
    else if (content_id=='_previous')
      content=elt.nextSibling;
    else if (content_id=='_parent')
      content=elt.parentNode;
    else content=fdbByID(content_id);
  if (!(content)) return true;
  else if (content.style.display=='none')
    fdbSetHideShow(elt,true);
  else fdbSetHideShow(elt,false);
  evt.preventDefault(); evt.cancelBubble=true;
  return false;
}

function fdbSetHideShow(elt,visible)
{
  var content_id=fdbGet(elt,'content'), content=false;
  if (!(content_id)) return;
  else if (content_id=='_next')
    content=elt.nextSibling;
  else if (content_id=='_previous')
    content=elt.nextSibling;
  else if (content_id=='_parent')
    content=elt.parentNode;
  else content=fdbByID(content_id);
  if (!(content)) return;
  if (visible) {
    // Update the display style
    var display_style='inline';
    if (content.getAttribute('display'))
      display_style=content.getAttribute('display');
    else if ((content.tagName=='DIV') || (content.tagName=='P') ||
	     (content.tagName=='UL') || (content.tagName=='OL') ||
	     (content.tagName=='LI'))
      display_style='block';
    content.style.display=display_style;
    // Update the element title
    if (elt.getAttribute('OPENTITLE'))
      elt.title=elt.getAttribute('OPENTITLE');
    else elt.title='click for less';}
  else {
    content.style.display='none';
    // Update the element title
    if (elt.getAttribute('CLOSEDTITLE'))
      elt.title=elt.getAttribute('CLOSEDTITLE');
    else elt.title='click for more';}
  var i=0, children=elt.childNodes;
  while (i<children.length) {
    var child=children[i++];
    if (child.nodeType==1)
      if (child.className=='whenHidden')
	if (visible) child.style.display='none';
	else child.style.display='inline';
      else if (child.className=='whenVisible')
	if (visible) child.style.display='inline';
	else child.style.display='none';}
}

function fdbHideShow(eltid,hiddenmsg,visiblemsg,initvisible)
{
  var topelt=fdbSpan('hideshow','[');
  if (!(hiddenmsg)) hiddenmsg='more';
  if (!(visiblemsg)) visiblemsg='less';
  var whenHidden=fdbAppend(topelt,fdbSpan('whenHidden',hiddenmsg));
  var whenVisible=fdbAppend(topelt,fdbSpan('whenVisible',visiblemsg));
  fdbAppend(topelt,']');
  topelt.setAttribute('content',eltid);
  topelt.onclick=_fdb_hideshow_toggle; 
  // When initially visible, reset tooltip and display information
  if (initvisible) {
    topelt.title='click to see less';
    whenHidden.display='none'; whenHidden.display='inline';}
  else topelt.title='click to see more';
  return topelt;
}

/* collapsars */

/* A collapsar is a 'button' which hides or reveals everything
   after it up to the 'collapsar stop'. */

function fdb_make_collapsar(collapsed_text,expanded_text)
{
  var collapsar=document.createElement('span');
  var when_collapsed=document.createElement('span');
  var when_expanded=document.createElement('span');
  collapsar.className='collapsar';
  collapsar.onclick=collapsar_click;
  when_collapsed.className='collapsed';
  fdbAppend(when_collapsed,fdbText(collapsed_text));
  when_collapsed.title='Click to expand';
  when_expanded.className='expanded';
  fdbAppend(when_expanded,fdbText(expanded_text));
  when_expanded.title='Click to collapse';
  when_expanded.style.display='none';
  fdbAppend(collapsar,when_collapsed);
  fdbAppend(collapsar,when_expanded);
  return collapsar;
}

function fd_collapsar_stop()
{
  var stop=document.createElement('span');
  stop.className='collapsar_stop';
  stop.style.display='none';
  return stop;
}

// This causes the .expanded and .collapsed children of this 
// node to be appropriately hidden or revealed
function _fdb_collapsar_set_expanded(elt,flag)
{
  var children=elt.childNodes, i=0;
  while (i<children.length)
    if (children[i].nodeType==1)
      if (children[i].className=='expanded') {
	if (flag) children[i].style.display='inline';
	else children[i].style.display='none';
	i++;}
      else if (children[i].className=='collapsed') {
	if (flag) children[i].style.display='none';
	else children[i].style.display='inline';
	i++;}
      else  i++;
    else i++;
}

function _fdb_collapsar_expand(elt)
{
  collapsar_set_expanded(elt,true);
  var sibling=elt.nextSibling;
  while (sibling) {
    if (sibling.className=='collapsar_stop') break;
    sibling.style.display='inline';
    if (sibling.className=='collapsar') break;
    sibling=sibling.nextSibling;}
}

function _fdb_collapsar_collapse(elt)
{
  collapsar_set_expanded(elt,false);
  var sibling=elt.nextSibling;
  while (sibling) {
    if (sibling.className=='collapsar_stop') break;
    else if (sibling.className=='collapsar')
      collapsar_set_expanded(sibling,false);
    sibling.style.display='none';
    sibling=sibling.nextSibling;}
}

function _fdb_collapsar_click(evt)
{
  var elt=evt.target;
  while (elt)
    if (elt.className=='collapsar') break;
    else elt=elt.parentNode;
  if (elt) {
    var next_node=elt.nextSibling;
    if (next_node) {
      if (next_node.style.display=='none')
	_fdb_collapsar_expand(elt);
      else _fdb_collapsar_collapse(elt);}}
}

/* Hot checks are spans where clicking anywhere in the span
   checks or unchecks the box. */

function _fdb_hotcheck_click(evt)
{
  var target=evt.target;
  var elt=fdbFindClass(target,'hotcheck');
  var unique_elt=false;
  if (elt) {
    var i=0, elts=elt.childNodes;
    while (i<elts.length) 
      if ((elts[i].nodeType==1) &&
	  (elts[i].tagName=='INPUT')) {
	if (elts[i]==target) {
	  elts[i].checked=true;
	  if (elts[i].type=='radio') unique_elt=elts[i];
	  elt.style.fontWeight='bold';}
	else if (elts[i].checked) {
	  elts[i].checked=false;
	  elt.style.fontWeight='normal';}
	else {
	  elts[i].checked=true;
	  if (elts[i].type=='radio') unique_elt=elts[i];
	  elt.style.fontWeight='bold';}
	break;}
      else i++;}
  if (unique_elt) {
    var inputs=document.getElementsByTagName('input');
    var i=0; while (i<inputs.length)
      if ((inputs[i].type=='radio') &&
	  (inputs[i].name==unique_elt.name) &&
	  (inputs[i]!=unique_elt)) {
	var parent=fdbFindClass(inputs[i],'hotcheck');
	if (parent) parent.style.fontWeight='normal';
	i++;}
      else i++;}
}

/* Seenotes */

var live_note=false;

function seenote_mouseover(evt)
{
  var target=evt.target;
  var note=fdbByID(target.getAttribute('seenote'));
  window.status='seenote_mouseover '+target.getAttribute('seenote');
  if (note) {
    if ((live_note) && (live_note!=note)) {
      live_note.style.display='none'; live_note=false;}
    note.style.display='block';
    live_note=note;}
}

function seenote_mouseout(evt)
{
  var target=evt.target;
  var note=fdbByID(target.getAttribute('seenote'));
  if (note) {
    note.style.display='none';
    if (live_note==note) live_note=false;}
}

function seenote_setup()
{
  var elements=document.getElementsByTagName('*');
  var i=0; if (elements) while (i<elements.length) {
    var elt=elements[i++];
    if (elt.getAttribute('seenote')) {
      elt.onmouseover=seenote_mouseover;
      elt.onmouseout=seenote_mouseout;}
    if (elt.className=='hotcheck')
      elt.onclick=_fdb_hotcheck_click;
  }
}

/* Font size controls */

function increase_font(evt)
{
  var elt=evt.target;
  var parent=elt.parentNode;
  var font_info=parent.style.fontSize;
  if (font_info) {
    if (font_info.match(/\d+[%]$/)) {
      var percent=parseInt(font_info.slice(0,-1));
      parent.style.fontSize=(percent+25).toString()+"%";}}
  else parent.style.fontSize="125%";
}

function decrease_font(evt)
{
  var elt=evt.target;
  var parent=elt.parentNode;
  var font_info=parent.style.fontSize;
  if (font_info) {
    if (font_info.match(/\d+[%]$/)) {
      var percent=parseInt(font_info.slice(0,-1));
      if (percent>25) {
	parent.style.fontSize=(percent-25).toString()+"%";}}}
  else parent.style.fontSize="75%";
}

function show_all(evt)
{
  var elt=evt.target;
  var parent=elt.parentNode;
  var font_info=parent.style.fontSize;
  if (font_info) {
    if (font_info.match(/\d+[%]$/)) {
      var percent=parseInt(font_info.slice(0,-1));
      if (percent>25) {
	parent.style.fontSize=(percent-25).toString()+"%";}}}
  else parent.style.fontSize="75%";
}

/* Miscellaneous */

function refile_form(evt)
{
  var target=evt.target;
  if (target.form) target.form.submit();
}

// Adding search engines
var browser_coverage="Mozilla/Firefox/Netscape 6";

function addBrowserSearchPlugin(spec,name,cat)
{
  if ((typeof window.sidebar == "object") &&
      (typeof window.sidebar.addSearchEngine == "function")) 
    window.sidebar.addSearchEngine (spec+'.src',spec+".png",name,cat);
  else alert(browser_coverage+" is needed to install a search plugin");
}

function fdbAjaxRequest()
{
  if (window.XMLHttpRequest)
    return new window.XMLHttpRequest();
  else if (typeof ActiveXObject != "undefined") {
    var req=new ActiveXObject("Microsoft.XMLHTTP");
    return req;}
  else {
    alert("Can't generate Ajax Request");
    return false;}
}

function printfire()
{
  if (document.createEvent) {
    printfire.args = arguments;
    var ev = document.createEvent("Events");
    ev.initEvent("printfire", false, true);
    dispatchEvent(ev);}
}

/* Logging */

function fdbMessage(aMessage)
{
  console.log(aMessage);
  //  var consoleService =
  //    Components.classes["@mozilla.org/consoleservice;1"].getService
  //      (Components.interfaces.nsIConsoleService);
  //  consoleService.logStringMessage("fdb: " + aMessage);
}


function fdbEventString(evt)
{
  /* Useful for debugging, this returns event information. */
  return evt.type+";"+
    ((evt.altKey)?("alt;"):"")+
    ((evt.shiftKey)?("shift;"):"")+
    ((evt.ctrlKey)?("ctrl;"):"");
}

/* DYTOC (Dynamic table of contents) support */

var dytoc_picked=null, dytoc_default=null, dytoc_displayed=null;

function dytoc_display_sectid(event)
{
  var cur=dytoc_displayed;
  var elt=fdbFindClass(event.target,'sectref');
  if (elt==null) return;
  var sectid=fdbGetAttrib(elt,"SECTID");
  var section=fdbByID(sectid);
  elt.style.textDecoration='underline';
  if (section==dytoc_displayed) return;
  if (dytoc_displayed) dytoc_displayed.style.display='none';
  dytoc_displayed=section;
  section.style.display='block';
  if (cur) cur.style.display='none';
}

function dytoc_undisplay_sectid(event)
{
  var elt=fdbFindClass(event.target,'sectref');
  if (elt==null) return;
  elt.style.textDecoration='none';
  var sectid=fdbGetAttrib(elt,"SECTID");
  var section=fdbByID(sectid);
  if ((section) && (section != dytoc_default))
    section.style.display='none';
  dytoc_display_default();
}

function dytoc_display_default()
{
  if (dytoc_default==null) return;
  if (dytoc_default==dytoc_displayed) return;
  if (dytoc_displayed!=null)
    dytoc_displayed.style.display='none';
  if (dytoc_default!=null)
    dytoc_default.style.display='block';
  dytoc_displayed=dytoc_default;
}

function dytoc_set_default(event)
{
  var elt=fdbFindClass(event.target,'sectref');
  if (elt==null) return;
  var sectid=fdbGetAttrib(elt,"SECTID");
  var section=fdbByID(sectid);
  if (section!=null) {
    if (dytoc_picked) fdbSetAttrib(dytoc_picked,'picked','no');
    dytoc_picked=elt;
    fdbSetAttrib(elt,'picked','yes');
    dytoc_default=section;}
}

function dytoc_set_default_to(sectid,sectrefid)
{
  var sectref=fdbByID(sectrefid);
  var section=fdbByID(sectid);
  if (dytoc_picked!=null) 
    fdbSetAttrib(dytoc_picked,'picked','no');
  dytoc_picked=sectref;
  if (sectref)
    fdbSetAttrib(sectref,'picked','yes');
  dytoc_default=section;
  if (dytoc_displayed==null)
    dytoc_display_default();
}

