/* -*- Mode: C; -*- */

/* Richtips are overlayed tooltips for metakeys, displayed
   in translucent rectangles.
   This file works in conjunction with richtips.css
*/

var richform_submit_on_empty=true;

function richform_container(target)
{
  var container_id=target.getAttribute('CONTAINER'), container=null;
  if (container_id) {
    container=document.getElementById(container_id);}
  else {
    container=target;
    while (container) 
      if (container.className=='container') break;
      else container=container.parentNode;}
  return container;
}

function richform_find_input(node,type,varname,value)
{
  if (1) {}
  else if ((node.nodeType==1) && (node.tagName=='INPUT'))
    fdbMessage('Looking for '+type+'('+varname+'='+value+') in '+node+
	       ' '+node.type+' '+node.name+'='+node.value);
  else fdbMessage('Looking for '+type+'('+varname+'='+value+') in '+node);
  if (node.nodeType==1)
    if ((node.tagName=='INPUT') &&
	(node.type==type) &&
	(node.name==varname) &&
	(node.value==value)) 
      return node;
    else {
      var nodes=node.childNodes;
      var i=0; while (i<nodes.length) {
	var elt=nodes[i++];
	if (elt.nodeType==1) {
	  var inside=richform_find_input(elt,type,varname,value);
	  if (inside) return inside;}}
      return null;}
  else return null;
}

var handle_langids=true;

function richform_string_entry(evt,defer)
{
  var ch=evt.charCode, kc=evt.keyCode;
  if ((ch==0x3b) || (kc==13)) {
    var target=evt.target;
    evt.cancelBubble=true;
    while (target)
      if (target.tagName=='INPUT')
	break;
      else target=target.parentNode;
    if (target==null) {
      fdbMessage('keypress failed, no target');
      return false;}
    var varname=target.getAttribute('VAR');
    if (varname==null) {
      fdbMessage('keypress failed, no var');
      return false;}
    var container=richform_container(target);
    if (container==null) {
      fdbMessage('keypress failed, no container');
      return false;}
    var stringval=target.value;
    if (stringval=='') {
      if (richform_submit_on_empty) target.form.submit();
      return false;}
    var checkbox=richform_find_input
      (container,'checkbox',varname,stringval);
    if (checkbox) {
      // fdbMessage('Found checkbox '+checkbox);
      checkbox.checked=true;
      target.value='';
      return false;}
    else checkbox=fdbCheckbox(varname,stringval);
    var stringval_span=fdbSpan("stringval");
    stringval_span.setAttribute('new','yes');
    checkbox.checked=true;
    fdbAppend(stringval_span,checkbox);
    if ((handle_langids) &&
	(stringval.indexOf('$')>=0) &&
	(stringval.indexOf('$')<4)) {
      var dollar=stringval.indexOf('$');
      var langid=stringval.slice(0,dollar);
      var wordform=stringval.slice(dollar+1);
      var lang_span=fdbNewElt('sub');
      lang_span.className='langids'
      fdbAppend(stringval_span,wordform);
      fdbAppend(lang_span,langid);
      fdbAppend(stringval_span,lang_span);}
    else fdbAppend(stringval_span,stringval);
    fdbAppend(container,stringval_span);
    fdbAppend(container," ");
    target.value="";
    return false;}
  else if (defer)
    return defer(evt);
  else return true;
}

var getmeanings_uri="getmeanings.fdcgi";

function richform_concept_entry(evt,defer)
{
  var ch=evt.charCode, kc=evt.keyCode;
  if ((ch==0x3b) || (kc==13)) {
    var target=evt.target;
    while (target)
      if (target.tagName=='INPUT')
	break;
      else target=target.parentNode;
    if (target==null) {
      fdbMessage('keypress failed, no target');
      return false;}
    var varname=target.getAttribute('VAR');
    if (varname==null) {
      fdbMessage('keypress failed, no var');
      return false;}
    var container=richform_container(target);
    if (container==null) {
      fdbMessage('keypress failed, no container');
      return false;}
    var stringval=target.value;
    if (stringval=='') {
      if (richform_submit_on_empty) target.form.submit();
      return false;}
    richform_getmeanings(stringval,varname,container);
    target.value='';
    return false;}
  else if (defer)
    return defer(evt);
  else return true;
}

function richform_getmeanings(term,varname,container)
{
  var req=new XMLHttpRequest();
  req.onreadystatechange=function() {
    if ((req.readyState == 4) && (req.status == 200)) {
      richform_gotmeanings
      (req.responseXML.documentElement,varname,container);}}
  req.open("GET",getmeanings_uri+"?TERM="+encodeURIComponent(term),true);
  req.send(null);
}

function richform_gotmeanings(doc,varname,container)
{
  var elements=doc.childNodes;
  var i=0; while (i<elements.length) {
    var elt=elements[i++];
    if (elt.nodeType!=1) continue;
    var oid=elt.getAttribute('oid');
    var selected=elt.hasAttribute('selected');
    var oid_check=richform_find_input(container,"checkbox",varname,oid);
    if (oid_check) {
      fdbMessage('found checkbox '+oid_check+' checked='+
		 oid_check.checked+'/'+oid_check.defaultChecked);
      if (oid_check.checked==oid_check.defaultChecked)
	if (selected) oid_check.checked=true;
	else oid_check.checked=false;
      return;}
    oid_check=fdbCheckbox(varname,oid,selected);
    var oid_elt=fdbSpan('concept',oid_check);
    oid_elt.setAttribute('oid',oid);
    if (elt.hasAttribute('gloss'))
      oid_elt.setAttribute('gloss',elt.getAttribute('gloss'));
    if (elt.hasAttribute('dterm')) {
      oid_elt.setAttribute('dterm',elt.getAttribute('dterm'));
      oid_elt.setAttribute('text',elt.getAttribute('text'));
      fdbAppend(oid_elt,elt.getAttribute('dterm'));}
    else if (elt.hasAttribute('norm')) {
      oid_elt.setAttribute('text',elt.getAttribute('norm'));
      fdbAppend(oid_elt,elt.getAttribute('norm'));}
    else fdbAppend(oid_elt,oid);
    fdbAppend(container,oid_elt);}
}


