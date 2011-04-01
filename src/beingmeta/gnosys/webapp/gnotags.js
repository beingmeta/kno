/* -*- Mode: JavaScript; -*- */

/* This file supports semantic tagging in HTML forms, including AJAX access
    to server-side disambiguation.

   It relies on fdweb/domutils.js and fdweb/handlers.js

   $Id$
*/

/* Design notes:
     We assume that the application (the current page) has
     an HTML form which includes a collection of displayed
     textual and conceptual (OIDs) tags.  It also assumes that
     there is a parent element which contains all of the tags.

     Each displayed tag consists of a 'checkspan' element (from fdweb)
     which contains an input checkbox and a descriptive tag.

     When a tag is added (by gnoAddTag or friends), it finds or creates
      the appropriate checkspan.  It then queues the corresponding term
      for disambiguation when appropriate.  The queued terms are passed
      to the server through an Ajax call.  The response to the call is used
      to either replace nodes with disambiguated concepts or add concepts
      to the display as unchecked selections.

*/

var _debug_gnotags=false;
var _debug_gnotagspans=false;
var _debug_disambig=false;

function gnoFindTagSpan(tagtop,varname,val)
{
  var nodes=fdbGetChildrenByTagName(tagtop,'input');
  var i=0; while (i<nodes.length) {
    var node=nodes[i++];
    if (_debug_gnotagspans)
      fdbLog('Looking at input type='+node.type+', name='+
	     node.name+', value='+node.value+
	     ', return='+
	     ((node.type=='checkbox') &&
	      (node.name==varname) &&
	      ((node.value==val) || (fdbHasAttrib(node,'term',val)))));
    if ((node.type=='checkbox') &&
	(node.name==varname) &&
	((node.value==val) || (fdbHasAttrib(node,'term',val))))
      return fdbGetParentByClassName(node,'checkspan');}
  return null;
}

function gnoNewTagSpan(varname,tag,checked)
{
  if ((_debug) || (_debug_gnotags) || (_debug_gnotagspans))
    if (typeof tag == 'string') 
      fdbLog('Making checkspan for '+tag+' with checked='+checked);
    else fdbLog('Making checkspan for '+
		tag['oid']+'='+tag['term']+
		' with checked='+checked);
  if (typeof tag == 'string') {
    var checkbox=fdbCheckbox(varname,tag,checked);
    var checkspan=fdbSpan('checkspan',checkbox,tag);
    if (checked) checkspan.setAttribute('ischecked','yes');
    checkspan.setAttribute('term',tag);
    checkbox.setAttribute('term',tag);
    return checkspan;}
  else {
    var oid=tag['oid'], dterm=tag['dterm'], gloss=tag['gloss'];
    var checkbox=fdbCheckbox('TAGS',oid,checked);
    var checkspan=fdbSpan("checkspan",checkbox,dterm);
    checkspan.setAttribute('term',tag['term']);
    checkbox.setAttribute('term',tag['term']);
    checkspan.title=gloss;
    checkspan.setAttribute('gloss',gloss);
    checkspan.setAttribute('oid',oid);
    checkspan.setAttribute('dterm',dterm);
    if (checked)
      checkspan.setAttribute('ischecked','yes');
    return checkspan;}
}

function gnoGetTagSpans(elt)
{
  var children=fdbGetChildrenByClassName(elt,'tagspans');
  if ((children==null) || (children.length==0)) return elt;
  else return children[0];
}

function gnoTagSpan(tagtop,varname,term,checked)
{
  var tagspan=gnoFindTagSpan(tagtop,varname,term);
  var tagspans=gnoGetTagSpans(tagtop);
  if ((_debug) || (_debug_gnotags) || (_debug_gnotagspans))
    if (tagspan)
      fdbLog('Found tagspan for '+varname+'='+term);
    else fdbLog('No tagspan for '+varname+'='+term);
  if (tagspan==null) {
    tagspan=gnoNewTagSpan(varname,term,checked);
    if ((_debug) || (_debug_gnotags) || (_debug_gnotagspans))
      fdbLog("Got new tagspan "+tagspan);
    if (checked)
      fdbPrepend(tagspans,tagspan," ");
    else fdbAppend(tagspans," ",tagspan);}
  return tagspan;
}

function gnoAddTextTag(tagtop,tag,checked,todisambig)
{
  if (typeof tagtop == 'string') tagtop=$(tagtop);
  if (typeof checked == 'undefined') checked=false;
  var varname=tagtop.getAttribute('VARNAME');
  var tagtext=tag;
  if (tag[0]=='\'') tagtext=tag.slice(1);
  if ((_debug) || (_debug_gnotags))
    fdbLog("Adding"+((checked) ? " checked " : " ")+
	   "text tag for "+varname+"="+tag+" to "+tagtop);
  if (tag[0]!='\'') todisambig.push(tag);
  return gnoTagSpan(tagtop,varname,tagtext,checked);
}

function _gnoAddTextTags(tagtop,tags,checked,todisambig)
{
  if ((_debug) || (_debug_gnotags))
    fdbLog("Adding "+tags+" to "+tagtop+"; todisambig="+todisambig);
  if (typeof tags == "string")
    if (tags.indexOf(';')>0) {
      tags=tags.split(';');
      var i=0; while (i<tags.length) {
	var tag=tags[i++];
	gnoAddTextTag(tagtop,tag,checked,todisambig);}}
    else gnoAddTextTag(tagtop,tags,checked,todisambig);
  else {
    var i=0; while (i<tags.length) 
      gnoAddTextTag(tagtop,tags[i++],checked,todisambig);}
}

function gnoAddTextTags(tagtop,tags,checked,do_disambig)
{
  var todisambig=[];
  if (typeof do_disambig === 'undefined') do_disambig=true;
  if (typeof checked === 'undefined') checked=false;
  if (typeof tagtop === 'string') tagtop=$(tagtop);
  if ((_debug) || (_debug_gnotags))
    fdbLog('Adding text tags '+tags+' '+typeof(tags)+' '+tags.length);
  if (typeof tags == 'string')
    if (tags.indexOf(';')>0) {
      var terms=tags.split(';');
      var i=0; while (i<terms.length)
	gnoAddTextTag(tagtop,terms[i++],checked,todisambig);}
    else _gnoAddTextTags(tagtop,tags,checked,todisambig);
  else _gnoAddTextTags(tagtop,tags,checked,todisambig);
  if ((_debug) || (_debug_gnotags))
    fdbLog('todisambig='+todisambig.toString());
  if (do_disambig)
    gnoDisambigTerms(todisambig,tagtop);
  return todisambig;
}

var tagelt_id="TAGS";
var lookupterms_uri="/lookupterms.fdcgi";

function gnoHandleTags(tagtext,checked,eltid)
{
  if (typeof eltid == 'undefined') eltid=tagelt_id;
  gnoAddTextTags($(eltid),tagtext,checked,true);
}

function gnoDisambigTerms(terms,tagtop)
{
  if ((terms==null) || (terms.length==0)) return;
  if ((_debug) || (_debug_gnotags) || (_debug_disambig))
    fdbLog('Disambiguating '+terms.length+' terms: '+terms+" into "+tagtop);
  if (typeof tagtop == "string") tagtop=$(tagtop);
  var req=new XMLHttpRequest();
  req.onreadystatechange=function() {
    if ((req.readyState == 4) && (req.status == 200)) {
      _gno_handle_disambig(req.responseText.parseJSON(),tagtop);}};
  var uri=lookupterms_uri+'?';
  var i=0; while (i<terms.length)
    uri=uri+'TERMS='+encodeURIComponent(terms[i++])+'&';
  if ((_debug) || (_debug_gnotags) || (_debug_disambig))
    fdbLog('making ajax request: '+uri);
  req.open("GET",uri,true);
  req.send(null);
}

function gnoTagSelectedText(fromid,toid)
{
  var elt=$(fromid);
  if (typeof toid == 'undefined') toid=tagelt_id;
  var selection=fdbGetSelection(elt);
  if (selection)
    gnoAddTextTags($(toid),selection,true,true);
}

function _gno_handle_disambig(disambig,tagtop)
{
  var varname=tagtop.getAttribute('VARNAME');
  if ((_debug) || (_debug_gnotags) || (_debug_disambig))
    fdbLog("Handling disambig response "+disambig);
  var i=0; while (i<disambig.length) {
    var tag=disambig[i++], value, term;
    var oid=tag['oid'], gloss=tag['gloss'], dterm=tag['gloss'];
    if (tag['replace']) {
      var existing=gnoFindTagSpan(tagtop,varname,tag['replace']);
      var checked=
	((existing) ? (existing.getAttribute('ischecked')) : (false));
      var newspan=gnoTagSpan(tagtop,varname,tag,checked);
      var unresolver=fdbSpan('unresolve','?');
      unresolver.title='select another meaning';
      unresolver.onclick=_gno_unresolve_onclick;
      fdbAppend(newspan," ",unresolver);
      if ((_debug) || (_debug_gnotags))
	fdbLog("Replacing"+((checked) ? " checked " : " ")+
	       "tag for "+tag+" with "+newspan+" from "+tag);
      if (existing) fdbReplace(existing,newspan);
      else fdbAppend(tagtop," ",newspan);}
    else {
      var existing=gnoFindTagSpan(tagtop,varname,tag['oid']);
      if (existing==null) {
	var newspan=gnoNewTagSpan(varname,tag,false);
	var termspan=gnoFindTagSpan(tagtop,varname,tag['term']);
	if (termspan)
	  fdbInsertAfter(termspan," ",newspan);
	else fdbAppend(tagtop," ",newspan);}}
  }
}

function _gno_unresolve_onclick(evt)
{
  _gno_unresolve(evt.target);
}

function _gno_unresolve(elt)
{
  var checkspan=fdbGetParentByClassName(elt,'checkspan');
  var tagtop=fdbGetParentByClassName(elt,'tags');
  var varname=tagtop.getAttribute('VARNAME');
  var tag=checkspan.getAttribute('term');
  var newspan=gnoNewTagSpan(varname,tag,fdbHasAttrib(checkspan,'ischecked'));
  var selector=fdbNewElement('select','meanings');
  var topopt=fdbNewElement('option',null);
  selector.name=varname;
  fdbAppend(topopt,'select a meaning');
  fdbAppend(selector,topopt);
  fdbAppend(newspan," ",selector);
  fdbReplace(checkspan,newspan);
  gno_getaltmeanings(tag,selector);
}

function gno_getaltmeanings(term,selector)
{
  if ((_debug) || (_debug_gnotags) || (_debug_disambig))
    fdbLog('Getting alternate meanings for '+term);
  var req=new XMLHttpRequest();
  req.onreadystatechange=function() {
    if ((req.readyState == 4) && (req.status == 200)) {
      var response=req.responseText.parseJSON();
      var i=response.length-1; while (i>=0) {
	var meaning=response[i--];
	var gloss=meaning['gloss'];
	var dterm=meaning['dterm'];
	var option=fdbNewElement('OPTION');
	option.value=meaning.oid;
	option.title=gloss;
	fdbAppend(option,dterm);
	fdbAppend(selector,"\n",option);}}}
  var uri=lookupterms_uri+'?TERMS='+encodeURIComponent(term)+'&ALL=yes';
  if ((_debug) || (_debug_gnotags) || (_debug_disambig))
    fdbLog('making ajax request: '+uri);
  req.open("GET",uri,true);
  req.send(null);
}
