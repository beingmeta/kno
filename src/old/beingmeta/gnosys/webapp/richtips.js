/* -*- Mode: C; -*- */

/* Richtips are overlayed tooltips for metakeys, displayed
   in translucent rectangles.
   This file works in conjunction with richtips.css
   $Id$
*/

/* Set to false to suppress richtip display. */
var display_richtips=true;
/* This is the current 'live' richtip displayed. */
var live_richtip=false;

/* How long to wait before actually getting the richtip element */
var richtip_delay=300;
var richtip_target=false;
var richtip_timeout=null;

/* This maps OIDs to richtips */
var richtips={};

/* The URI for the getinfo script */
var getinfo_uri="getinfo.fdcgi";

function get_richtip(elt)
{
  var oid=elt.getAttribute('oid');
  if (!(oid)) return null;
  var cached=richtips[oid];
  if (cached) {
    if (!(cached.getAttribute('gotten')))
      getRichtipInfo(cached);
    return cached;}
  var richtip=fdbDiv('richtip');
  richtips[oid]=richtip;
  richtip.setAttribute('oid',oid);
  var dterm=elt.getAttribute('dterm');
  var text=elt.getAttribute('text');
  var gloss=elt.getAttribute('gloss');
  var aka=elt.getAttribute('aka');
  if (dterm) {
    var span=fdbSpan('dterm');
    var paren=dterm.indexOf('('), close=dterm.indexOf(')');
    if ((paren>0) && (close>0) && (paren<close)) {
      fdbAppend(span,dterm.slice(0,paren));
      fdbAppend(span,fdbSpan("disambiguator",dterm.slice(paren,close)));
      fdbAppend(span,dterm.slice(close));}
    else {
      var colon=dterm.indexOf(':');
      if (colon>0) {
	fdbAppend(span,dterm.slice(0,colon));
	fdbAppend(span,fdbSpan("disambiguator",dterm.slice(colon)));}
      else fdbAppend(span,dterm);}
    fdbAppend(richtip,span);}
  else fdbAppend(richtip,fdbSpan('term',text));
  fdbAppend(richtip,' ');
  fdbAppend(richtip,fdbSpan('gloss',gloss));
  fdbAppend(document.body,richtip);
  getRichtipInfo(richtip);
  return richtip;
}

function richtip_mouseover(evt)
{
  var elt=evt.target;
  if (!(display_richtips)) return;
  while (elt)
    if ((elt.hasAttribute) &&
	(elt.hasAttribute('oid')))
      break;
    else elt=elt.parentNode;
  if (!(elt)) return;
  if (elt.hasAttribute('norichtips')) return;
  var richtip=get_richtip(elt);
  if (!(richtip)) return;
  if (live_richtip==richtip) {
    richtip.style.display='block';
    return;}
  else if (live_richtip)
    live_richtip.style.display='none';
  live_richtip=richtip;
  var xoff=0, yoff=0; var node=elt;
  while (node) {
    xoff=xoff+node.offsetLeft;
    yoff=yoff+node.offsetTop;
    node=node.offsetParent;}
  yoff=yoff+elt.offsetHeight;
  richtip.style.left=xoff+'px';
  richtip.style.top=yoff+'px';
  richtip.style.display='block';
  // window.status='display_expansion='+oid+'; height='+expansions_elt.offsetHeight;
  if ((xoff+richtip.offsetWidth)>window.innerWidth)
    xoff=xoff-((xoff+richtip.offsetWidth)-window.innerWidth);
  if (xoff<0) xoff=1;
  richtip.style.left=xoff+'px';
  richtip.style.top=yoff+'px';
}

function richtip_mouseout(evt)
{
  // fdbMessage("richtip_mouseout");
  if (!(display_richtips)) return;
  if (live_richtip) 
    live_richtip.style.display='none';
  if (richtip_timeout) {
    clearTimeout(richtip_timeout);
    richtip_target=null; ricthip_timeout=null;}
}

/* AJAX calls to get more richtip info */

function getRichtipInfo(richtip)
{
  if (richtip_target==richtip) return;
  else if (richtip_target) {
    if (richtip_timeout) clearTimeout(richtip_timeout);}
  richtip_target=richtip;
  richtip_timeout=setTimeout(getRichtipInfo_delayed,richtip_delay,richtip);
}

function handleRichtipInfo(doc)
{
  var elements=doc.childNodes;
  var i=0; while (i<elements.length) {
    var elt=elements[i++];
    if (elt.nodeType!=1) continue;
    var richtip=richtips[elt.getAttribute('oid')];
    var rels=elt.childNodes;
    richtip.setAttribute('gotten','yes');
    var j=0; while (j<rels.length) {
      var rel=rels[j++];
      if (rel.nodeType!=1) continue;
      var reldiv=fdbDiv('rel');
      fdbAppend(richtip,reldiv);
      fdbAppend(reldiv,fdbSpan('relname',rel.getAttribute('relname')));
      fdbAppend(reldiv," ");
      var vals=rel.childNodes;
      var k=0; while (k<vals.length) {
	var val=vals[k++];
	if (val.nodeType!=1) continue;
	if (k>1) fdbAppend(reldiv," . ");
	var term=val.getAttribute('dterm');
	if (!(term)) term=val.getAttribute('norm');
	fdbAppend(reldiv,fdbSpan('dterm',term));}}
  }
}

var default_language=null;

function getRichtipInfo_delayed(richtip)
{
  if (richtip!=richtip_target) return;
  var req=new XMLHttpRequest();
  var language=document.body.getAttribute('xml:lang');
  req.onreadystatechange=function() {
    if ((req.readyState == 4) && (req.status == 200)) {
      var response=req.responseXML;
      handleRichtipInfo(response.documentElement);}}
  if (language==null) language=default_language;
  if (language==null)
    req.open("GET",getinfo_uri+"?CONCEPT="+
	     encodeURIComponent(richtip.getAttribute('oid')),true);
  else req.open("GET",getinfo_uri+"?CONCEPT="+
		encodeURIComponent(richtip.getAttribute('oid'))+
		"&LANGUAGE="+language,true);
  req.send(null);
}




