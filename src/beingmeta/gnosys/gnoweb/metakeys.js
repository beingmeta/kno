/*
A metakey is a span with TEXT (and optionally OID) attributes and a
title of its gloss.
Alt-Clicking pops up a menu of meanings, indicated by dterms with gloss titles.
Selecting a meaning:
  changes the OID and gloss and the value in the associated checkbox

Ctrl-click toggles the checkbox for the context and value
*/

/* Metakey elements */

var mk_active_elt=false;

function mk_new(text,varname,selected)
{
  var mk_elt=fdbSpan('metakey');
  var text_node=document.createTextNode(text);
  if (varname) {
    var checkbox=fdbInput('checkbox',varname,text);
    if (selected==true) checkbox.checked=selected;
    else checkbox.checked=false;
    fdbAppend(mk_elt,checkbox);
    mk_elt.checkbox=checkbox;}
  else mk_elt.checkbox=false;
  fdbAppend(mk_elt,text_node);
  mk_elt.text_node=text_node;
  mk_elt.setAttribute('text',text);
  return mk_elt;
}

function mk_set_meaning(mk_elt,meaning,show_dterm)
{
  var oid=meaning.getAttribute('oid');
  fdbCopyAttribs(mk_elt,meaning,'oid','gloss','dterm');
  mk_elt.setAttribute('name',fdbOid2Id(oid,"OID"));
  if (meaning.getAttribute('gloss'))
    mk_elt.setAttribute
      ('title',meaning.getAttribute('dterm')+' - '+
       meaning.getAttribute('gloss'));
  if (mk_elt.checkbox)
    mk_elt.checkbox.value=mk_pair(mk_elt.getAttribute('text'),oid);
  else {
    var children=mk_elt.getElementsByTagName('INPUT');
    if (children) {
      var i=0; while (i<children.length)
	if ((children[i].nodeType==1) &&
	    (children[i].type=='checkbox')) {
	  mk_elt.checkbox=children[i];
	  mk_elt.checkbox.value=mk_pair(mk_elt.getAttribute('text'),oid);
	  break;}
	else i++;}}
  if (mk_elt.dterm) {
    var dterm=fdbSpan('dterm',meaning.getAttribute('dterm'));
    fdbReplace(mk_elt.dterm,dterm);
    mk_elt.dterm=dterm;}
  else if (show_dterm) {
    var dterm=fdbSpan('dterm',meaning.getAttribute('dterm'));
    mk_elt.dterm=dterm;
    mk_elt.appendChild(dterm);}
}

function mk_add(text,varname,selected,meaning)
{
  var mk_elt=mk_new(text,varname,selected);
  // Set the meaning if you have one
  if (meaning) mk_set_meaning(mk_elt,meaning);
  // Get possible meanings, ideally disambiguating
  mk_getmeanings(text,function(doc){
    var selected=false, i=0; var meanings=doc.childNodes;
    if (!(mk_elt.getAttribute('oid')))
      while (i<meanings.length) {
	var meaning=meanings[i++];
	if (meaning.getAttribute('selected')) {
	  selected=true;
	  mk_set_meaning(mk_elt,meaning);}}
    //if (selected) mk_short_highlight(mk_elt);
  },varname);
  return mk_elt;
}

function mk_pair(text,oid)
{
  if (oid)
    return ":(\""+text+"\" . "+oid.slice(1)+")";
  else return text;
}

/* Metakey event handlers */


function mk_disambig_click(evt)
{
  window.status='disambig click handler: '+fdbEventString(evt);
  var metakey=fdbFindClass(evt.target,'metakey');
  window.status='disambig click action'+fdbEventString(evt);
  mk_hide_expansion(metakey);
  mk_open_menu(metakey);
}

function mk_simple_keypress(evt)
{
  var ch=evt.charCode, kc=evt.keyCode;
  if ((ch==0x3b) || (kc==13)) {
    var elt=evt.target;
    var varname=fdbFindAttrib(elt,'var');
    var container=elt.getAttribute('target');
    if (!(container))
      container=mk_find_target(varname);
    else if (typeof container == "string")
      container=document.getElementById(container);
    if (elt.value.indexOf(';')<0) {
      var mk_elt=mk_add(elt.value,varname,true);
      container.appendChild(mk_elt);}
    else mk_disambig(elt.value,varname,container);
    elt.value="";}
}

function mk_form_keypress(evt)
{
  var elt=evt.target;
  var kc=evt.keyCode;
  var ch=evt.charCode;
  if ((ch==0x3b) || (kc==13))
    if (elt.value=='') {
      var submit_command=elt.getAttribute('COMMAND');
      if (submit_command)
	_fdb_clickit_command(submit_command);
      return false;}
    else {
      var varname=fdbFindAttrib(elt,'var');
      var container=elt.getAttribute('target');
      if (typeof container == "string")
	container=document.getElementById(container);
      var string=elt.value;
      evt.preventDefault(); evt.cancelBubble=true;
      if (elt.value.indexOf(';')<0) {
	var mk_elt=mk_add(elt.value,varname,true);
	container.appendChild(mk_elt);}
      else mk_disambig(elt.value,varname,container);
      elt.value=''; 
      return false;}
  else return true;
}

function mk_hint_click(evt)
{
  var mk_elt=fdbFindClass(evt.target,'metakey');
  if (!(mk_elt)) return;
  var varname=fdbFindAttrib(evt.target,'var');
  var container=fdbFindAttrib(evt.target,'target');
  if (typeof container == "string")
    container=document.getElementById(container);
  var new_elt=mk_add(mk_elt.getAttribute('text'),varname,true,mk_elt);
  container.appendChild(new_elt);
  mk_elt.style.fontStyle='italic';
}

function mk_check_click(evt)
{
  if (evt.ctrlKey) {}
  else if (evt.shiftKey) {}
  else if (evt.ctrlKey) {}
  else {
    var mk_elt=fdbFindClass(evt.target,'metakey');
    if (!(mk_elt)) return;
    if (mk_elt.checkbox)
      if (mk_elt.checkbox.checked)
	mk_elt.checkbox.checked=false;
      else mk_elt.checkbox.checked=true;}
}

function mk_default_click(evt)
{
  var mk_elt=fdbFindClass(evt.target,'metakey');
  if (!(mk_elt)) return;
  else if (mk_elt.checkbox) 
    mk_open_menu(mk_elt);
  else  {
    var varname=fdbFindAttrib(evt.target,'var');
    var container=fdbFindAttrib(evt.target,'target');
    if (typeof container == "string")
      container=document.getElementById(container);
    var new_elt=mk_add(mk_elt.getAttribute('text'),varname,true,mk_elt);
    container.appendChild(new_elt);
    mk_elt.style.fontStyle='italic';}
}

function mk_mouseout(evt)
{
  var elt=fdbFindClass(evt.target,'metakey');
  if (elt) mk_active_elt=false;
  if ((mk_active_menu) && (mk_active_menu.for_elt==elt))
    active_menu_timer=setTimeout("mk_hide_menu()",250);
  if ((mk_active_expansion) && (mk_active_expansion.for_elt==elt))
    active_expansion_timer=setTimeout("mk_hide_expansion()",250);
}

function mk_mouseover(evt)
{
  var elt=fdbFindClass(evt.target,'metakey');
  if (!elt) return;
  mk_active_elt=elt;
  if ((mk_active_menu) && (mk_active_menu.for_elt==elt))
    if (active_menu_timer) clearTimeout(active_menu_timer);
  if ((mk_active_expansion) &&
      (mk_active_expansion.for_elt==elt) &&
      (evt.ctrlKey)) {
    if (active_expansion_timer) clearTimeout(active_expansion_timer);}
  else if (evt.ctrlKey) {
    var metakey=fdbFindClass(evt.target,'metakey');
    mk_open_expansion(metakey);}
}

function mk_keydown(evt)
{
  if (evt.keyCode==17) {
    if (mk_active_elt) mk_open_expansion(mk_active_elt);}
}

function mk_keyup(evt)
{
  if (evt.keyCode==17) mk_hide_expansion();
}

/* Briefly highlighting elements (when they change) */

var mk_highlighted_elt=false;

function mk_short_highlight(mk_elt)
{
  if (mk_highlighted_elt) mk_unhighlight();
  mk_elt.style.textDecoration='underline';
  mk_highlighted_elt=mk_elt;
  setTimeout("mk_unhighlight()",2000);
}

function mk_unhighlight()
{
  if (mk_highlighted_elt) {
    var elt=mk_highlighted_elt;
    mk_highlighted_elt=false;
    elt.style.textDecoration=null;}
}

/* Getting context */

function mk_getcontext(varname)
{
  var qstring='';
  if (varname) {
    var input_elts=document.getElementsByTagName('INPUT');
    var i=0; while (i<input_elts.length) {
      var node=input_elts[i++];
      if (node.name==varname) {
	if ((node.type=='hidden') ||
	    (((node.type=='checkbox') || (node.type=='radio')) &&
	     (node.checked)))
	  qstring=qstring+'&'+'CONTEXT='+encodeURIComponent(node.value);}
      else if (node.hasAttribute('CONTEXT'))
	qstring=qstring+'&'+'CONTEXT='+encodeURIComponent(node.value);}}
  return qstring;
}

/* Getting meanings */

var getmeanings_script="getmeanings.fdcgi";

var mk_meanings=[], mk_meaning_callbacks=[];

function mk_getmeanings(keyterm,callback,varname)
{
  if (mk_meanings[keyterm])
    callback(mk_meanings[keyterm]);
  else if (mk_meaning_callbacks[keyterm]) 
    mk_meaning_callbacks[keyterm].push(callback);
  else {
    var req=new XMLHttpRequest();
    req.onreadystatechange=function() {
      if ((req.readyState == 4) && (req.status == 200)) {
	var base=false;
	var doc=req.responseXML.documentElement;
	_mk_gotmeanings(keyterm,doc);}}
    mk_meaning_callbacks[keyterm]=[];
    if (callback) mk_meaning_callbacks[keyterm].push(callback);
    var context=mk_getcontext(varname);
    req.open("GET",getmeanings_script+"?"+
	     "KEYWORD="+encodeURIComponent(keyterm)+
	     context,true);
    req.send(null);}
}

function _mk_gotmeanings(keyterm,doc)
{
  var keyword=fdbGet(doc,'keyword');
  mk_meanings[keyterm]=doc;
  mk_meanings[keyword]=doc;
  var callbacks=mk_meaning_callbacks[keyterm];
  mk_meaning_callbacks[keyterm]=false;
  var i=0; while (i<callbacks.length)
    callbacks[i++](doc);
}

/* Getting expansions */

var getexpansions_script='getexpansions.fdcgi';

var mk_expansions=[], mk_expansion_callbacks=[];

function mk_getexpansions(oid,callback)
{
  if (mk_expansions[oid])
    callback(mk_expansions[oid]);
  else if (mk_expansion_callbacks[oid]) 
    mk_expansion_callbacks[oid].push(callback);
  else {
    var req=new XMLHttpRequest();
    window.status='getting expansions for '+oid;
    req.onreadystatechange=function() {
      if ((req.readyState == 4) && (req.status == 200)) {
	var base=false;
	var doc=req.responseXML.documentElement;
	_mk_gotexpansions(oid,doc);}}
    mk_expansion_callbacks[oid]=[];
    if (callback) mk_expansion_callbacks[oid].push(callback);
    req.open("GET",getexpansions_script+"?"+
	     "CONCEPTS="+encodeURIComponent(oid),true);
    req.send(null);}
}

function _mk_gotexpansions(oid,doc)
{
  var nodes=doc.childNodes, i=0, len=nodes.length;
  window.status='got expansions from '+oid;
  while (i<len) {
    var node=nodes[i++];
    var exoid=node.getAttribute('oid');
    mk_expansions[exoid]=node;
    var callbacks=mk_expansion_callbacks[exoid];
    window.status='callback for '+exoid;
    mk_expansion_callbacks[exoid]=false;
    var j=0; while (j<callbacks.length)
      callbacks[j++](node);}
}

/* Doing disambiguation */

var disambig_script='disambig.fdcgi';

function mk_disambig(string,varname,container)
{
  var req=new XMLHttpRequest();
  var qstring='KEYWORDS='+encodeURIComponent(string)+'&'+
    mk_getcontext(varname);
  req.onreadystatechange=function() {
    if ((req.readyState == 4) && (req.status == 200)) {
      var base=false;
      var doc=req.responseXML.documentElement;
      _mk_handle_disambig(doc,varname,container);}}
  req.open("GET",disambig_script+"?"+qstring+mk_getcontext(varname),
	   true);
  req.send(null);
}

function _mk_handle_disambig(data,varname,container)
{
  var nodes=data.childNodes;
  _mk_clear_new(varname);
  var i=0; while (i<nodes.length) {
    var meaning=nodes[i++];
    if (meaning.nodeType==1) {
      var base=meaning.getAttribute('base');
      var mk_elt=mk_add(base,varname,true,meaning);
      container.appendChild(mk_elt);}}
}

/* Opening menus */

var mk_active_menu=false;

function mk_create_menu(mk_elt,doc)
{
  if (mk_elt.menu) return mk_elt.menu;
  else {
    var text=mk_elt.getAttribute('text');
    var oid=mk_elt.getAttribute('oid');
    var menu=mk_elt.menu=document.createElement('div'); 
    var meanings=doc.childNodes;
    menu.for_elt=mk_elt;
    menu.className='metakey_menu';
    menu.onmouseout=mk_menu_mouseout;
    menu.onmouseover=mk_menu_mouseover;
    var select_box=document.createElement('select'); 
    select_box.onclick=mk_menu_click;
    select_box.size=9;
    fdbAppend(menu,select_box);
    document.body.appendChild(menu);
    var i=0; while (i<meanings.length) {
      var meaning=meanings[i++];
      var option=mk_make_option(text,meaning);
      if (oid==meaning.getAttribute('oid'))
	option.selected=true;
      else option.selected=false;
      fdbAppend(select_box,option);}
    return menu;}
}

function mk_make_option(keyterm,meaning)
{
  var option=document.createElement('OPTION');
  fdbSetAttrib(option,'gloss',fdbGetAttrib(meaning,'gloss'));
  fdbSetAttrib(option,'title',
	       'click to select: '+fdbGetAttrib(meaning,'gloss'));
  fdbSetAttrib(option,'oid',fdbGetAttrib(meaning,'oid'));
  fdbSetAttrib(option,'dterm',fdbGetAttrib(meaning,'dterm'));
  fdbSetAttrib(option,'synonyms',fdbGetAttrib(meaning,'dterm'));
  fdbAppend(option,fdbGetAttrib(meaning,'dterm'));
  option.value=fdbGetAttrib(meaning,'oid');
  return option;
}

function mk_menu_click(evt)
{
  var scan=evt.target, option, menu;
  while (scan) {
    if (scan.className=='metakey_menu')
      menu=scan;
    else if (scan.tagName=='OPTION') 
      option=scan;
    scan=scan.parentNode;}
  if ((menu) && (option)) 
    mk_set_meaning(menu.for_elt,option,true);
  mk_hide_menu();
  evt.stopPropagation();
  return false;
}

/* Handling menu dynamics */

var active_menu_timer=false;

function mk_menu_mouseout(evt)
{
  active_menu_timer=setTimeout("mk_hide_menu()",250);
}

function mk_menu_mouseover(evt)
{
  clearTimeout(active_menu_timer);
}

function mk_open_menu(mk_elt)
{
  if (mk_elt.menu)
    mk_display_menu(mk_elt.menu,mk_elt);
  else mk_getmeanings
	 (mk_elt.getAttribute('text'),
	  function(doc) {
	   mk_create_menu(mk_elt,doc);
	   mk_open_menu(mk_elt);});
}

function mk_display_menu(menu,oidelt)
{
  var node=oidelt;
  // Now move the menu to just beneath the element
  var xoff=0, yoff=0;
  if (mk_active_menu)
    mk_active_menu.style.display='none';
  mk_active_menu=menu;
  while (node) {
    xoff=xoff+node.offsetLeft;
    yoff=yoff+node.offsetTop;
    node=node.offsetParent;}
  yoff=yoff+oidelt.offsetHeight;
  menu.style.display='block';
  // Move the menu over to be on the screen (if possible)
  if ((yoff+menu.offsetHeight)>window.innerHeight)
    yoff=yoff-((yoff+menu.offsetHeight)-window.innerHeight);
  if ((xoff+menu.offsetWidth)>window.innerWidth)
    xoff=xoff-((xoff+menu.offsetWidth)-window.innerWidth);
  if (xoff<0) xoff=1;
  if (yoff<0) yoff=1;
  menu.style.left=xoff+'px';
  menu.style.top=yoff+'px';
}


function mk_hide_menu()
{
  if (mk_active_menu) {
    var menu=mk_active_menu;
    mk_active_menu=false;
    menu.style.display='none';}
}

/* Expansion overlays */

var mk_active_expansion=false;

function mk_create_expansion(mk_elt,doc)
{
  if (mk_elt.expansion) return mk_elt.expansion;
  else {
    window.status='creating expansion';
    var text=mk_elt.getAttribute('text');
    var oid=mk_elt.getAttribute('oid');
    var expansion=mk_elt.expansion=fdbDiv('metakey_expansion');
    var nodes=doc.childNodes;
    var seen=[]; var i=0, lim=nodes.length, n_words=0;
    expansion.for_elt=mk_elt;
    expansion.onmouseout=mk_expansion_mouseout;
    expansion.onmouseover=mk_expansion_mouseover;
    var glosselt=fdbDiv('gloss');
    var expandelt=fdbDiv('synonyms');
    fdbAppend(expansion,glosselt);
    fdbAppend(expansion,expandelt);
    fdbAppend(glosselt,fdbText(doc.getAttribute('gloss')));
    fdbAppend(expandelt,fdbSpan('head',' expands to '));
    while (i<lim) {
      var meaning_elt=nodes[i++];
      var metakeyterms=document.createElement('span');
      var cat=meaning_elt.getAttribute('category');
      var gloss=meaning_elt.getAttribute('gloss');
      metakeyterms.className='metakeyterms';
      if (cat) 
	metakeyterms.title=cat+': '+gloss;
      else metakeyterms.title=gloss;
      metakeyterms.setAttribute('oid',meaning_elt.getAttribute('oid'));
      var terms=meaning_elt.childNodes; var j=0, jlim=terms.length;
      if (n_words>0) {
	fdbAppend(expandelt,fdbText(' < '));}
      n_words=0;
      while (j<jlim) {
	var term_node=terms[j++]; var term=term_node.getAttribute('term');
	if (j>1) {
	  var k=0; while (k<seen.length)
	    if (seen[k]==term) break; else k++;
	  if (k<seen.length) continue;}
	seen[seen.length]=term;
	var text_span=fdbSpan('term');
	if (term.indexOf(' ')<0)
	  fdbAppend(text_span,fdbText(term));
	else fdbAppend(text_span,fdbText('"'+term+'"'));
	if (term_node.hasAttribute('langid')) {
	  var langid=term_node.getAttribute('langid');
	  text_span.setAttribute('langid',langid);
	  fdbAppend(text_span,fdbSpan('langid',langid));}
	if (n_words>0)
	  fdbAppend(metakeyterms,fdbText(' . '));
	n_words++;
	fdbAppend(metakeyterms,text_span);}
      fdbAppend(expandelt,metakeyterms);}
    document.body.appendChild(expansion);
    return expansion;}
}

/* Handling expansion dynamics */

var active_expansion_timer=false;

function mk_expansion_mouseout(evt)
{
  active_expansion_timer=setTimeout("mk_hide_expansion()",250);
}

function mk_expansion_mouseover(evt)
{
  clearTimeout(active_expansion_timer);
}

function mk_open_expansion(mk_elt)
{
  window.status='opening expansion';
  if (mk_elt.expansion)
    mk_display_expansion(mk_elt.expansion,mk_elt);
  else mk_getexpansions
	 (mk_elt.getAttribute('oid'),
	  function(doc) {
	   window.status='open_expansion callback';
	   mk_create_expansion(mk_elt,doc);
	   mk_open_expansion(mk_elt);});
}

function mk_display_expansion(expansion,oidelt)
{
  var node=oidelt;
  // Now move the expansion to just beneath the element
  var xoff=0, yoff=0;
  if ((mk_active_expansion) &&
      (mk_active_expansion!=expansion)) {
    clearTimeout(active_expansion_timer);
    mk_active_expansion.style.display='none';}
  mk_active_expansion=expansion;
  window.status='displaying expansion '+expansion+
    ' for '+oidelt.getAttribute('oid');
  while (node) {
    xoff=xoff+node.offsetLeft;
    yoff=yoff+node.offsetTop;
    node=node.offsetParent;}
  yoff=yoff+oidelt.offsetHeight;
  expansion.style.display='block';
  // Move the expansion over to be on the screen (if possible)
  if ((yoff+expansion.offsetHeight)>window.innerHeight)
    yoff=yoff-((yoff+expansion.offsetHeight)-window.innerHeight);
  if ((xoff+expansion.offsetWidth)>window.innerWidth)
    xoff=xoff-((xoff+expansion.offsetWidth)-window.innerWidth);
  if (xoff<0) xoff=1;
  if (yoff<0) yoff=1;
  expansion.style.left=xoff+'px';
  expansion.style.top=yoff+'px';
  window.status='putting expansion at '+xoff+','+yoff+' and '+
    expansion.offsetWidth+'x'+expansion.offsetHeight+' '+
    expansion.style.display;
}


function mk_hide_expansion()
{
  // window.status="mk_hide_expansion("+mk_active_expansion+")";
  if (mk_active_expansion) {
    var expansion=mk_active_expansion;
    mk_active_expansion=false;
    expansion.style.display='none';}
}

function mk_find_target(varname)
{
  if (typeof varname == "string") {
    var probe=fdbByID('NEW_'+varname);
    if (probe) return probe;
    else return fdbById(varname);}
  else return varname;
}

