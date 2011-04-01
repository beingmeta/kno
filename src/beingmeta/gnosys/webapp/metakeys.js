/* -*- Mode: C; -*- */

// This is true if mouseovers always show expansions
var live_metakeys=false;
// This is the currently live metakey
var live_metakey=false;
// This is true if there is an expansion displayed
var expansion_visible=false;
var decorate_metakeys=false;

var page_submit_fn=false;

function mk_getvarname(elt)
{
  while (elt) {
    var probe=((elt.getAttribute) && (elt.getAttribute('VAR')));
    if (probe) return probe;
    else elt=elt.parentNode;}
  return null;
}

function mk_ambiguity_elt(varname)
{
  if (varname) {
    var elt=fdbByID(varname+'_AMBIG');
    if (elt) return elt;
    else return fdbByID('AMBIG');}
  else return fdbByID('AMBIG');
}

function mk_ambiguity_elt()
{
  return fdbByID('AMBIG');
}

function mk_expansions_elt()
{
  return fdbByID('EXPANSIONS');
}

function mk_tryexpansion(oid)
{
  return fdbByID(fdbOid2Id(oid,'EX'));
}

function mk_narrative(text,container)
{
  var narrative=document.createElement('span');
  narrative.className='narrative';
  fdbAppend(narrative,fdbText(text));
  if (arguments.length==2)
    fdbAppend(container,narrative);
  return narrative;
}

var expanded_elt=false;

function _mk_set_expanded_elt(elt)
{
  if (expanded_elt==elt) {
    elt.style.display='block';
    return;}
  else if (expanded_elt) expanded_elt.style.display='none';
  if (elt) {
    expanded_elt=elt; elt.style.display='block';}
}

var pending_ambig=false;
var pending_ambig_var=false;

function handle_pending_ambig()
{
  if (pending_ambig) {
    mk_getambig(pending_ambig,pending_ambig_var);
    pending_ambig=false;
    pending_ambig_var=false;}
}

/* Getting context for AJAX calls */

function mk_getvalues(varname)
{
  var result=[];
  var input_elts=document.getElementsByTagName('input');
  var i=0; while (i<input_elts.length) {
    var node=input_elts[i++];
    if (node.name==varname) {
      if ((node.type=='checkbox') || (node.type=='radio'))
	if (node.checked) result[result.length]=node.value;
      if (node.type=='hidden') result[result.length]=node.value;}}
  return result;
}

function mk_getcontext(varname)
{
  var qstring='';
  if (varname) {
    var keywords_elt=fdbByID(varname+'_KEYWORDS');
    if ((keywords_elt) && (keywords_elt.value) &&
	(keywords_elt.value.length>0))
      qstring=qstring+'KEYWORDS='+encodeURIComponent(keywords_elt.value)+'&';
    var metakeys=mk_getvalues(varname);
    var i=0; while (i<metakeys.length) {
      var metakey=metakeys[i++];
      if ((metakey.nodeType==1) && (metakey.className=='metakey'))
	qstring=qstring+'CONCEPTS='+
	  encodeURIComponent(metakey.getAttribute('oid'))+'&';}}
  var input_elts=document.getElementsByTagName('input');
  var i=0; while (i<input_elts.length)
    if (input_elts[i].hasAttribute('CONTEXT'))
      qstring=qstring+'CONTEXT='+encodeURIComponent(input_elts[i++].value)+'&';
    else i++;
  return qstring;
}

/* Dragging metakeys */

var dragging_metakey=false, about_to_drag=false;
var drag_from=false, drag_to=false;
var drag_xoff=0, drag_yoff=0, drag_clickx=false, drag_clicky=false;
var drag_threshold=1500;

function dragging_mousemove(evt)
{
  if (dragging_metakey) {
    // window.status='dragging_metakey='+dragging_metakey+'@'+evt.pageX+','+evt.pageY;
    dragging_metakey.style.left=(evt.pageX+drag_xoff)+'px';
    dragging_metakey.style.top=(evt.pageY+drag_yoff)+'px';}
}

function startDrag(evt)
{
  if (about_to_drag) {
    var container=fdbFindClass(about_to_drag,"codefines");
    if (container) {
      mk_hide_expansion();
      drag_to=drag_from=container; drag_to=drag_from;
      drag_from.style.backgroundColor='lightgray';
      // about_to_drag.className='dragging_'+about_to_drag.className;
      drag_xoff=16; drag_yoff=-(drag_from.offsetHeight)/2;
      dragging_metakey=about_to_drag; about_to_drag=false;
      dragging_metakey.style.position='absolute';
      dragging_metakey.style.left=drag_clickx+drag_xoff;
      dragging_metakey.style.top=drag_clicky+drag_yoff;
      // window.status='dragging_metakey='+dragging_metakey+'@'+dragging_metakey.style.left+','+dragging_metakey.style.top;
      document.body.style.cursor='move';
      document.body.onmousemove=dragging_mousemove;
      // window.status='dragging_metakey='+dragging_metakey;
      mk_hide_expansion();}}
}

function stopDrag(evt)
{
  if (dragging_metakey) {
    dragging_metakey.style.position='inherit';
    document.body.style.cursor='inherit';
    document.body.style.onmouseover=null;
    if (drag_to) drag_to.style.backgroundColor='inherit';
    // dragging_metakey.className=dragging_metakey.className.slice(9);
    dragging_metakey=false;}
}

function metakey_drag_mousedown(evt)
{
  if (menuon) return;
  var oidelt=fdbGetOidElt(evt.target);
  if (oidelt) {
    about_to_drag=oidelt;
    drag_clickx=evt.pageX; drag_clicky=evt.pageY;
    setTimeout(startDrag,250);
    evt.preventDefault();
    return false;}
  else return true;
}
function metakey_drag_mouseup(evt)
{
  // window.status='mouseuop drag_start='+drag_start+'; diff='+(evt.timeStamp-drag_start);
  if ((!(dragging_metakey)) && (about_to_drag)) {
    about_to_drag=false; stopDrag();
    evt.preventDefault();
    return metakey_click(evt);}
  else evt.preventDefault();
  if (dragging_metakey) {
    var elt=evt.target;
    document.body.style.cursor='inherit';
    if ((drag_to) && (drag_to != drag_from)) {
      var to_new=mk_get_newspan(drag_to);
      var oid=dragging_metakey.getAttribute('oid');
      var to_id=drag_to.id;
      var tocheck=fdbOid2Elt(oid,'X'+to_id);
      if (tocheck) tocheck.checked=true;
      else {
	tocheck=document.createElement('input');
	tocheck.type='checkbox'; tocheck.className='hidden';
	tocheck.name=to_id; tocheck.value=oid;
	tocheck.id=fdbOid2Id(oid,'X'+to_id);
	tocheck.checked=true;
	fdbAppend(drag_to,tocheck);}
      if (drag_from) {
	var from_id=drag_from.id;
	var fromcheck=fdbOid2Elt(oid,'X'+from_id);
	if (fromcheck) fromcheck.checked=false;}
      dragging_metakey.parentNode.removeChild(dragging_metakey);
      fdbAppend(to_new,fdbText(' '));
      fdbAppend(to_new,dragging_metakey);
      dragging_metakey.id=fdbOid2Id(oid,to_id);
      mk_display_expansion_at(oid,dragging_metakey);
      stopDrag();
      return false;}
    else {
      stopDrag();
      mk_display_expansion_at(oid,dragging_metakey);
      return true;}}
  else return true;
}

function metakey_drag_mouseover(evt)
{
  if (dragging_metakey) {
    var elt=evt.target;
    var to_elt=fdbFindClass(elt,'codefines');
    if (to_elt) {
      if (drag_to) drag_to.style.backgroundColor='inherit';
      to_elt.style.backgroundColor='lightgray';
      drag_to=to_elt;}
    metakey_mouseover(evt);}
  else metakey_mouseover(evt);
}

function metakey_drag_mouseout(evt)
{
  if (dragging_metakey) {
    var elt=evt.target;
    var to_elt=fdbFindClass(elt,'codefines');
    if (to_elt) {
      to_elt.style.background='inherit';
      drag_to=false;}
    metakey_mouseout(evt);}
  else metakey_mouseout(evt);
}

/* Application utility functions */

function mk_display_expansion_at(oid,elt)
{
  if (dragging_metakey) return;
  // This undisplays all of the current expansions and then displays the 
  // requested expansion.
  var expansion_elt=mk_tryexpansion(oid);
  var expansions_elt=mk_expansions_elt();
  if ((expansion_elt) && (expansions_elt)) {
    var nodes=expansions_elt.childNodes;
    var i=0, lim=nodes.length;
    // Hide all the expansions
    while (i<lim)
      if ((nodes[i].nodeType==1) &&
	  (nodes[i].className=='expansion')) {
	nodes[i].style.display='none'; i++;}
      else i++;
    // Display the one we want
    _mk_set_expanded_elt(expansion_elt);
    // Now move the expansions elt to the elt
    var xoff=0, yoff=0; var node=elt;
    while (node) {
      xoff=xoff+node.offsetLeft;
      yoff=yoff+node.offsetTop;
      node=node.offsetParent;}
    yoff=yoff+elt.offsetHeight;
    expansions_elt.style.display='block';
    expansion_visible=true;
    // window.status='display_expansion='+oid+'; height='+expansions_elt.offsetHeight;
    if ((xoff+expansions_elt.offsetWidth)>window.innerWidth)
      xoff=xoff-((xoff+expansions_elt.offsetWidth)-window.innerWidth);
    if (xoff<0) xoff=1;
    expansions_elt.style.left=xoff+'px';
    expansions_elt.style.top=yoff+'px';
    return true;}
  else return false;
}

function mk_hide_expansion()
{
  var expansions_elt=fdbByID('EXPANSIONS');
  if (expansions_elt) {
    expansion_visible=false;
    expansions_elt.style.display='none';}
}

function mk_get_ambig_elt(base)
{
  // This undisplays all of the current meaning descriptions
  // and then displays the selected one.
  var meanings=mk_ambiguity_elt();
  if (meanings) {
    var nodes=meanings.childNodes;
    var i=0; while (i<nodes.length) {
      var node=nodes[i++];
      if ((node.nodeType==1) &&
	  (node.className=='ambiguity') &&
	  (node.getAttribute('term')==base))
	return node;}}
  return null;
}

function mk_remove_ambig_elt(base)
{
  var elt=mk_get_ambig_elt(base);
  if (elt)
    elt.parentNode.removeChild(elt);
}

/* Metakey functions */

function _mk_default_add_metakey(metakeys_elt,metakey_elt)
{
  var varname=mk_getvarname(metakeys_elt);
}

function mk_get_newspan(elt)
{
  if ((elt) && (elt.childNodes)) {
    var nodes=elt.childNodes;
    var i=0; while (i<nodes.length) {
      var node=nodes[i++];
      if ((node.nodeType==1) && (node.className=='new'))
	return node;}}
  return elt;
}

function mk_add_tag(varname,phrase)
{
  var metakeys=fdbByID(varname);
  var addto=mk_get_newspan(metakeys);
  if (!(addto)) addto=metakeys;
  var children=metakeys.childNodes;
  var i=0; if (children) while (i<children.length)
    if (children[i].nodeType==1)
      if (children[i].getAttribute('base')==phrase) 
	return children[i];
      else {
	var j=0; var grandchildren=children[i++].childNodes;
	if (grandchildren) while (j<grandchildren.length) 
	  if ((grandchildren[j].nodeType==1) &&
	      (grandchildren[j].getAttribute('base')==phrase))
	    return grandchildren[j];
	  else j++;}
    else i++;
  // window.status='need new phrase element for '+phrase+' with '+varname;
  var phrase_elt=document.createElement('span');
  phrase_elt.className='texttag';
  var checkbox=document.createElement('input');
  checkbox.type='checkbox'; checkbox.type='hidden';
  checkbox.name=varname; checkbox.value=':(\"'+phrase+'\")';
  checkbox.checked=true;
  phrase_elt.checkbox=checkbox;
  fdbAppend(phrase_elt,checkbox);
  phrase_elt.appendChild
    (fdbText
     ((phrase.indexOf(' ')<0) ? (phrase) : ('"'+phrase+'"')));
  phrase_elt.setAttribute('base',phrase);
  fdbAppend(addto,phrase_elt);
  fdbAppend(addto,fdbText(' '));
  return phrase_elt;
}

function mk_tag_checkbox(tagelt)
{
  if (tagelt.checkbox) return tagelt.checkbox;
  var children=tagelt.childNodes, i=0;
  while (i<children.length)
    if ((children[i].nodeType==1) &&
	(children[i].tagName=='INPUT') &&
	(children[i].type=='checkbox')) {
      tagelt.checkbox=children[i];
      return children[i];}
    else i++;
  return false;
}

function mk_tag_set_meaning(tagelt,meaning)
{
  var children=tagelt.childNodes, i=0;
  while (i<children.length)
    if ((children[i].nodeType==1) &&
	(children[i].tagName=='A') &&
	(children[i].className=='oid'))
      tagelt.removeChild(children[i++]);
    else i++;
  tagelt.className='tag';
  var checkbox=mk_tag_checkbox(tagelt);
  checkbox.value=meaning.getAttribute('tag');
  tagelt.setAttribute('tag',meaning.getAttribute('tag'));
  tagelt.setAttribute('oid',meaning.getAttribute('oid'));
  tagelt.setAttribute('gloss',meaning.getAttribute('gloss'));
  tagelt.title=meaning.getAttribute('gloss');
  if (decorate_metakeys)
    fdbAppend(tagelt,mk_tag_diamond(meaning));
}

function mk_tag_set_tag(tagelt,tag)
{
  tagelt.checkbox.value=tag;
  tagelt.setAttribute('tag',tag);
}

function mk_tag_add_meaning(tagelt,meaning)
{
  tagelt.className='tag';
  if (decorate_metakeys)
    fdbAppend(tagelt,mk_tag_diamond(meaning));
}

/* Metakey Menus */

var menuon=false, livemenu=false, menu_timeout=false;

function menuoff()
{
  if (livemenu) livemenu.style.display='none';
  livemenu=false; menuon=false; menu_timeout=false;
}

function delayed_menuoff()
{
  if (!(menuon)) {
    if (livemenu) livemenu.style.display='none';
    livemenu=false; menuon=false; menu_timeout=false;}
}

function menuout()
{
  menu_timeout=setTimeout(delayed_menuoff,500);
}

function keepMenu()
{
  menuon=true;
  if (menu_timeout) clearTimeout(menu_timeout);
  menu_timeout=false; 
}

function make_menu_item(menu,text)
{
  // var item=document.createElement('div');
  var item=document.createElement('OPTION');
  item.className='item';
  fdbAppend(menu,item);
  if (text) fdbAppend(item,fdbText(text));
  return item;
}

function mk_metakey_menu(mkelt,hint)
{
  var base=mkelt.getAttribute('base');
  if (mkelt.menu) return mkelt.menu;
  else {
    var menu=mkelt.menu=document.createElement('div'); 
    menu.forelt=mkelt;
    menu.className='metakey_menu_container';
    menu.onmouseover=metakey_menu_mouseover;
    menu.onmouseout=metakey_menu_mouseout;
    var select_box=document.createElement('select'); 
    select_box.className='metakey_menu';
    select_box.onclick=metakey_menu_click;
    select_box.size=9;
    fdbAppend(menu,select_box);
    fdbAppend(mkelt,menu);
    if (hint) {
      var add_item=make_menu_item(select_box,'Add this tag');
      add_item.title='Add this meaning of "'+base+'" as a tag';
      add_item.onclick=mk_hint_add;}
    else {
      var delete_item=make_menu_item(select_box,'Remove this entry');
      delete_item.title='Remove this meaning of "'+base+'"';
      delete_item.onclick=metakey_menu_toggle;}
    if (mkelt.getAttribute('oid')) {
      var edit_item=
	make_menu_item(select_box,'Edit this meaning of "'+base+'"');
      edit_item.title='Edit this meaning of "'+base+'"';
      edit_item.onclick=metakey_menu_edit;}
    var define_item=
      make_menu_item(select_box,'Define a new meaning for "'+base+'"');
    define_item.title='Click to define a new meaning';
    define_item.onclick=metakey_menu_define;
    var ambig_elt=mk_get_ambig_elt(base);
    // if ((ambig_elt) && (ambig_elt.childNodes))
    //   window.status=ambig_elt.childNodes.length+' meanings';
    // else window.status='no meanings';
    if (ambig_elt) {
      var nodes=ambig_elt.childNodes;
      var i=0; while (i<nodes.length) {
	var copy=nodes[i].cloneNode(true);
	// Copy the showmore_mouseover method
	if (nodes[i].onmouseover)
	  copy.onmouseover=(nodes[i].onmouseover);
	i++; fdbAppend(select_box,copy);}}
    return menu;}
}

function display_metakey_menu(menu,oidelt)
{
  var node=oidelt;
  mk_hide_expansion();
  // Now move the menu to just beneath the element
  var xoff=0, yoff=0;
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
  if ((livemenu) && (livemenu!=menu))
    livemenu.style.display='none';
  livemenu=menu; menuon=true;
}

/* Metakey menu methods */

function metakey_menu_mouseover(evt)
{
  var elt; keepMenu();
  if (elt=fdbFindClass(evt.target,'item')) {
    elt.style.color='blue';
    elt.style.textDecoration='underline';}
  else if (elt=fdbFindClass(evt.target,'meaning')) 
    elt.style.textDecoration='underline';
  // This avoids the generic metakey mouseovers
  evt.stopPropagation(); menuon=true;
}

function metakey_menu_mouseout(evt)
{
  var elt=evt.target;
  var rel=evt.relatedTarget;
  if ((elt) && (elt.className=='metakey_menu_container') &&
      (rel.parentNode != elt))
    elt.style.display='none';
  else if (elt=fdbFindClass(evt.target,'item')) {
    elt.style.color=null;
    elt.style.textDecoration=null;}
  else if (elt=fdbFindClass(evt.target,'meaning')) 
    elt.style.textDecoration=null;
  menuon=false; menuout();
}

function metakey_menu_click(evt)
{
  var elt=evt.target;
  var menu=evt.target;
  if (evt.target.tagName!='OPTION') return;
  while (menu)
    if (menu.className=='metakey_menu') break;
    else menu=menu.parentNode;
  if (elt) {
    var varname=mk_getvarname(elt);
    var base=elt.getAttribute('base');
    var hints_elt=fdbFindClassById(elt,'hints');
    var tag=mk_add_tag(varname,base);
    mk_tag_set_meaning(tag,elt);
    tag.style.fontStyle='italic';}
  menuoff();
  evt.stopPropagation();
}

function metakey_menu_toggle(evt)
{
  var item=evt.target;
  evt.stopPropagation();
  while (item)
    if (item.className=='item') break;
    else item=item.parentNode;
  if (!(item)) {
    alert('Application DOM error: metakey_menu_toggle (item)');
    return;}
  var menu=item;
  while (menu)
    if (menu.className=='metakey_menu') break;
    else menu=menu.parentNode;
  var varname=mk_getvarname(menu);
  var tagelt=menu.parentNode.forelt;
  var tagval=tagelt.getAttribute('TAG');
  menuoff();
  if (!(tagelt)) {
    alert('Application DOM error: metakey_menu_toggle (oidelt)');
    return;}
  var checkbox=mk_tag_checkbox(tagelt);
  if (checkbox)
    if (checkbox.checked) {
      checkbox.checked=false;
      tagelt.className='deleted_tag';
      var newnode=document.createElement('div');
      newnode.className='item';
      fdbAppend(newnode,fdbText('Restore this'));
      newnode.onclick=metakey_menu_toggle;
      menu.replaceChild(newnode,item);}
    else {
      checkbox.checked=true;
      tagelt.className='tag';
      var newnode=document.createElement('div');
      newnode.className='item';
      fdbAppend(newnode,fdbText('Delete this'));
      menu.replaceChild(newnode,item);}
  else {
    alert('Application DOM error: metakey_menu_toggle (no checkbox) for '+
	  varname+' and '+tagval+' at '+tagelt.className);
    return;}
}

function metakey_menu_edit(evt)
{
  var menu=evt.target;
  while (menu)
    if (menu.className=='metakey_menu') break;
    else menu=menu.parentNode;
  if (menu) menuoff();
  else {
    alert('Application DOM error: metakey_menu_edit');
    return;}
  evt.stopPropagation();
  var oid=fdbFindAttrib(menu,'oid');
  if (oid) 
    window.open('define.fdcgi?CONCEPT='+oid+'&'+mk_getcontext(false),
		'',
		'innerWidth=512px,innerHeight=575px,status=1,location=0,resizable=1,scrollbars,right=75,top=20',0);
  else alert('Application DOM error: metakey_menu_edit');
}

function metakey_menu_define(evt)
{
  var menu=evt.target;
  while (menu)
    if (menu.className=='metakey_menu') break;
    else menu=menu.parentNode;
  if (menu)
    menuoff();
  else {
    alert('Application DOM error: metakey_menu_define');
    return;}
  evt.stopPropagation();
  var base=fdbFindAttrib(menu,'base');
  if (base) {
    window.open('define.fdcgi?TERM='+base+'&'+mk_getcontext(false),
		'',
		'innerWidth=512px,innerHeight=575px,status=1,location=0,resizable=1,scrollbars,right=75,top=20',0);
    if ((menu) && (menu.forelt)) menu.forelt.menu=false;
    mk_remove_ambig_elt(base);}
  else alert('Application DOM error: metakey_menu_define');
}

/* Metakey methods */

function getbaselt(elt)
{
  while (elt)
    if ((elt.getAttribute) && (elt.getAttribute('base')))
      return elt;
    else elt=elt.parentNode;
  return false;
}

function metakey_click(evt)
{
  var elt=evt.target;
  var oidelt=getbaselt(elt);
  if (!(oidelt)) 
  var menu=oidelt.menu;
  mk_hide_expansion();
  if (!(menu)) menu=mk_metakey_menu(oidelt);
  display_metakey_menu(menu,oidelt);
}

function metakey_mouseover(evt)
{
  var evtelt=evt.target;
  var elt=evt.target;
  var oidelt=fdbGetOidElt(elt);
  if (oidelt)
    if ((live_metakeys) || (evt.shiftKey)) {
      var oid=oidelt.getAttribute('oid');
      var base=oidelt.getAttribute('base');
      oidelt.style.background='lightgray';
      // window.status=oid;
      if ((livemenu) && (livemenu==oidelt.menu)) keepMenu();
      if (!(menuon)) {
	mk_getambig(base,false);
	mk_getexpansion(oid,oidelt.getAttribute('gloss'));
	mk_display_expansion_at(oid,oidelt);}}
    else {
      var base=oidelt.getAttribute('base');
      var oid=oidelt.getAttribute('oid');
      // window.status=oid;
      mk_getambig(base,false);
      live_metakey=oidelt;}
}

function metakey_mouseout(evt)
{
  var oidelt=fdbGetOidElt(evt.target);
  if (oidelt) {
    var exelt=mk_tryexpansion(oidelt.getAttribute('oid'));
    var rel=evt.relatedTarget;
    if (exelt) exelt.style.display='none';
    oidelt.style.background='inherit';}
  if (livemenu) {menuon=false; menuout();}
  mk_hide_expansion();
  live_metakey=false;
}

function metakey_mousemove(evt)
{
  if (!(live_metakeys))
    if (evt.shiftKey)
      if (expansion_visible) {}
      else {
	if (live_metakey) {
	  var oidelt=live_metakey;
	  var base=oidelt.getAttribute('base');
	  var oid=oidelt.getAttribute('oid');
	  mk_getambig(base,false);
	  mk_getexpansion(oid,oidelt.getAttribute('gloss'));
	  mk_display_expansion_at(oid,oidelt);}}
    else if (expansion_visible)
      mk_hide_expansion();
    else {}
}

/* Hint methods */

/* Hints are metakeys or strings presented to the user. */

function mk_hint_add(elt)
{
  var oidelt=fdbGetOidElt(elt);
  mk_hint_click(oidelt);
  return false;
}

function mk_hint_mouseover(evt)
{
  var elt=evt.target, oid=false; 
  var varname=mk_getvarname(elt);
  // window.status='mk_hint_mouseover '+elt;
  while (elt)
    if ((elt.getAttribute) && (elt.getAttribute('tag'))) break;
    else elt=elt.parentNode;
  if (elt) elt.style.textDecoration='underline';
}

function showmore_mouseover(evt)
{
  var elt=evt.target;
  // window.status='showmore_mouseover';
  while (elt)
    if (elt.tagName=='SELECT') break;
    else elt=elt.parentNode;
  if (elt) {
    // window.status='expanding '+elt;
    elt.setAttribute('expanded','yes');}
}

function mk_hint_mouseout(evt)
{
  var elt=evt.target, oid=false; 
  var varname=mk_getvarname(elt);
  // window.status='mk_hint_mouseover '+elt;
  while (elt)
    if ((elt.getAttribute) && (elt.getAttribute('tag'))) break;
    else elt=elt.parentNode;
  if (elt) elt.style.textDecoration='inherit';
}

/* The text input for a metakey widget */

function mkw_keypress(evt)
{
  var elt=evt.target;
  var varname=mk_getvarname(elt);
  mkw_textchange(elt);
  var ch=evt.charCode, kc=evt.keyCode;
  if ((ch==0x3b) || (kc==13)) {
    var end=elt.getAttribute('cursor');
    var string=elt.value;
    if (string.indexOf(';')<0) {
      var stringval=trimString(string);
      mk_getambig(stringval,varname,true,true);}
    else {
      var end=string.length; var start=end-1;
      while ((start>0) && (string.charAt(start)!=';')) start--;
      if (start>0) start++;
      var stringval=trimString(string,start,end);
      if (start<end) 
	mk_getambig(stringval,varname,true,true);}
    elt.value=''; evt.preventDefault(); evt.cancelBubble=true;
    return false;}
}

function mk_search_keypress(evt)
{
  var elt=evt.target;
  var varname=mk_getvarname(elt);
  mkw_textchange(elt);
  var ch=evt.charCode, kc=evt.keyCode;
  if (kc==13) document.forms[0].submit();
  else if (ch==0x3b) mk_disambig(elt.value,'Q');
}

function mkw_textclick(evt)
{
  var elt=evt.target;
  var varname=mk_getvarname(elt);
  var string=elt.value; var len=string.length;
  var start=elt.selectionStart, end=elt.selectionStart;
  while ((start>0) && (string.charAt(start)!=';')) start--;
  if (string.charAt(start)==';') start++;
  while ((end<len) && (string.charAt(end)!=';')) end++;
  if (end>start+1) 
    mk_getambig(trimString(string,start,end),varname,true);
}

function mkw_textchange(keyword_elt)
{
  var previous=keyword_elt.getAttribute('old_value');
  var current=keyword_elt.value, cursor;
  if (!(previous)) previous='';
  keyword_elt.setAttribute('old_value',current);
  cursor=mkw_get_changepos(previous,current);
  if (cursor>=0)
    keyword_elt.setAttribute('cursor',cursor);
}

function mkw_get_changepos(oldval,newval)
{
  var i=0, old_len=oldval.length, new_len=newval.length;
  var in_phrase=false, boundary=true, dterm=false;
  var pt=0, phrase_start=-1, phrase_end=-1, word_start=0, word_end;
  if (oldval == newval) return -1;
  if (new_len==0) return 0;
  while (i<new_len) {
    var ch=newval.charAt(i);
    if (i>=old_len) return new_len;
    else if (ch!=oldval.charAt(i)) return i;
    else i++;}
  return new_len;
}

function mkw_insert_semi(elt)
{
  var cursor=elt.selectionStart;
  var current=elt.value;
  elt.value=current.slice(0,cursor)+';'+current.slice(cursor);
}

function mkw_mouseout(evt)
{
  var elt=fdbFindClass(evt.target,'mkwidget');
  if (elt) {
    var varname=elt.getAttribute('VAR');
    var controls_elt=fdbByID(varname+'_CONTROLS');
    var keywords_elt=fdbByID(varname+'_KEYWORDS');
    if (!((keywords_elt.focused)||(keywords_elt.value.length>0)))
      controls_elt.style.display='none';}
}

function mkw_mouseover(evt)
{
  var elt=fdbFindClass(evt.target,'mkwidget');
  if (elt) {
    var varname=elt.getAttribute('VAR');
    var controls_elt=fdbByID(varname+'_CONTROLS');
    var keywords_elt=fdbByID(varname+'_KEYWORDS');
    elt.mousefree=false; controls_elt.style.display='block';}
}

function mkw_keywords_focus(evt)
{
  var elt=evt.target;
  elt.focused=true;
  elt.style.color='blue';
}

function mkw_keywords_blur(evt)
{
  var elt=evt.target;
  var mkwidget=fdbFindClass(elt,'mkwidget');
  var varname=mk_getvarname(elt);
  elt.focused=false;
  elt.style.color='gray';
  if (mkwidget.mouseless) {
    var controls_elt=fdbByID(varname+'_CONTROLS');
    controls_elt.style.display='none';}
}

/* AJAX magic */

/* Handling getambig results */

function mk_getambig(keyterm,varname,define)
{
  if (mk_ambiguity_elt()) {
    var ambig_elt=mk_get_ambig_elt(keyterm,varname);
    if (ambig_elt) return ambig_elt;
    // We don't have it, so we need to request it.
    // First we make a placeholder (this serves as a kind of lock as well,
    // since subsequent calls to getmeanings won't generate a request
    // because they'll find a placeholder.
    if (varname) mk_add_tag(varname,keyterm);
    var req=new XMLHttpRequest();
    req.onreadystatechange=function() {
      if ((req.readyState == 4) && (req.status == 200)) {
	var base=false;
	var doc=req.responseXML.documentElement;
	_mk_handle_getambig(doc,varname,define);}}
    req.open("GET","getambig.fdcgi?KEYWORD="+encodeURIComponent(keyterm)+
	     "&"+mk_getcontext(varname),true);
    req.send(null);
    return ambig_elt;}
}

var meanings_to_show=2;

function mk_tag_diamond(meaning)
{
  var oid=meaning.getAttribute('oid');
  var tag=meaning.getAttribute('tag');
  var gloss=meaning.getAttribute('gloss');
  var base=meaning.getAttribute('base');
  var anchor_elt=document.createElement('A');
  var img_elt=document.createElement('IMG');
  anchor_elt.className='oid';
  anchor_elt.setAttribute('oid',oid);
  anchor_elt.setAttribute('gloss',gloss);
  anchor_elt.setAttribute('base',base);
  anchor_elt.setAttribute('tag',tag);
  img_elt.src='bm_diamond.png'; img_elt.alt='+';
  fdbAppend(anchor_elt,img_elt);
  return anchor_elt;
}

function _mk_handle_getambig(result,varname,require)
{
  var ambig_container=mk_ambiguity_elt();
  var base=result.getAttribute('keyword');
  var meanings=result.childNodes, selected_meaning=false;
  var exposed=true, n_meanings=((meanings) ? (meanings.length) : (0)),
    assigned=0, to_show=
    ((n_meanings<meanings_to_show) ? (n_meanings) :
     (n_meanings==(meanings_to_show+1)) ? (n_meanings) :
     (meanings_to_show));
  // Create the ambig element
  var ambig_elt=document.createElement('select');
  ambig_elt.setAttribute('term',base);
  ambig_elt.className='ambiguity';
  var count_node=document.createElement('option');
  count_node.className='narrative';
  fdbAppend(count_node,fdbText('The word "'+base+'" '));
  if (meanings)
    if (meanings.length==0)
      fdbAppend(count_node,fdbText('is unrecognized'));
    else if (meanings.length==1)
      fdbAppend(count_node,fdbText('is unambiguous'));
    else
      count_node.appendChild
	(fdbText('has '+meanings.length+' meanings'));
  else fdbAppend(count_node,fdbText('is unrecognized'));
  fdbAppend(ambig_elt,count_node);
  if (meanings) {
    var i=0; while (i<meanings.length) {
      var meaning=meanings[i++];
      var oid=meaning.getAttribute('oid');
      var dterm=meaning.getAttribute('dterm');
      var gloss=meaning.getAttribute('gloss');
      var frequency=meaning.getAttribute('frequency');
      var score=meaning.getAttribute('score');
      var disambiguator=meaning.getAttribute('disambiguator');
      var selected=meaning.getAttribute('selected');
      var tag=meaning.getAttribute('tag');
      var shortgloss=gloss;
      if (gloss.indexOf(';')>0) 
	shortgloss=gloss.slice(0,gloss.indexOf(';'));
      // var elt=document.createElement('div');
      var elt=document.createElement('option');
      elt.className='meaning';
      fdbAppend(elt,fdbText(shortgloss));
      elt.title=gloss;
      elt.setAttribute('oid',oid);
      elt.setAttribute('base',base);
      elt.setAttribute('dterm',dterm);
      elt.setAttribute('gloss',gloss);
      elt.setAttribute('score',score);
      elt.setAttribute('tag',tag);
      elt.title=gloss;
      if ((selected) && (varname)) {
	selected_meaning=elt;
	assigned++;}
      if (exposed)
	if (i>to_show) { /* ((score<=1) && (i>to_show)) */
	  var showmore=document.createElement('option');
	  showmore.className='showmore';
	  showmore.appendChild
	    (fdbText
	     ('There are '+((meanings.length-i)+1)+' more meanings...'));
	  showmore.onmouseover=showmore_mouseover;
	  ambig_elt.removeChild(count_node);
	  fdbAppend(ambig_elt,showmore);
	  exposed=false;}
      if (exposed) elt.setAttribute('exposed','yes');
      fdbAppend(ambig_elt,elt);}}
  var current_elt=mk_get_ambig_elt(base);
  if (varname) {
    var tag=mk_add_tag(varname,base);
    if (assigned==1) 
      mk_tag_set_meaning(tag,selected_meaning);
    else if (assigned>0) {
      var i=0; while (i<meanings.length) {
	var meaning=meanings[i++];
	if (meaning.getAttribute('selected')) 
	  mk_tag_add_meaning(tag,meaning);}}
    else {}}
  if (current_elt)
    ambig_container.replaceChild(ambig_elt,current_elt);
  else fdbAppend(ambig_container,ambig_elt);
}

var prompt_to_remove=false;

function remove_define_prompt(evt)
{
  var prompt=fdbFindClass(evt.target,'define_prompt');
  if (prompt) {
    prompt.parentNode.removeChild(prompt);}
  return false;
}

function mk_open_define_window(evt)
{
  var elt=evt.target;
  var base=elt.getAttribute('base');
  var varname=elt.getAttribute('var');
  window.open(elt.href,'',
	      'innerWidth=512px,innerHeight=575px,status=1,location=0,resizable=1,scrollbars,right=75,top=20',0);
  mk_remove_ambig_elt(base);
  pending_ambig=base; pending_ambig_var=varname;
  return false;
}

/* Getting meanings */

var meanings=[];

function getmeanings(keyterm,varname,with_alt)
{
  if (meanings[keyterm])
    gotmeanings(meanings[keyterm],varname);
  else {
    var req=new XMLHttpRequest();
    req.onreadystatechange=function() {
      if ((req.readyState == 4) && (req.status == 200)) {
	var base=false;
	var doc=req.responseXML.documentElement;
	gotmeanings(doc,varname,with_alt);}}
    req.open("GET","getmeanings.fdcgi?"+
	     "KEYWORD="+encodeURIComponent(keyterm),true);
    req.send(null);}
}

function gotmeanings(result,varname,with_alt)
{
  var keyword=fdbGet(result,'keyword');
  var meanings_elt=fdbByID('NEW_'+varname);
  // window.status='gotmeanings '+varname+'; toelt='+meanings_elt;
  var meanings=result.childNodes;
  if ((meanings) && (meanings.length>0)) {
    var i=0, n_selected=0; while (i<meanings.length) {
      var meaning=meanings[i++];
      var selected=fdbGet(meaning,'selected');
      if (selected) n_selected++;
      if ((with_alt) || (selected))
	add_meaning(meaning,meanings_elt,
		    varname,fdbGet(meaning,'selected'));}
    if (!((with_alt) || (n_selected>0))) {
      var i=0; while (i<meanings.length) {
	var meaning=meanings[i++];
	add_meaning(meaning,meanings_elt,varname,false);}}}
  else add_define_link(keyword,meanings_elt,false);
}

/* Getting expansions */

function _mk_add_expansion_placeholder(oid,gloss)
{
  var expansions_elt=mk_expansions_elt();
  var new_node=document.createElement('div');
  new_node.className='placeholder';
  new_node.id=fdbOid2Id(oid,'EX');
  new_node.setAttribute('oid',oid);
  new_node.style.display='none';
  if (gloss) {
    var glossnode=document.createElement('div');
    glossnode.className='gloss';
    fdbAppend(glossnode,fdbText(gloss));
    fdbAppend(new_node,glossnode);}
  fdbAppend(new_node,fdbText('Loading expansions ...'));
  fdbAppend(expansions_elt,new_node);
  return new_node;
}

function mk_getexpansions()
{
  var expansions_elt=mk_expansions_elt();
  var expansions=expansions_elt.childNodes;
  var query_string='';
  var i=0; while (i<expansions.length)
    if ((expansions[i].nodeType==1) &&
	(expansions[i].className=='placeholder')) {
      var oid=expansions[i].getAttribute('oid');
      query_string=query_string+'CONCEPTS='+encodeURIComponent(oid)+'&';
      i++;}
    else i++;
  if (query_string=='') return;
  var req=new XMLHttpRequest();
  req.onreadystatechange=function() {
    if ((req.readyState == 4) && (req.status == 200)) {
      var base=false;
      var doc=req.responseXML.documentElement;
      _mk_handle_getexpansions(doc);}}
  req.open("GET","getexpansions.fdcgi?"+query_string,true);
  req.send(null);
}

function mk_getexpansion(oid,gloss)
{
  var exelt=mk_tryexpansion(oid);
  if ((exelt) && (exelt.className != 'placeholder'))
    return exelt;
  else if (exelt) {}
  else _mk_add_expansion_placeholder(oid,gloss);
  var req=new XMLHttpRequest();
  req.onreadystatechange=function() {
    if ((req.readyState == 4) && (req.status == 200)) {
      var base=false;
      var doc=req.responseXML.documentElement;
      _mk_handle_getexpansions(doc);}}
  req.open("GET","getexpansions.fdcgi?CONCEPTS="+encodeURIComponent(oid)+
	   "&",true);
  req.send(null);
}

function mk_needexpansion(oid,gloss)
{
  var exelt=mk_tryexpansion(oid);
  if (exelt) return exelt;
  else return _mk_add_expansion_placeholder(oid,gloss);
}

function _mk_handle_getexpansions(expansions)
{
  var nodes=expansions.childNodes;
  var i=0; while (i<nodes.length) {
    var expansion=nodes[i++];
    if (expansion.nodeType==1)
      _mk_add_expansion(expansion);}
  
}

function _mk_add_expansion(expansion)
{
  var oid=expansion.getAttribute('oid');
  var placeholder=mk_tryexpansion(oid);
  var nodes=expansion.childNodes;
  var seen=[];
  var i=0, lim=nodes.length, n_concepts=0;
  var ex_elt=document.createElement('div');
  // var head_elt=document.createElement('span');
  var glosselt=document.createElement('div');
  ex_elt.className='expansion';
  ex_elt.id=fdbOid2Id(oid,'EX');
  //  head_elt.appendChild
  //    (fdbText(expansion.getAttribute('dterm')));
  //  fdbAppend(ex_elt,head_elt);
  fdbAppend(ex_elt,fdbText(' expands to '));
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
    var terms=meaning_elt.childNodes; var j=0, jlim=terms.length, n_terms=0;
    if ((n_concepts>0) && (jlim>0)) {
      fdbAppend(ex_elt,fdbText(' < '));}
    while (j<jlim) {
      var term_node=terms[j++]; var term=term_node.getAttribute('term');
      if (j>1) {
	var k=0; while (k<seen.length)
	  if (seen[k]==term) break; else k++;
	if (k<seen.length) continue;}
      seen[seen.length]=term;
      var text_span=document.createElement('span');
      text_span.className='term';
      if (term.indexOf(' ')<0)
	fdbAppend(text_span,fdbText(term));
      else fdbAppend(text_span,fdbText('"'+term+'"'));
      if (n_terms>0)
	fdbAppend(metakeyterms,fdbText(' . '));
      n_terms++;
      fdbAppend(metakeyterms,text_span);}
    fdbAppend(ex_elt,metakeyterms);
    n_concepts++;}
  glosselt.className='gloss';
  fdbAppend(glosselt,fdbText(expansion.getAttribute('gloss')));
  fdbAppend(ex_elt,glosselt);
  if (placeholder) {
    if (expanded_elt==placeholder) expanded_elt=ex_elt;
    ex_elt.style.display=placeholder.style.display;
    placeholder.parentNode.replaceChild(ex_elt,placeholder);}
  else {
    var expansions_elt=mk_expansions_elt();
    fdbAppend(expansions,ex_elt);}
}

/* Doing disambiguation */

var disambig_script='disambig.fdcgi';

function mk_disambig(string,varname)
{
  var req=new XMLHttpRequest();
  var qstring='KEYWORDS='+encodeURIComponent(string)+'&'+
    mk_getcontext(varname);
  req.onreadystatechange=function() {
    if ((req.readyState == 4) && (req.status == 200)) {
      var base=false;
      var doc=req.responseXML.documentElement;
      _mk_handle_disambig(doc,varname);}}
  req.open("GET",disambig_script+"?"+qstring,true);
  req.send(null);
}

function _mk_clear_new(varname)
{
  var elt=fdbByID(varname);
  if ((elt) && (elt.childNodes)) {
    var children=elt.childNodes;
    var i=0; while (i<children.length)
      if ((children[i].tagName=='SPAN') &&
	  (children[i].className=='new')) {
	var child=children[i];
	var grandchildren=child.childNodes;
	if (grandchildren) {
	  var j=0; while (j<grandchildren.length)
	    child.removeChild(grandchildren[j++]);}
	return;}
      else i++;}
}

function _mk_handle_disambig(data,varname)
{
  var nodes=data.childNodes;
  _mk_clear_new(varname);
  var i=0; while (i<nodes.length) {
    var meaning=nodes[i++];
    if (meaning.nodeType==1) {
      var oid=meaning.getAttribute('oid');
      var base=meaning.getAttribute('base');
      var term=meaning.getAttribute('term');
      var dterm=meaning.getAttribute('dterm');
      var gloss=meaning.getAttribute('gloss');
      var elt=mk_add_tag(varname,term);
      mk_tag_set_meaning(elt,meaning);
      mk_needexpansion(oid);}}
  mk_getexpansions();
}

/* Hints */

var last_hint=false;

function hint_click(evt)
{
  var tagelt=evt.target;
  while (tagelt)
    if ((tagelt.className=='tag') ||
	(tagelt.className=='texttag') ||
	(tagelt.className=='ambigtag') ||
	(tagelt.className=='meaning') ||
	(tagelt.className=='metakey'))
      break;
    else tagelt=tagelt.parentNode;
  if (!(tagelt)) return;
  if (tagelt.className=='texttag') {
    var string=tagelt.getAttribute('text');
    var newtagelt=add_tag(current_varname,string);
    tag_getmeanings(string,newtagelt,false);
    return;}
  else if (tagelt.className=='meaning') {
    set_tag_meaning(tagelt.fortag,tagelt);
    return;}
  var addto=fdbByID('NEW_'+current_varname);
  if (!(addto)) addto=fdbByID(current_varname);
  var checkbox=document.createElement('input');
  checkbox.type='checkbox'; checkbox.name=current_varname;
  checkbox.value=fdbGet(tagelt,'tag'); checkbox.checked=true;
  var taghead=get_taghead(tagelt);
  taghead.insertBefore(checkbox,taghead.firstChild);
  tagelt.parentNode.removeChild(tagelt);
  if (tagelt.className=='texttag')
    tagelt.title='unresolved keyword: click to define';
  else if (tagelt.getAttribute('gloss'))
    tagelt.title='click for more: '+tagelt.getAttribute('gloss');
  else tagelt.title='click for more';
  tagelt.style.textDecoration='none';
  fdbAppend(addto,fdbText(' '));
  fdbAppend(addto,tagelt);
}

function hint_copy_click(evt)
{
  var tagelt=evt.target;
  while (tagelt)
    if ((tagelt.className=='tag') ||
	(tagelt.className=='texttag') ||
	(tagelt.className=='ambigtag') ||
	(tagelt.className=='meaning') ||
	(tagelt.className=='metakey'))
      break;
    else tagelt=tagelt.parentNode;
  if (!(tagelt)) return;
  if ((last_hint==tagelt) && (page_submit_fn)) {
    if (page_submit_fn) page_submit_fn();
    return false;}
  else last_hint=tagelt;
  if (tagelt.className=='texttag') {
    var string=tagelt.getAttribute('text');
    var newtagelt=add_tag(current_varname,string);
    tag_getmeanings(string,newtagelt,false);
    return;}
  var addto=fdbByID('NEW_'+current_varname);
  if (!(addto)) addto=fdbByID(current_varname);
  var newelt=tagelt.cloneNode(true);
  var checkbox=document.createElement('input');
  checkbox.type='checkbox'; checkbox.name=current_varname;
  checkbox.value=fdbGet(tagelt,'tag'); checkbox.checked=true;
  var taghead=get_taghead(newelt);
  taghead.insertBefore(checkbox,taghead.firstChild);
  if (newelt.className=='texttag')
    newelt.title='unresolved keyword: click to define';
  else if (newelt.getAttribute('gloss'))
    newelt.title='click for more: '+tagelt.getAttribute('gloss');
  else newelt.title='click for more';
  newelt.style.textDecoration='none';
  fdbAppend(addto,fdbText(' '));
  fdbAppend(addto,newelt);
}

function ambiginfo_click(evt)
{
  var target=evt.target, newelt;
  while (target)
    if ((target.className=='tag') ||
	(target.className=='texttag') ||
	(target.className=='ambigtag') ||
	(target.className=='meaning') ||
	(target.className=='metakey'))
      break;
    else target=target.parentNode;
  if (!(target)) return;
  else if (target.className=='meaning') {
    var ambiginfo=fdbFindClass(target,"ambiginfo");
    var ambigresult=ambiginfo.result;
    var tagelt=ambiginfo.fortag;
    // Update the ambigresult to be used by recompute_ambiginfo
    var oid=fdbGet(target,'oid');
    /* Update the getmeanings result info to reflect the new selection. */
    fdbCopyAttribs(ambigresult,target,
		   'oid','dterm','gloss','synonyms','tag');
    fdbSetAttrib(ambigresult,'SELECTED',1);
    var meanings=ambigresult.childNodes, i=0;
    while (i<meanings.length) {
      var meaning=meanings[i++];
      if (meaning.nodeType!=1) {}
      else if (fdbGet(meaning,'oid')==oid) {
	fdbSetAttrib(meaning,'selected','true');}
      else {
	fdbRemoveAttrib(meaning,'selected');}}
    // Replace the ambiginfo
    fdbReplace(ambiginfo,display_ambiginfo
	       (ambiginfo.tagName,ambigresult,tagelt));  
    // Set the tag meaning
    set_tag_meaning(tagelt,target);
    evt.preventDefault(); evt.cancelBubble=true;
    return false;}
  else {
    var addto=fdbByID('NEW_'+current_varname);
    if (!(addto)) addto=fdbByID(current_varname);
    else newelt=tagelt.fortag.cloneNode(true);
    if (tagelt.className=='texttag')
      newtagelt.title='the keyword "'+tagelt.getAttribute('text')+'"';
    else if (tagelt.getAttribute('gloss'))
      newtagelt.title='click for more: '+tagelt.getAttribute('gloss');
    else newtagelt.title='click for more';
    var checkbox=document.createElement('input');
    checkbox.type='checkbox'; checkbox.name=current_varname;
    checkbox.value=tagelt.getAttribute('tag'); checkbox.checked=true;
    var taghead=get_taghead(newtagelt);
    taghead.insertBefore(checkbox,taghead.firstChild);
    newtagelt.setAttribute('dontdefine','yes');
    newtagelt.style.textDecoration='none';
    fdbAppend(addto,fdbText(' '));
    fdbAppend(addto,newtagelt);}
}

/* Tag support functions */

function add_tag(to,string,meaning)
{
  var varname=false, addto;
  if (typeof to == 'string') {
    addto=fdbByID('NEW_'+to); varname=to;
    if (!(addto)) addto=fdbByID(varname);}
  else addto=to;
  if (!(addto)) {
    alert('No place to put varname '+to);
    return;}
  // var promptelt=fdbByID('NEW_'+varname+'_PROMPT');
  // if (promptelt) promptelt.style.display='inline';
  var tagval, tag=document.createElement('span'); 
  var taghead=document.createElement('span'); taghead.className='taghead';
  if (meaning) {
    if (varname)
      tag.title='click for more: '+meaning.getAttribute('gloss');
    else tag.title='click to add: '+meaning.getAttribute('gloss');
    tag.className='tag';
    tagval=meaning.getAttribute('tag');
    tag.setAttribute('meanings',1);
    tag.setAttribute('oid',tag.getAttribute('oid'));}
  else {
    tag.className='texttag';
    tag.title='unresolved keyword: click to define';
    tagval=':("'+string+'")';
    tag.setAttribute('meanings',0);}
  tag.setAttribute('text',string);
  tag.setAttribute('tag',tagval);
  if (varname) {
    var checkbox=document.createElement('input');
    checkbox.type='checkbox'; checkbox.name=varname;
    checkbox.value=tagval; checkbox.checked=true;
    fdbAppend(taghead,checkbox);}
  fdbAppend(taghead,fdbText(string));
  fdbAppend(tag,taghead);
  if (meaning) {
    fdbAppend(tag,fdbText(' '));
    fdbAppend(tag,display_meaning(meaning));}
  fdbAppend(addto,tag);
  return tag;
}

function set_tag_meaning(tagelt,meaning,invisibly)
{
  var tagval=meaning.getAttribute('tag');
  var oid=meaning.getAttribute('oid');
  tagelt.className='tag';
  tagelt.setAttribute('tag',tagval);
  tagelt.setAttribute('oid',oid);
  tagelt.setAttribute('meanings',1);
  var checkbox=get_tagcheck(tagelt);
  checkbox.value=tagval;
  var children=tagelt.childNodes, toremove=[];
  var i=0; while (i<children.length)
    if (children[i].nodeType!=1) i++;
    else if (children[i] == meaning) i++;
    else if ((children[i].className=='meaning') ||
	     (children[i].className=='hidden_narrative') ||
	     (children[i].className=='definelink') ||
	     (children[i].className=='hideshow') ||
	     (children[i].className=='extrameanings')) {
      toremove.push(children[i]); i++;}
    else i++;
  i=0; while (i<toremove.length) tagelt.removeChild(toremove[i++]);
  fdbAppend(tagelt,meaning);
  if (!(invisibly)) {
    meaning.style.display='inline';
    tagelt.tagexpanded=true;}
  var gloss=fdbGet(meaning,'gloss');
  tagelt.setAttribute('gloss',gloss);
  tagelt.title='click for less: '+gloss;
  meaning.style.textDecoration='underline';
}

function get_tagcheck(tagelt)
{
  var elts=tagelt.getElementsByTagName('INPUT');
  if (elts) return elts[0];
  else return false;
}

function get_taghead(tagelt)
{
  var children=tagelt.childNodes;
  var i=0; while (i<children.length) 
    if ((children[i].nodeType==1) &&
	(children[i].className=='taghead'))
      return children[i];
    else i++;
  return false;
}

function display_meaning(meaning)
{
  var display=document.createElement('span');
  display.className='meaning';
  display.setAttribute('oid',meaning.getAttribute('oid'));
  display.setAttribute('dterm',meaning.getAttribute('dterm'));
  display.setAttribute('gloss',meaning.getAttribute('gloss'));
  display.title='select ('+meaning.getAttribute('frequency')+' items) '
    +meaning.getAttribute('gloss');
  if (meaning.getAttribute('selected'))
    display.style.textDecoration='underline';
/*   var diamoid=document.createElement('a'); */
/*   diamoid.className='diamond'; diamoid.target='_new'; */
/*   diamoid.title='click to edit: '+display.title; */
/*   diamoid.href='define.fdcgi?CONCEPT='+meaning.getAttribute('oid'); */
/*   diamoid.onclick=define_link_click; */
/*   var diamoidimg=document.createElement('img'); */
/*   diamoidimg.alt='+'; diamoidimg.src='bm_diamond.png'; */
/*   fdbAppend(diamoid,diamoidimg); */
/*   fdbAppend(display,diamoid); */
  if (meaning.getAttribute('disambiguator')) {
    var disambiguator=document.createElement('span');
    disambiguator.className='disambiguator';
    var disambigtext=fdbText
      (meaning.getAttribute('disambiguator'));
    fdbAppend(disambiguator,disambigtext);
    fdbAppend(display,disambiguator);}
  else {
    var dterm=meaning.getAttribute('dterm');
    var split=dterm.indexOf(':');
    if (split>=0) {
      fdbAppend(meaning,fdbText(dterm.slice(0,split)));
      var disambiguator=document.createElement('span');
      disambiguator.className='disambiguator';
      fdbAppend(disambiguator,fdbText(dterm.slice(split)));
      fdbAppend(display,disambiguator);}
    else {
      var disambiguator=document.createElement('span');
      disambiguator.className='disambiguator';
      fdbAppend(disambiguator,dterm);
      fdbAppend(display,disambiguator);}}
  return display;
}

/* Generating ambiginfo (explanatory detail) */

function ambiginfo_synonym(synonym)
{
  var textspan=document.createElement('span');
  textspan.title='add this synonym';
  textspan.className='texttag';
  textspan.setAttribute('text',synonym);
  textspan.setAttribute('tag',':("'+synonym+'")');
  var head=document.createElement('span');
  head.className='taghead'; 
  fdbAddWord(head,synonym);
  fdbAppend(textspan,head);
  return textspan;
}

function display_ambiginfo(tagname,result,tagelt)
{
  var ambiginfo=document.createElement(tagname);
  ambiginfo.className='ambiginfo'; ambiginfo.id='AMBIGINFO';
  ambiginfo.result=result; ambiginfo.fortag=tagelt;
  ambiginfo.onclick=ambiginfo_click; 
  var term=result.getAttribute('keyword');
  var n_possible=result.getAttribute('count');
  var n_selected=result.getAttribute('selected');
  if (n_possible==0) {
    var term=fdbGet(tagelt,'text');
    fdbAppend(ambiginfo,(fdbText('The term "'+term+'" is unknown.  ')));
    var anchor=
      fdbAppend(ambiginfo,
		fdbAnchor('define.fdcgi?TERM='+encodeURIComponent(term),
			  'definelink','_new',
			  'Define a meaning for "'+term+'".'));
    anchor.onclick=define_link_click;
    return ambiginfo;}
  else if (n_selected==1) {
    var oid=fdbGet(result,'oid');
    var dterm=fdbGet(result,'dterm');
    var dtermspan=fdbSpan('dterm');
    if (dterm.indexOf(':')>0) {
      var colon=dterm.indexOf(':');
      fdbAppend(dtermspan,fdbText(dterm.slice(0,colon)));
      var disambiguator=fdbSpan('disambiguator');
      fdbAppend(disambiguator,fdbText(dterm.slice(colon)));
      fdbAppend(dtermspan,disambiguator);}
    else fdbAppend(dtermspan,fdbText(dterm));
    fdbAppend(ambiginfo,dtermspan);
    fdbAppend(ambiginfo,fdbText(' '));
    var synonyms=result.getAttribute('synonyms');
    if (synonyms) {
      fdbAppend(ambiginfo,fdbNarrative(' aka '));
      var start=0, end=synonyms.indexOf(';',start);
      while (end>0) {
	if (start==0) fdbAppend(ambiginfo,' ');
	else fdbAppend(ambiginfo,fdbNarrative(' or '));
	fdbAppend(ambiginfo,ambiginfo_synonym(synonyms.slice(start,end)));
	start=end+1; end=synonyms.indexOf(';',start);}}
    var gloss=result.getAttribute('gloss');
    if (gloss) {
      fdbAppend(ambiginfo,fdbNarrative(' defined as '));
      fdbAppend(ambiginfo,fdbSpan('gloss','('+gloss+')'));}
    if (n_selected>n_possible)
      fdbAppend(ambiginfo,fdbNarrative(' might also mean '));}
  else if (n_selected>0) {
    fdbAppend(ambiginfo,fdbNarrative
	      ('"'+term+'" has '+n_selected+' likely meanings out of '+
	       n_possible+' meanings: '));
    var meanings=result.childNodes, i=0, count=0;
    while (i<meanings.length) 
      if (meanings[i].nodeType==1) {
	var meaning=meanings[i];
	if (meanings[i].getAttribute('selected')) {
	  if (count>0) fdbAppend(ambiginfo,fdbNarrative(' or '));
	  else fdbAppend(ambiginfo,fdbText(' '));
	  fdbAppend(ambiginfo,display_meaning(meaning));}
	i++; count++;}
      else i++;}
  else {
    fdbAppend(ambiginfo,fdbNarrative
	      ('Couldn\'t resolve "'+term+'" to any of '+n_possible+
	       ' meanings:'));
    var meanings=result.childNodes, i=0, count=0;
    while (i<meanings.length) 
      if (meanings[i].nodeType==1) {
	var meaning=meanings[i];
	if (count>0) fdbAppend(ambiginfo,fdbNarrative(' or '));
	else fdbAppend(ambiginfo,fdbText(' '));
	fdbAppend(ambiginfo,display_meaning(meaning));
	i++; count++;}
      else i++;}
  var meanings=result.childNodes, i=0, count=0;
  while (i<meanings.length)
    if (meanings[i].nodeType==1) {
      var meaning=meanings[i];
      if (fdbGet(meaning,'selected')) i++;
      else if (fdbGet(meaning,'visible')) {
	if (count>0) fdbAppend(ambiginfo,fdbNarrative(' or '));
	else fdbAppend(ambiginfo,fdbNarrative(' '));
	fdbAppend(ambiginfo,display_meaning(meaning));
	i++; count++;}
      else i++;}
    else i++;
  if (n_selected+count<n_possible) {
    fdbAppend(ambiginfo,fdbHideShow('HIDDENMEANINGS'));
    var hidden=fdbAppend(ambiginfo,fdbSpan('hiddenmeanings'));
    hidden.id='HIDDENMEANINGS'; hidden.style.display='none';
    count=0; i=0; while (i<meanings.length) 
      if (meanings[i].nodeType==1) {
	var meaning=meanings[i];
	if (fdbGet(meaning,'selected')) i++;
	else if (fdbGet(meaning,'visible')) i++;
	else {
	  if (count>0) fdbAppend(hidden,fdbNarrative(' or '));
	  else fdbAppend(hidden,fdbText(' '));
	  fdbAppend(hidden,display_meaning(meaning));
	  i++; count++;}}
      else i++;}
  return ambiginfo;
}

/* Handling the results of disambiguation */

/* Tag handlers */

function define_link_click(evt)
{
  var elt=evt.target;
  while (elt)
    if (elt.href) break;
    else elt=elt.parentNode;
  // window.status='define_link_click '+elt;
  if (elt) {
    window.open(elt.href,'',
		'innerWidth=512px,innerHeight=575px,status=1,location=0,resizable=1,scrollbars,right=75,top=20',0);
    evt.preventDefault(); evt.cancelBubble=true;
    return false;}
}

function endsinstarp(string)
{
  if (string.slice(string.length-1)=='*') return true;
  else return false;
}

function tag_keypress(evt)
{
  var elt=evt.target;
  var kc=evt.keyCode;
  var ch=evt.charCode;
  if (kc==13)
    if (elt.value=='') {
      var submit_command=elt.getAttribute('COMMAND');
      if (submit_command)
	_fdb_clickit_command(submit_command);
      return false;}
    else {
      var varname=mk_getvarname(elt);
      var string=elt.value, show_all=false;
      if (endsinstarp(string)) {
	show_all=true; string=string.slice(0,-1);}
      window.status='string='+string+' show_all='+show_all;
      evt.preventDefault(); evt.cancelBubble=true;
      var tagelt=add_tag(varname,string);
      tag_getmeanings(string,tagelt,false);
      elt.value=''; 
      return false;}
  else if (ch==63) {
    var varname=mk_getvarname(elt);
    var string=elt.value; 
    var tagelt=add_tag(varname,string);
    evt.preventDefault(); evt.cancelBubble=true;
    tag_getmeanings(string,tagelt,true);
    elt.value=''; 
    return false;}
  else return true;
}

function tag_click(evt)
{
  var tagelt=evt.target; var meaning=false;
  while (tagelt)
    if (tagelt.tagName=='INPUT') return;
    else if ((tagelt.className=='tag') ||
	     (tagelt.className=='texttag') ||
	     (tagelt.className=='ambigtag'))
      break;
    else if (tagelt.className=='meaning') {
      meaning=tagelt; tagelt=tagelt.parentNode;}
    else tagelt=tagelt.parentNode;
  if (!(tagelt)) {
    window.status='No tag elt';
    return;}
  else if (meaning) {
    set_tag_meaning(tagelt,meaning);
    return;}
  else if (tagelt.className=='texttag')
    if (tagelt.getAttribute('dontdefine')) {}
    else open_define_dialog(tagelt.getAttribute('text'));
  else {
    var children=tagelt.childNodes, display_value;
    if (tagelt.tagexpanded) {
      tagelt.title='click for more: '+tagelt.getAttribute('gloss');
      tagelt.tagexpanded=false; display_value='none';}
    else {
      tagelt.title='click for less: '+tagelt.getAttribute('gloss');
      tagelt.tagexpanded=true; display_value='inline';}
    var i=0; while (i<children.length) {
      var child=children[i++];
      if (child.nodeType!=1) {}
      else if ((child.className=='meaning') ||
	       (child.className=='definelink') ||
	       (child.className=='hidden_narrative'))
	child.style.display=display_value;
      else if (child.className=='hideshow')
	if (display_value=='none') {
	  fdbSetHideShow(child,false);
	  child.style.display=display_value;}
	else child.style.display=display_value;}}
}

function open_define_dialog(arg)
{
  if (!(arg)) alert('Bad define call');
  else if (arg[0]==':')
    window.open('define.fdcgi?CONCEPT='+arg+'&'+mk_getcontext(false),
		'',
		'innerWidth=512px,innerHeight=575px,status=1,'+
		'location=0,resizable=1,scrollbars,right=75,top=20',0);
  else window.open('define.fdcgi?TERM='+encodeURIComponent(arg)+'&'+mk_getcontext(false),
		   '',
		   'innerWidth=512px,innerHeight=575px,status=1,'+
		   'location=0,resizable=1,scrollbars,right=75,top=20',0);
}


/* Getting tag meanings */

function tag_getmeanings(keyterm,tagelt,show_more)
{
  if (meanings[keyterm])
    disambig_tag(tagelt,meanings[keyterm],show_more);
  else {
    var req=new XMLHttpRequest();
    req.onreadystatechange=function() {
      if ((req.readyState == 4) && (req.status == 200)) {
	var base=false;
	var doc=req.responseXML.documentElement;
	disambig_tag(tagelt,doc,show_more);}}
    req.open("GET","getmeanings.fdcgi?"+
	     "KEYWORD="+encodeURIComponent(keyterm),true);
    req.send(null);}
}

function disambig_tag(tagelt,result,expanded)
{
  var ambiginfo=fdbByID('AMBIGINFO'), newambiginfo=false;
  var n_possible=fdbGet(result,'count');
  var n_selected=fdbGet(result,'selected');
  var tagval=fdbGet(result,'tag');
  var term=fdbGet(tagelt,'text');
  // Display new ambiginfo if appropriate
  if (ambiginfo)
    fdbReplace(ambiginfo,display_ambiginfo(ambiginfo.tagName,result,tagelt));
  // Update the tag element to reflect the results
  if (n_possible==0) {}
  else if (n_selected==1) {
    tagelt.className='tag';
    tagelt.setAttribute('gloss',result.getAttribute('gloss'));
    tagelt.title='click for more: '+result.getAttribute('gloss');}
  else {
    tagelt.className='ambigtag';
    tagelt.title='ambiguous: click for meanings';}
  tagelt.setAttribute('meanings',n_selected);
  tagelt.setAttribute('tag',tagval);
  /* Change the tag checkbox if there is one. */
  var checkbox=get_tagcheck(tagelt); if (checkbox) checkbox.value=tagval;
  /* Now, scan the meanings and add the selected ones (hidden). */
  var results=result.childNodes;
  var i=0,shown=0; while (i<results.length) 
    if (results[i].getAttribute('selected')) {
      if (shown>0) 
	fdbAppend(tagelt,fdbNarrative(' or ',true));
      else fdbAppend(tagelt,fdbText(' '));
      fdbAppend(tagelt,display_meaning(results[i]));
      shown++; i++;}
    else i++;
  /* Add the 'define new link' */
  fdbAppend(tagelt,fdbNarrative(' or ',true));
  var definelink=fdbAppend
    (tagelt,fdbAnchor('define.fdcgi?TERM='+encodeURIComponent(term),
		      'definelink','_new',
		      'define new'));
  definelink.onclick=define_link_click;
  if (shown<results.length) {
    var extra_meanings=fdbSpan('extrameanings'), extra=0;
    var hidden_meanings, hidden=0;
    fdbAppend(tagelt,fdbNarrative(' or ',true));
    var hideshow=fdbAppend(tagelt,fdbHideShow('_next'));
    hideshow.style.display='none';
    i=0; while (i<results.length)
      if (results[i].getAttribute('selected')) i++;
      else if (fdbGet(results[i],'visible')) {
	if (extra>0) fdbAppend(extra_meanings,fdbNarrative(' or '));
	fdbAppend(extra_meanings,display_meaning(results[i]));
	extra++; i++;}
      else i++;
    // We set this explicitly because it's used as a state variable
    // by the HideShow control.
    extra_meanings.style.display='none';
    fdbAppend(tagelt,extra_meanings);
    if (shown+extra<results.length)
      if (extra>0) {
	hidden_meanings=fdbSpan('extrameanings');
	fdbAdd(extra_meanings,fdbHideShow('_next','still more','hide these'));
	fdbAdd(extra_meanings,hidden_meanings);}
      else hidden_meanings=extra_meanings;
    if (shown+extra<results.length) {
      var hidden=0; i=0; while (i<results.length)
	if (results[i].getAttribute('selected')) i++;
	else if (fdbGet(results[i],'visible')) i++;
	else {
	  if (hidden>0) fdbAppend(hidden_meanings,fdbNarrative(' or '));
	  else fdbAppend(hidden_meanings,fdbText(' '));
	  fdbAppend(hidden_meanings,display_meaning(results[i]));
	  hidden++; i++;}}}
  if (expanded) {
    // Display the alternate meanings
    tagelt.tagexpanded=true;
    tagelt.title='click for less: '+tagelt.getAttribute('gloss');
    var tagchildren=tagelt.childNodes;
    var j=0; while (j<tagchildren.length) {
      var child=tagchildren[j++];
      if ((child.nodeType==1) &&
	  ((child.className=='meaning') ||
	   (child.className=='definelink') ||
	   (child.className=='hideshow') ||
	   (child.className=='hidden_narrative')))
	child.style.display='inline';}}
}

