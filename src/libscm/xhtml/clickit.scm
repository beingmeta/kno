;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc. All rights reserved

(in-module 'xhtml/clickit)

;;; This provides various XHTML widgets which use stylesheets and
;;;  javascript provided by webtools.css and webtools.js respectively.

(use-module 'webtools)
(use-module 'xhtml)


;;;; Getting the current URL

(define (geturl (w/query #f))
  (stringout (if (or (req/test 'https "on")
		     (= (req/get 'SERVER_PORT) 443))
		 "https://"
		 "http://")
    (req/get 'SERVER_NAME)
    (when (req/test 'SERVER_PORT)
      (unless (or (and (= (req/get 'SERVER_PORT) 80)
		       (not (req/test 'https "on")))
		  (= (req/get 'SERVER_PORT) 443))
	(printout ":" (req/get 'SERVER_PORT))))
    (try (req/get 'script_name) "")
    (try (req/get 'path_info) "")
    (when (and w/query (req/get 'query_string #f)
	       (or (not (empty-string? (req/get 'query_string #f)))
		   (position #\? (req/get 'request_uri ""))))
      (printout "?" (req/get 'query_string)))))

(module-export! 'geturl)

;;;; SITEURL

(define (siteurl app . args)
  (if (null? args)
      (mkpath (try (req/get 'appbase #{}) (dirname (geturl)))
	      app)
      (apply scripturl
	     (mkpath (try (req/get 'appbase #{}) (dirname (geturl)))
		     app)
	     args)))

(module-export! 'siteurl)

;;;; Favicons

(define (xhtml/favicon (icon "/favicon.ico"))
  (htmlheader (xmlelt "link" 'rel "shortcut icon" 'href icon)))
(module-export! 'xhtml/favicon)


;;;; Action anchors

(define (commandclick command label title)
  (anchor* (stringout "javascript:_fdb_clickit_command(':" command "');")
	   ((class "clickit")
	    (title title)
	    (onmouseover "_fdb_clickit_mouseover(event);")
	    (onmouseout "_fdb_clickit_mouseout(event);")	    )
	   label))
(define (actionclick action label title)
  (anchor* (stringout "javascript:_fdb_clickit_action('" action "');")
	   ((class "clickit")
	    (title title)
	    (onmouseover "_fdb_clickit_mouseover(event);")
	    (onmouseout "_fdb_clickit_mouseout(event);"))
	   label))
(define (acclick action command label title)
  (anchor* (stringout "javascript:_fdb_clickit_action_command('" action "',':" command "');")
	   ((class "clickit")
	    (title title)
	    (onmouseover "_fdb_clickit_mouseover(event);")
	    (onmouseout "_fdb_clickit_mouseout(event);"))
	   label))
(define (urlclick url label title)
  (anchor* url
	   ((class "clickit")
	    (title title)
	    (onmouseover "_fdb_clickit_mouseover(event);")
	    (onmouseout "_fdb_clickit_mouseout(event);"))
	   label))
(define (scriptclick script label title (alturl #f))
  (anchor* (or alturl "javascript:")
	   ((class "clickit")
	    (title title)
	    (onclick (stringout script "; return false;"))
	    (onmouseover "_fdb_clickit_mouseover(event);")
	    (onmouseout "_fdb_clickit_mouseout(event);"))
	   label))
(define (xurlclick url label title)
  (anchor* url
      ((class "clickit") (title title) (target "_blank")
       (onmouseover "_fdb_clickit_mouseover(event);")
       (onmouseout "_fdb_clickit_mouseout(event);"))
    label))
(module-export!
 '{commandclick
   actionclick
   urlclick
   xurlclick
   acclick
   scriptclick})


;;;; Tabs buttons

(define (tabbutton text image content
		   (selectvar 'livetab) (defaultselect #f)
		   (title #f))
  (if (and selectvar (req/test selectvar))
      (when (req/test selectvar content)
	(input id (stringout selectvar "_INPUT") 
	       type "HIDDEN" name (symbol->string selectvar)
	       value content))
      (when defaultselect
	(when selectvar
	  (req/set! selectvar content)
	  (input id (stringout selectvar "_INPUT") 
		 type "HIDDEN" name (symbol->string selectvar)
		 value content))))
  (span ((class "tab")
	 (selectvar (if selectvar (symbol->string selectvar)))
	 (title (if title title))
	 (shown (if (if selectvar (req/test selectvar content)
			defaultselect)
		    "yes"))
	 (contentid content))
    (if image
	(img SRC image ALT text)
	text)))

(module-export! 'tabbutton)


;;;; Hide/show buttons

(define (hideshow contentid hidetext showtext (var #f) (title #f))
  (span ((class "hideshow")
	 (onclick "_fdb_hideshow_toggle(event);")
	 (title (or title "click to hide or reveal additional content"))
	 (content contentid))
    "["
    (span ((class "whenHidden")) hidetext)
    (span ((class "whenVisible")) showtext)
    "]"))

(module-export! 'hideshow)


;;;; Vistoggle

(define vistoggle
  (macro expr
    `(xmlblock SPAN
	 ((class "vistoggle_hidden")
	  (onclick "_fdb_vistoggle(event);")
	  (content ,(get-arg expr 1)))
       ,@(subseq expr 2))))

(module-export! 'vistoggle)


;;;; Hot checkboxes (deprecated)

(define (hotcheck/radio var val (text #f) (title #f))
  (span ((class "hotcheck") (title (if title title))
	 (onclick "_fdb_hotcheck_click(event);")
	 (style (if (req/test var val) "font-weight: bold;")))
    (span ((class "left")) (or text val))
    (input TYPE "radio"
	   NAME (if (symbol? var) var (stringout var))
	   VALUE val
	   ("CHECKED" (req/test var val)))
    (span ((class "right")) (or text val))))

(define (hotcheck var val (text #f) (title #f))
  (span ((class "hotcheck") (title (if title title))
	 (onclick "_fdb_hotcheck_click(event);")
	 (style (if (req/test var val) "font-weight: bold;")))
    (span ((class "left")) (or text val))
    (input TYPE "checkbox"
	   NAME (if (symbol? var) var (stringout var))
	   VALUE val
	   ("CHECKED" (req/test var val)))
    (span ((class "right")) (or text val))))

(module-export! '{hotcheck hotcheck/radio})


;;;; Checkspans (like hotchecks but with a different name)

(define (checkspan/radio var val (text #f) (title #f) (checked) (handler #f))
  (default! checked (req/test var val))
  (span ((class (if checked "checkspan ischecked"
		    "checkspan"))
	 (title (if title title))
	 (onclick (if handler
		      (if (string? handler) handler
			  "fdjtCheckSpan_onclick(event);")))
	 (ischecked (if checked "yes")))
    (span ((class "left")) (or text val))
    (input TYPE "radio"
	   NAME (if (symbol? var) var (stringout var))
	   VALUE val
	   ("CHECKED" (if checked "CHECKED")))
    (span ((class "right")) (or text val))))

(define (checkspan var val (text #f) (title #f) (checked) (handler #f))
  (default! checked (req/test var val))
  (span ((class (if checked "checkspan ischecked" "checkspan"))
	 (title (if title title))
	 (onclick (if handler
		      (if (string? handler) handler
			  "fdjtCheckSpan_onclick(event);")))
	 (ischecked (if checked "yes")))
    (span ((class "left")) (or text val))
    (input TYPE "checkbox"
	   NAME (if (symbol? var) var (stringout var))
	   VALUE val
	   ("CHECKED" (if checked "CHECKED")))
    (span ((class "right")) (or text val))))

(module-export! '{checkspan checkspan/radio})


;;;; Text input boxes which contain a prompt when blurred and empty

(define autoprompt
  (macro expr
    `(input TYPE "TEXT" class "autoprompt"
	    ,@(cdr expr)
	    onfocus "return _fdb_autoprompt_focus(event);"
	    onblur "return _fdb_autoprompt_blur(event);"
	    onload "return _fdb_autoprompt_onload(event);")))

(module-export! 'autoprompt)

(define (dummyfn)
  (autoprompt 'name "x" value (stringout (+ 2 3))))


;;;; Font sizers

;;; This provides widgets for increasing and decreasing font size

(define (font-sizers)
  (span ((class "font_sizer")
	 (onclick "_fdb_decrease_font(event);")
	 (title "click to decrease font size"))
    "-")
  (span ((class "font_sizer")
	 (onclick "_fdb_increase_font(event);")
	 (title "click to increase font size"))
    "+"))

(define (fontsizers) (font-sizers))

(module-export! '{font-sizers fontsizers})



;;;; Search bars

;;; This provides XHTML generation functions for 'searchbars' that can
;;;  be used in paging through sets of results.

(define (searchbar baseuri len)
  (let* ((start (->number (req/get 'start 0)))
	 (window (->number (req/get 'window 10)))
	 (end (min (+ start window) len)))
    (div ((class "searchbar"))
      (if (= start 0)
	  (span ((class "searchtick")) "<")
	  (anchor* (stringout baseuri
		     "&START=" (max 0 (- start window))
		     "&WINDOW=" window)
	      ((class "searchtick")) "<"))
      " "
      (dotimes (i (inexact->exact (ceiling (/~ len window))))
	(if (> i 0) (xmlout " . "))
	(anchor* (stringout baseuri
		   "&START=" (* i window)
		   "&WINDOW=" window)
	    ((class "searchtick"))
	  (* i window)))
      " "
      (if (< (+ start window) len)
	  (anchor* (stringout baseuri
		     "&START=" (+ start window)
		     "&WINDOW=" window)
	      ((class "searchtick")) ">")
	  (span ((class "searchtick")) ">")))))

(define (scrolltick baseuri start end len)
  (unless (>= start len)
;     (when (> (+ start (* (- end start) 2)) len)
;       (set! end len))
    (anchor* (scripturl baseuri 'start start 'window (- end start))
	((class "scrolltick"))
      "+" (- end start) ">")))

(define (scrollticks baseuri len . seq)
  (let* ((start (->number (req/get 'start 0)))
	 (window (->number (req/get 'window 10)))
	 (end (min (+ start window) len))
	 (done #f))
    (doseq (elt seq)
      (unless done
	(scrolltick baseuri end (+ end elt) len)
	(when (>= (+ end elt) len) (set! done #t))
	(xmlout " ")))))

(module-export! '{scrollticks searchbar})

;;; Editable

(define (editable var value (helptext #f))
  (div ((class (stringout "display" (downcase var)))
	(id (stringout "DISPLAY" (upcase var)))
	(clicktoshow (stringout "EDIT" (upcase var)))
	(clicktohide (stringout "DISPLAY" (upcase var)))
	(onclick "fdb_showhide_onclick(event);")
	(title "click to edit"))
    (or value ""))
  (if helptext
      (input type "TEXT" name var value (or value "")
	     id (stringout "EDIT" (upcase var)) style "display: none;"
	     onfocus "fdb_showhelp_onfocus(event);"
	     onblur "fdb_hidehelp_onblur(event);"
	     helptext helptext)
      (input type "TEXT" name var value (or value "")
	     id (stringout "EDIT" (upcase var)) style "display: none;")))

(module-export! 'editable)

;;; Versions of %WATCH for XHTML

(define (xhtml%watcher tagname)
  (macro expr
    (let* ((args (cdr expr))
	   (head (car args))
	   (real-args (if (string? head) (cdr args) args)))
      `(let ((_watchstart #f) (_watchval #f)
	     (_tagname ,tagname))
	 (,xmlblock `(,_tagname (class "knowatch"))
	     (if ,(string? head) "[%CALL " "[%WATCH ")
	   (if ,(string? head)
	       (,xmlblock (span (class "head")) ,head)
	       (,xmlblock (span (class "call")) ',head))
	   ,@(map (lambda (arg)
		    `(,xmlblock (span (class "binding"))
			 " " (,xmlblock (span (class "expr")) ',arg) "="
			 (,xmlblock (span (class "value")) ,arg)))
		  real-args)
	   "]")
	 (set! _watchstart (elapsed-time))
	 (set! _watchval ,head)
	 (unless ,(string? head)
	   (,xmlblock `(,_tagname (class "knowatch"))
	       "[%RETURN "
	     "(" (- (elapsed-time) _watchstart) " secs)"
	     (,xmlblock (span (class "call")) ',head)
	     " ==> "
	     (,xmlblock (span (class "result")) _watchval)
	     "]"))
	 _watchval))))

(define span%watch (xhtml%watcher "span"))
(define div%watch (xhtml%watcher "div"))
(define p%watch (xhtml%watcher "p"))

(module-export! '{span%watch div%watch p%watch})
