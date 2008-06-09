;;; -*- Mode: scheme; Text-encoding: utf-8; -*-

(in-module 'xhtml/clickit)

;;; This provides various XHTML widgets which use stylesheets and
;;;  javascript provided by fdweb.css and fdweb.js respectively.

(define version "$Id: mttools.scm 2552 2008-04-24 13:19:29Z haase $")

(use-module 'fdweb)
(use-module 'xhtml)


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
  (if (and selectvar (cgitest selectvar))
      (when (cgitest selectvar content)
	(input id (stringout selectvar "_INPUT") 
	       type "HIDDEN" name (symbol->string selectvar)
	       value content))
      (when defaultselect
	(when selectvar
	  (cgiset! selectvar content)
	  (input id (stringout selectvar "_INPUT") 
		 type "HIDDEN" name (symbol->string selectvar)
		 value content))))
  (span ((class (if (if selectvar (cgitest selectvar content)
			defaultselect)
		    "selected_tab"
		    "tab"))
	 (selectvar (if selectvar (symbol->string selectvar)))
	 (onmouseover "_fdb_tab_mouseover(event);")
	 (onmouseout "_fdb_tab_mouseout(event);")
	 (onclick "return _fdb_tab_click(event);")
	 (title (if title title))
	 (contentid content))
    (if image
	(image SRC image ALT text)
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


;;;; Hot checkboxes

(define (hotcheck/radio var val (text #f) (title #f))
  (span ((class "hotcheck") (title (if title title))
	 (onclick "_fdb_hotcheck_click(event);")
	 (style (if (cgitest var val) "font-weight: bold;")))
    (span ((class "left")) (or text value))
    (input TYPE "radio" NAME (symbol->string var) VALUE val
	   ("CHECKED" (cgitest var val)))
    (span ((class "right")) (or text value))))

(define (hotcheck var val (text #f) (title #f))
  (span ((class "hotcheck") (title (if title title))
	 (onclick "_fdb_hotcheck_click(event);")
	 (style (if (cgitest var val) "font-weight: bold;")))
    (span ((class "left")) (or text value))
    (input TYPE "checkbox" NAME (symbol->string var) VALUE val
	   ("CHECKED" (cgitest var val)))
    (span ((class "right")) (or text value))))

(module-export! '{hotcheck hotcheck/radio})


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
  (let* ((start (->number (cgiget 'start 0)))
	 (window (->number (cgiget 'window 10)))
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
    (when (> (+ start (* (- end start) 2)) len)
      (set! end len))
    (anchor* (scripturl+ baseuri 'start start 'window (- end start))
	((class "scrolltick"))
      "+" (- end start) ">")))

(define (scrollticks baseuri len . seq)
  (let* ((start (->number (cgiget 'start 0)))
	 (window (->number (cgiget 'window 10)))
	 (end (min (+ start window) len))
	 (done #f))
    (doseq (elt seq)
      (unless done
	(scrolltick baseuri end (+ end elt) len)
	(when (> (+ end elt) len) (set! done #t))
	(xmlout " ")))))

(module-export! '{scrollticks searchbar})

