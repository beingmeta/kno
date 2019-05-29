(in-module 'gnosys/metakeys/translate)

;;; Copyright (C) 2005 beingmeta, inc.
;;; This program code is proprietary to and a value trade secret of 
;;;   beingmeta, inc.  Disclosure or distribution without explicit
;;;   approval is forbidden.

(use-module 'brico)
(use-module 'gnosys)
(use-module 'metakeys)

(module-export! '{change-language concept->keyentry})

(define (change-language keylist language)
  (let ((new-entries (translate-keyentries (keylist-entries keylist) language)))
    (vector (stringout (doseq (entry new-entries i)
			 (if (> i 0) (printout " "))
			 (if (position #\Space (keyentry-term entry))
			     (printout "\"" (keyentry-term entry) "\"")
			     (printout (keyentry-term entry)))))
	    language
	    new-entries)))

(define (concept->keyentry concept language)
  (let ((dterm (get-dterm concept language)))
    (vector dterm (dterm-base dterm)
	    (qc (parse-dterm (dterm-base dterm) language)) concept)))

(define (translate-keyentries entries language)
  (if (null? entries) '()
      (let ((actual (keyentry-actual (car entries)))
	    (result (translate-keyentries (cdr entries) language)))
	(do-choices (new (concept->keyentry actual language))
	  (set! result (cons new result)))
	result)))
