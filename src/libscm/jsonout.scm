;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc. All rights reserved

;;; DON'T EDIT THIS FILE !!!
;;;
;;; The reference version of this module now in the src/libscm
;;; directory of the FramerD/KNO source tree. Please edit the file in
;;; src/libscm instead or move any added functionality to an extension
;;; module.

(in-module 'jsonout)

(use-module '{fdweb varconfig})

(define json-lisp-prefix ":")

(define-init *wrapvecs* #t)
(varconfig! JSON:WRAPVECS *wrapvecs*)

;; Whether to output all slots/keys as :SLOTNAME
(define-init *ugly-slots* #f)
(varconfig! JSON:UGLYSLOTS *ugly-slots*)

;; A function for converting slots to keystrings
(define-init *json-slotkeyfn* #f)
(varconfig! JSON:SLOTKEYFN *json-slotkeyfn*)

;; A slotid/key storing an ID for rendering tables 
(define-init *json-refslot* #f)
(varconfig! JSON:REFSLOT *json-refslot*)

;; A slotid or function for generating references from OIDs
(define-init *json-oidref* #f)
(varconfig! JSON:OIDREF *json-oidref*)

;; A slotid or function for generating references from UUIDS
(define *json-uuidfn* #f)
(varconfig! JSON:UUIDFN *json-uuidfn*)

(define *render-timestamp*
  (lambda (t) (printout (get t 'tick))))
(varconfig! JSON:TIMESTAMPFN *render-timestamp*)

(define %volatile '{*json-refslot* *json-oidref* *json-uuidfn*})

(defambda (jsonelt value (prefix #f) (initial #f))
  (printout (if (not initial) ",") (if prefix prefix)
    (unless (singleton? value) (printout "["))
    (if (singleton? value)
	(jsonout value #f)
	(do-choices (value value i)
	  (when (> i 0) (printout ","))
	  (jsonout value #f)))
    (unless (singleton? value) (printout "]"))))
(define (jsonvec vector)
  (printout "[" (doseq (elt vector i)
		  (jsonelt elt #f (= i 0)))
    "]"))

(defambda (jsonfield field value (valuefn #f) (prefix #f) (context #f)
		     (vecval #f))
  (printout
    (if prefix prefix)
    (if (symbol? field)
	(write (downcase (symbol->string field)))
	(if (string? field) (write field)
	    (write (unparse-arg field))))
    ": "
    (if (or vecval (not (singleton? value))) (printout "["))
    (do-choices (value value i)
      (when (> i 0) (printout ","))
      (if valuefn
	  (jsonout (valuefn value context) #f)
	  (jsonout value #f)))
    (if (or vecval (not (singleton? value))) (printout "]"))))
(defambda (jsonfield+ field value (valuefn #f) (prefix #f) (context #f)
		      (vecval #f))
  (printout
    (if prefix prefix)
    (if (symbol? field)
	(write (downcase (symbol->string field)))
	(if (string? field) (write field)
	    (write (unparse-arg field))))
    ": ["
    (do-choices (value value i)
      (when (> i 0) (printout ","))
      (if valuefn
	  (jsonout (valuefn value context) #f)
	  (jsonout value #f)))
    "]"))

(define (jsontable table (valuefn #f) (context #f))
  (printout "{"
	    (let ((initial #t))
	      (do-choices (key (getkeys table) i)
		(let ((v (get table key)))
		  (unless initial (printout ", "))
		  (jsonfield key (qc v) valuefn "" context)
		  (set! initial #f))))
	    
	    "}"))

(defambda (jsonout value (opts #f) (emptyval))
  (when (string? opts)
    (set! emptyval opts)
    (set! opts #f))
  (default! emptyval "[]")
  (cond ((ambiguous? value)
	 (printout "[" (do-choices (v value i)
			 (printout (if (> i 0) ",") (jsonout v)))
	   "]"))
	((fail? value) (if emptyval (printout emptyval)))
	((or (oid? value) (uuid? value) (timestamp? value))
	 (jsonoutput (exportjson value opts) 0))
	((string? value) (jsonoutput value 0))
	((or (vector? value) (compound? value 'jsonvec)) (jsonvec value))
	((eq? value #t) (printout "true"))
	((eq? value #f) (printout "false"))
	((number? value) (printout value))
	((table? value) (jsontable value))
	(else (let ((string (stringout (printout json-lisp-prefix)
			      (write value))))
		(jsonoutput string 0)))))

(module-export! '{jsonout jsonvec jsontable jsonfield jsonfield+ jsonelt})

;;; Support for JSON responses

(define (jsonp/open (var #f) (assign #f) (callback #f))
  (if var (printout "var " var "=")
      (if assign
	  (printout assign "=")
	  (if callback (printout callback "(")))))
(define (jsonp/close (var #f) (assign #f) (callback #f))
  (if (or var assign callback)
      (printout (if callback ")") ";")))

(module-export! '{jsonp/open jsonp/close})

;;; Converting a FramerD/Scheme object into an object that converts to
;;; JSON better
(defambda (exportjson object (opts #f) (toplevel #f) 
		      (oidfn) (uuidfn) (slotkeyfn) (refslot))
  (default! oidfn
    (if opts (getopt opts 'oidfn *json-oidref*) *json-oidref*))
  (default! uuidfn
    (if opts (getopt opts 'uuidfn *json-uuidfn*) *json-uuidfn*))
  (default! slotkeyfn
    (if opts (getopt opts 'slotkeyfn *json-slotkeyfn*) *json-slotkeyfn*))
  (default! refslot
    (if opts (getopt opts 'refslot *json-refslot*) *json-refslot*))
  (cond ((fixnum? (qc object)) object)
	((symbol? (qc object)) object)
	((oid? (qc object))
	 (try (tryif oidfn
		(tryif (or (symbol? oidfn) (oid? oidfn)) (get object oidfn))
		(oidfn object opts toplevel))
	      (tryif *json-refslot* (get object *json-refslot*))
	      (glom json-lisp-prefix (oid->string object))))
	((ambiguous? object)
	 (choice->vector 
	  (for-choices (object object) 
	    (exportjson object opts #f oidfn uuidfn slotkeyfn refslot))))
	((timestamp? object) 
	 ((getopt opts 'timestampfn *render-timestamp*) object))
	((pair? object)
	 (if (proper-list? object)
	     (if (getopt opts 'wrapvecs *wrapvecs*)
		 (vector (forseq (elt (->vector object))
			   (exportjson elt opts #f oidfn uuidfn slotkeyfn refslot)))
		 (forseq (elt (->vector object)) 
		   (exportjson elt opts #f oidfn uuidfn slotkeyfn refslot)))
	     `#[CAR ,(exportjson (car object) opts #f oidfn uuidfn slotkeyfn refslot)
		CDR ,(exportjson (car object) opts #f oidfn uuidfn slotkeyfn refslot)]))
	((string? object)
	 (if (has-prefix object {"#" "\\"}) (glom "\\" object)
	     object))
	((packet? object) (glom "#x\"" (packet->base16 object) "\""))
	((uuid? object)
	 (try (tryif uuidfn (uuidfn object opts toplevel))
	      (glom "#U" (uuid->string object))))
	((vector? object)
	 (if (getopt opts 'wrapvecs *wrapvecs*)
	     (vector (forseq (elt (->vector object))
		       (exportjson elt opts #f oidfn slotkeyfn refslot)))
	     (forseq (elt (->vector object))
	       (exportjson elt opts #f oidfn slotkeyfn refslot))))
	((and refslot (table? object) (test object refslot))
	 (get object refslot))
	((table? object)
	 (let ((obj (frame-create #f)))
	   (do-choices (key (getkeys object))
	     (store! obj
	       (cond ((and (oid? key) oidfn)
		      (try (tryif (oid? oidfn) (get key oidfn))
			   (tryif (symbol? oidfn) (get key oidfn))
			   (oidfn key)
			   (oid->string key)))
		     ((oid? key) (oid->string key))
		     ((and (symbol? key) slotkeyfn) (slotkeyfn key))
		     ((and (symbol? key) *ugly-slots*)
		      (glom json-lisp-prefix (symbol->string key)))
		     ((and (symbol? key) toplevel)
		      (downcase (symbol->string key)))
		     ((string? key) key)
		     (else (let ((exported (exportjson key opts #f oidfn slotkeyfn refslot)))
			     (if (string? exported) exported
				 (lisp->string exported)))))
	       (for-choices (v (get object key))
		 (exportjson v opts #f oidfn slotkeyfn refslot))))
	   obj))
	 (else object)))
(define (export->json arg (opts #f)) (exportjson arg opts #t))

(defambda (importjson object (opts #f) (toplevel #f))
  (if (vector? object)
      (for-choices (elt (elts object))
	(if (vector? elt) 
	    (forseq (subelt elt) (importjson subelt opts))
	    (importjson elt opts)))
      (if (string? object)
	  (cond ((has-prefix object "\\") (slice object 1))
		((has-prefix object "#x") (base16->packet (slice object 2)))
		((has-prefix object "#T") (timestamp (slice object 2)))
		((has-prefix object "#U") (getuuid (slice object 2)))
		((has-prefix object "#@") (string->lisp (slice object 1)))
		(else object))
	  (if (oid? object) object
	      (if (table? object)
		  (let ((obj (frame-create #f)))
		    (do-choices (key (getkeys object))
		      (if (and toplevel (string? key) (lowercase? key))
			  (add! obj (string->lisp key)
				(importjson (get object key) opts))
			  (add! obj key (importjson (get object key) opts))))
		    obj)	      
		  object)))))
(define (import->json obj (opts #f)) (importjson obj opts #t))
(module-export! '{export->json import->json})

;;; JSON stringout

(define (json->string x (opts #f))
  (stringout (jsonout (exportjson x opts #t))))
(define (jsonstringout x (opts #f)) (json->string x opts))
(define (json/stringout x (opts #f)) (json->string x opts))

(module-export! '{json->string jsonstringout json/stringout})


