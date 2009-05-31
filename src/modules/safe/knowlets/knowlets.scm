;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2009 beingmeta, inc.  All rights reserved.

(in-module 'knowlets)

;;; Core file for the knowlets implementation
;;; Provides data structures, core tables, and basic
;;;  KR functions
(define id "$Id$")
(define revision "$Revision$")

(use-module '{texttools ezrecords varconfig logger})
(use-module 'knowlets/drules)

(module-export!
 '{knowlet
   kno/dterm kno/dref kno/ref kno/probe knowlet?
   kno/add! kno/drop! kno/replace! kno/find
   kno/phrasemap
   kno/slotid kno/slotids kno/slotnames kno/relcodes
   knowlet-name knowlet-opts knowlet-language
   knowlet-oid knowlet-pool knowlet-index
   knowlet-alldterms knowlet-dterms knowlet-drules
   default-knowlet knowlets
   knowlet:pool knowlet:index knowlet:indices
   knowlet! ->knowlet iadd!
   kno/logging
   langids})

(define %loglevel %warn!)

(define kno/logging {})

;;;; Global tables, variables, and structures (with their configs)

(define-init knowlets (make-hashtable))

(define knowlet:pool (make-mempool "knowlets" @2009/0 (* 4 1024 1024)))
(define (knowlet-pool-config var (val))
  (if (bound? val)
      (set! knowlet:pool
	    (cond ((string? val) (use-pool val))
		  ((pool? val) val)
		  ((oid? val)
		   (try (getpool val)
			(make-mempool "knowlets" val (* 1024 1024))))
		  (else (error "Not a valid knowlet pool"))))
      knowlet:pool))
(config-def! 'KNO:POOL knowlet-pool-config)

(define knowlet:index (make-hashtable))
(define (knowlet-index-config var (val))
  (if (bound? val)
      (begin
	(set! knowlet:index
	      (cond ((string? val) (open-index val))
		    ((index? val) val)
		    (else (make-hashtable))))
	(set+! knowlet:indices knowlet:index))
      knowlet:index))
(config-def! 'KNO:INDEX knowlet-index-config)

(define knowlet:indices knowlet:index)
(varconfig! KNO:INDICES knowlet:indices open-index choice)

(define knowlet:language 'en)
(varconfig! KNO:LANG knowlet:language)

;;; Various useful global tables

(define langids (file->dtype (get-component "langids.dtype")))
(define langnames (file->dtype (get-component "langnames.table")))
(define kno/slotids {})
(define kno/slotnames (make-hashtable))
(define kno/relcodes (make-hashtable))
(define slotids-finished #f)

(define knowlet-slots-init
  '((genls genl broader class category kindof isa ^)
    (specls specl narrower cases instances examples _)
    (commonly usually typically ^*)
    (sometimes possibly ^~)
    (never not disjoint disjoin -)
    (rarely unusually atypically -*)
    (somenot maybenot notrequired mightnotbe -~)
    (mirror inverse)
    (hooks hook ~)))
(define knowlet-slots-initialized #f)

(unless knowlet-slots-initialized
  ;; Initialize knowlet-slots
  (dolist (slot-init knowlet-slots-init)
    (set+! kno/slotids (car slot-init))
    (add! kno/slotnames (elts slot-init) (car slot-init))
    (let ((relcodes (pick (symbol->string (elts slot-init))
			  string-starts-with? '(ispunct))))
      (add! kno/relcodes (car slot-init) relcodes)))
  (do-choices (name (getkeys langnames))
    (set+! kno/slotids (get langnames name))
    (add! kno/slotnames name (get langnames name)))
  (set! knowlet-slots-initialized #t))

(define kno/slotid
  (macro expr
    `(if (overlaps? ,(get-arg expr 1) kno/slotids) ,(get-arg expr 1)
	 (tryget kno/slotnames ,(get-arg expr 1)))))

;;; The KNOWLET structure itself

(defrecord knowlet name
  (oid #f)
  (language 'en)
  (opts (make-hashset))
  ;; Where dterms within this knowlet are allocated
  (pool knowlet:pool)
  ;; Maps string dterms to OID dterms
  (dterms (make-hashtable))
  ;; Inverted index for dterms in the knowlet
  (index (make-hashtable))
  ;; GENLS index (index of the transitive closure of GENLS)
  ;;  (useful in inference and searching)
  (genls* (make-hashtable))
  ;; Phrasemaps to help in pulling phrases out of documents
  ;;  This is a hashtable mapping (LANGID . word) to a phrase vector
  (phrasemaps (make-hashtable))
  ;; All dterms in this knowlet (a hashset)
  (alldterms (make-hashset))
  ;; Rules for disambiguating words into dterms
  (drules (make-hashtable)))

;;; Creating and referencing knowlets

(defslambda (new-knowlet name pool opts)
  (let* ((pool (use-pool pool))
	 (oid (frame-create pool 'knoname name '%id name))
	 (language (try (intersection opts langids) knowlet:language))
	 (new (cons-knowlet name oid language (choice->hashset opts))))
    (index-frame knowlet:index oid 'knoname name)
    (store! knowlets (choice oid name) new)
    new))

(define dont-index '{%id})

(defslambda (restore-knowlet oid)
  (let* ((name (get oid 'knoname))
	 (language (get oid 'language))
	 (opts (get oid 'opts))
	 (new (cons-knowlet name oid language (choice->hashset opts)
			    (try (getpool oid) knowlet:pool)))
	 (index (knowlet-index new))
	 (genls*index (knowlet-genls* new))
	 (drules (knowlet-drules new))
	 (kdterms (knowlet-dterms new)))
    (store! knowlets (choice oid name) new)
    (let ((dterms (find-frames knowlet:indices 'knowlet oid)))
      (prefetch-oids! dterms)
      (hashset-add! (knowlet-alldterms new) dterms)
      (do-choices (dterm dterms)
	(index-frame (knowlet-index new) dterm
	  (difference (getkeys dterm) dont-index))
	(add! kdterms (get dterm '{dterms dterm}) dterm)
	(add! genls*index (get* dterm 'genls) dterm)
	(do-choices (drule (get dterm 'drules))
	  (add! drules (drule-cues drule) drule))))
    new))

(define (knowlet name (pool knowlet:pool) (opts #{}))
  (try (tryif (knowlet? name) name)
       (get knowlets name)
       (let ((existing (find-frames knowlet:indices 'knoname name)))
	 (if (exists? existing)
	     (restore-knowlet existing)
	     (new-knowlet name pool (qc opts))))))

(define (knowlet! . args)
  (let ((kno (apply knowlet args)))
    (set! default-knowlet kno)
    kno))

(define (->knowlet object)
  (try (tryif (knowlet? object) object)
       (tryif (and (oid? object) (test object 'knowlet))
	 (get knowlets (get object 'knowlet)))
       (get knowlets object)
       (restore-knowlet
	(find-frames knowlet:indices 'knoname object))))

;;; Creating and referencing dterms

(define default-knowlet #f)
(varconfig! knowlet default-knowlet knowlet)

(define (kno/dterm term (knowlet default-knowlet))
  (try (get (knowlet-dterms knowlet) term)
       (new-dterm term knowlet)))
(define (new-dterm term knowlet)
  (let ((f (frame-create knowlet:pool
	     'knowlet (knowlet-oid knowlet)
	     'dterm term 'dterms term
	     (knowlet-language knowlet) term
	     '%id term)))
    (store! (knowlet-dterms knowlet) term f)
    (hashset-add! (knowlet-alldterms knowlet) f)
    (index-frame knowlet:index f '{dterm dterms knowlet})
    (index-frame (knowlet-index knowlet) f '{dterm dterms})
    (index-frame (knowlet-index knowlet)
	f (knowlet-language knowlet))
    f))

(define (kno/dref term (knowlet default-knowlet) (create #t))
  (try (get (knowlet-dterms knowlet) term)
       (tryif create (kno/dterm term knowlet))))

(define (kno/probe term (knowlet default-knowlet))
  (get (knowlet-dterms knowlet) term))

(define (kno/ref term (knowlet default-knowlet) (lang) (tryhard #f))
  (default! lang (knowlet-language knowlet))
  (try (find-frames (knowlet-index knowlet) lang term)
       (tryif tryhard
	      (find-frames (knowlet-index knowlet)
		lang (choice (metaphone term #t)
			     (string->packet (disemvowel term)))))))

;;; String indexing

(define (dedash string)
  (tryif (position #\- string)
	 (choice (string-subst string "-" " " )
		 (string-subst string "-" ""))))

(defambda (kno/string-indices value (phonetic #f))
  (let* ((values (stdspace value))
	 (expvalues (choice values (basestring values)))
	 (normvalues (capitalize (pick expvalues somecap?)))
	 (indexvals (choice expvalues normvalues (dedash normvalues)))
	 (metavals (tryif phonetic
		     (metaphone (pick (choice values normvalues) length > 2)
				#t))))
    (choice indexvals metavals)))

(define (kno/index-string f slotid (value) (knowlet) (index))
  (default! value (get f slotid))
  (default! knowlet (get knowlets (get f 'knowlet)))
  (default! index (knowlet-index knowlet))
  (add! index (cons slotid (kno/string-indices value)) f))

;;; Find and edit operations on dterms

(defambda (kno/find . args)
  (let* ((n-args (length args))
	 (knowlet (if (even? n-args) default-knowlet (car args)))
	 (args (if (even? n-args) args (cdr args))))
    (if (< n-args 4)
	(find-frames (knowlet-index knowlet)
	  (kno/slotid (car args)) (cadr args))
	(if (< n-args 5)
	    (find-frames (knowlet-index knowlet)
	      (kno/slotid (car args)) (cadr args)
	      (kno/slotid (third args)) (fourth args))
	    (apply find-frames (knowlet-index knowlet)
		   (do ((query
			 '() (cons (cadr args)
				   (cons (kno/slotid (car args)) query))))
		       ((null? args) (reverse query))))))))

(define infer-onadd (make-hashtable))
(define infer-ondrop (make-hashtable))

(defambda (kno/add! dterm slotid value)
  (detail%watch "KNO/ADD!" dterm slotid value)
  (let* ((slotid (kno/slotid slotid))
	 (cur (get dterm slotid))
	 (new (difference value cur))
	 (knowlet (get knowlets (get dterm 'knowlet))))
    (when (exists? new)
      (add! dterm slotid new)
      (index-frame (knowlet-index knowlet) dterm slotid new)
      (unless (exists? cur)
	(index-frame (knowlet-index knowlet) dterm 'has slotid))
      ((get infer-onadd slotid) dterm slotid new)
      (detail%watch "KNO/ADD!" dterm slotid new))))

(defambda (kno/drop! dterm slotid value)
  (let* ((slotid (kno/slotid slotid))
	 (drop (intersection value (get dterm slotid)))
	 (knowlet (get knowlets (get dterm 'knowlet))))
    (when (exists? drop)
      (drop! dterm slotid drop)
      (drop! (knowlet-index knowlet) (cons slotid drop) dterm)
      (if (fail? (get dterm slotid))
	  (drop! (knowlet-index knowlet) (cons 'has slotid) dterm))
      ((get infer-ondrop slotid) dterm slotid drop))))

(defambda (kno/replace! dterm slotid value (toreplace {}))
  (for-choices dterm
    (for-choices (slotid (kno/slotid slotid))
      (let ((replace (difference (try toreplace (get dterm slotid))
				 value)))
	(let ((new (difference value (get dterm slotid)))
	      (todrop replace)
	      (knowlet (get knowlets (get dterm 'knowlet))))
	  (when (exists? todrop)
	    (drop! dterm slotid todrop)
	    ((get infer-ondrop slotid) dterm slotid todrop)
	    (drop! (knowlet-index knowlet) (cons slotid todrop)
		   todrop))
	  (when (exists? new)
	    (add! dterm slotid new)
	    (index-frame (knowlet-index knowlet) dterm slotid new)
	    ((get infer-onadd slotid) dterm slotid new)))))))

;;; Special inference methods

(define (add-genl! f s g)
  (let ((g* (get* g 'genls))
	(g*cur (get f 'genls*))
	(knowlet (get knowlets (get f 'knowlet))))
    (add! f 'genls* g)
    (add! f 'genls* (difference g* g*cur))
    (add! (knowlet-genls* knowlet) (difference g* g*cur) f)))

(define (drop-genl! f s g)
  ;; This is called after the drop happens, so g*cur actually reflects
  ;;  the update.
  (let* ((g* (get* g 'genls))
	 (g*cur (get f 'genls*))
	 (knowlet (get knowlets (get f 'knowlet)))
	 (g*drop (difference g* g*cur))
	 (g*index (knowlet-genls* knowlet)))
    (drop! f 'genls* g)
    (drop! f 'genls* g*drop)
    (drop! g*index g*drop f)
    (do-choices (specl (get g*index f))
      (let* ((g*cur (get* specl 'genls))
	     (g*drop (difference g*drop g*cur)))
	(when (exists? g*drop)
	  (drop! specl 'genls* g*drop)
	  (drop! g*index g*drop specl))))))

(add! infer-onadd 'genls add-genl!)
(add! infer-ondrop 'genls drop-genl!)

;;; Specls (just the inverse)

(define (add-specl! f s v)
  (add! v 'genls f)
  (add-genl! v 'genls f))

(define (drop-specl! f s g)
  (drop! g 'genls f)
  (drop-genl! g 'genls f))

(add! infer-onadd 'specls add-specl!)
(add! infer-ondrop 'specls drop-specl!)

;;; Symmetric

(define (add-symmetric! frame slotid value (mirror))
  (default! mirror slotid)
  (unless (test value mirror frame)
    (kno/add! value mirror frame)))

(define (drop-symmetric! frame slotid value (mirror))
  (default! mirror slotid)
  (when (test value mirror frame)
    (kno/drop! value mirror frame)))

(add! infer-onadd '{mirror equivalent identical} add-symmetric!)
(add! infer-ondrop '{mirror equivalent identical} drop-symmetric!)

;;; Natural language terms

(defambda (add-phrase! frame slotid value)
  (let ((knowlet (get knowlets (get frame 'knowlet))))
    ;; Index expanded vales (including metaphone hashes)
    (add! (knowlet-index knowlet)
	  (cons slotid (kno/string-indices value))
	  frame)
    ;; Update the phrasemap
    (when (compound? value)
      (let ((wordv (words->vector value))
	    (phrasemap (try (get (knowlet-phrasemaps knowlet) slotid)
			    (new-phrasemap knowlet slotid))))
	(add! phrasemap (elts wordv) wordv)
	(add! phrasemap (list (first wordv)) wordv)))))

(defambda (drop-phrase! frame slotid value (mirror))
  (let ((knowlet (get knowlets (get frame 'knowlet)))
	(excur (kno/string-indices (get frame slotid)))
	(exdrop (kno/string-indices value))
	(index (knowlet-index knowlet)))
    ;; Update the index, noting that some expanded values
    ;;  may still apply after the drop
    ;; Note that we won't bother updating the phrasemap because
    ;;  it should only be used heuristically
    (drop! (knowlet-index knowlet)
	   (cons slotid (difference exdrop excur))
	   frame)))

(store! infer-onadd langids add-phrase!)
(store! infer-ondrop langids drop-phrase!)

(defslambda (new-phrasemap knowlet langid)
  (let ((phrasemap (get (knowlet-phrasemaps knowlet) langid)))
    (when (fail? phrasemap)
      (set! phrasemap (make-hashtable))
      (store! (knowlet-phrasemaps knowlet) langid phrasemap))
    phrasemap))

(define (kno/phrasemap knowlet (langid))
  (default! langid (knowlet-language knowlet))
  (try (get (knowlet-phrasemaps knowlet) langid)
       (new-phrasemap knowlet langid)))

;;; DRULES

(define (add-drule! frame slotid value)
  (add! (knowlet-drules (get knowlets (get frame 'knowlet)))
	(drule-cues value)
	value))
(define (drop-drule! frame slotid value)
  (drop! (knowlet-drules (get knowlets (get frame 'knowlet)))
	 (drule-cues value)
	 value))
(store! infer-onadd 'drules add-drule!)
(store! infer-ondrop 'drules drop-drule!)

;;; IADD!

(define (get-index f)
  (try (knowlet-index (get knowlets (get f 'knowlet)))
       knowlet:index))

(defambda (iadd! f slotid value (index))
  (if (bound? index)
      (if (singleton? slotid)
	  (begin (add! index (cons 'has slotid) (reject f slotid))
		 (add! f slotid value)
		 (add! index (cons slotid value) f))
	  (do-choices slotid
	    (add! index (cons 'has slotid) (reject f slotid))
	    (add! f slotid value)
	    (add! index (cons slotid value) f)))
      (let ((kindex (get-index f)))
	(if (singleton? kindex)
	    (do-choices slotid
	      (add! kindex (cons 'has slotid) (reject f slotid))
	      (add! f slotid value)
	      (add! kindex (cons slotid value) f))
	    (do-choices f
	      (let ((kindex (get-index f)))
		(do-choices slotid
		  (add! kindex (cons 'has slotid) (reject f slotid))
		  (add! f slotid value)
		  (add! index (cons slotid value) f))))))))




