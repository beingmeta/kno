;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2009 beingmeta, inc.  All rights reserved.

(in-module 'knodules)

;;; Core file for the knodules implementation
;;; Provides data structures, core tables, and basic
;;;  KR functions
(define id "$Id$")
(define revision "$Revision: 4048 $")

(use-module '{texttools ezrecords varconfig logger})
(use-module 'knodules/drules)

(module-export!
 '{knodule
   kno/dterm kno/dref kno/ref kno/probe knodule?
   kno/add! kno/drop! kno/replace! kno/find
   kno/phrasemap
   kno/slotid kno/slotids kno/slotnames kno/relcodes
   knodule-name knodule-opts knodule-language
   knodule-oid knodule-pool knodule-index
   knodule-alldterms knodule-dterms knodule-drules
   knodule-prime
   default-knodule knodules kno/set-dterm!
   knodule:pool knodule:index knodule:indices
   knodule! ->knodule iadd!
   kno/logging
   langids})

(define %loglevel %warn!)

(define kno/logging {})

;;;; Global tables, variables, and structures (with their configs)

(define-init knodules (make-hashtable))

(define knodule:pool (make-mempool "knodules" @2009/0 (* 4 1024 1024)))
(define (knodule-pool-config var (val))
  (if (bound? val)
      (set! knodule:pool
	    (cond ((string? val) (use-pool val))
		  ((pool? val) val)
		  ((oid? val)
		   (try (getpool val)
			(make-mempool "knodules" val (* 1024 1024))))
		  (else (error "Not a valid knodule pool"))))
      knodule:pool))
(config-def! 'KNO:POOL knodule-pool-config)

(define knodule:index (make-hashtable))
(define (knodule-index-config var (val))
  (if (bound? val)
      (begin
	(set! knodule:index
	      (cond ((string? val) (open-index val))
		    ((index? val) val)
		    (else (make-hashtable))))
	(set+! knodule:indices knodule:index))
      knodule:index))
(config-def! 'KNO:INDEX knodule-index-config)

(define knodule:indices knodule:index)
(varconfig! KNO:INDICES knodule:indices open-index choice)

(define knodule:language 'en)
(varconfig! KNO:LANG knodule:language)

;;; Various useful global tables

(define langids (file->dtype (get-component "langids.dtype")))
(define langnames (file->dtype (get-component "langnames.table")))
(define kno/slotids {})
(define kno/slotnames (make-hashtable))
(define kno/relcodes (make-hashtable))
(define slotids-finished #f)

(define knodule-slots-init
  '((genls genl broader class category kindof isa ^)
    (specls specl narrower cases instances examples _)
    (commonly usually typically ^*)
    (sometimes possibly ^~)
    (never not disjoint disjoin -)
    (rarely unusually atypically -*)
    (somenot maybenot notrequired mightnotbe -~)
    (mirror inverse)
    (hooks hook ~)))
(define knodule-slots-initialized #f)

(unless knodule-slots-initialized
  ;; Initialize knodule-slots
  (dolist (slot-init knodule-slots-init)
    (set+! kno/slotids (car slot-init))
    (add! kno/slotnames (elts slot-init) (car slot-init))
    (let ((relcodes (pick (symbol->string (elts slot-init))
			  string-starts-with? '(ispunct))))
      (add! kno/relcodes (car slot-init) relcodes)))
  (do-choices (name (getkeys langnames))
    (set+! kno/slotids (get langnames name))
    (add! kno/slotnames name (get langnames name)))
  (set! knodule-slots-initialized #t))

(define kno/slotid
  (macro expr
    `(if (overlaps? ,(get-arg expr 1) kno/slotids) ,(get-arg expr 1)
	 (tryget kno/slotnames ,(get-arg expr 1)))))

;;; The KNODULE structure itself

(defrecord knodule name
  (oid #f)
  (language 'en)
  (opts (make-hashset))
  ;; Where dterms within this knodule are allocated
  (pool knodule:pool)
  ;; Maps string dterms to OID dterms
  (dterms (make-hashtable))
  ;; Inverted index for dterms in the knodule
  (index (make-hashtable))
  ;; GENLS index (index of the transitive closure of GENLS)
  ;;  (useful in inference and searching)
  (genls* (make-hashtable))
  ;; Phrasemaps to help in pulling phrases out of documents
  ;;  This is a hashtable mapping (LANGID . word) to a phrase vector
  (phrasemaps (make-hashtable))
  ;; All dterms in this knodule (a hashset)
  (alldterms (make-hashset))
  ;; Rules for disambiguating words into dterms
  (drules (make-hashtable))
  ;; 'Prime' dterms are important for this knodule
  (prime (make-hashset)))

;;; Creating and referencing knodules

(defslambda (new-knodule name pool opts)
  (let* ((pool (use-pool pool))
	 (oid (frame-create pool 'knoname name '%id name))
	 (language (try (intersection opts langids) knodule:language))
	 (new (cons-knodule name oid language (choice->hashset opts))))
    (index-frame knodule:index oid 'knoname name)
    (store! knodules (choice oid name) new)
    new))

(define dont-index '{%id})

(defslambda (restore-knodule oid)
  (let* ((name (get oid 'knoname))
	 (language (get oid 'language))
	 (opts (get oid 'opts))
	 (new (cons-knodule name oid language (choice->hashset opts)
			    (try (getpool oid) knodule:pool)))
	 (index (knodule-index new))
	 (genls*index (knodule-genls* new))
	 (drules (knodule-drules new))
	 (kdterms (knodule-dterms new)))
    (store! knodules (choice oid name) new)
    (let ((dterms (find-frames knodule:indices 'knodule oid)))
      (prefetch-oids! dterms)
      (hashset-add! (knodule-alldterms new) dterms)
      (do-choices (dterm dterms)
	(index-frame (knodule-index new) dterm
	  (difference (getkeys dterm) dont-index))
	(add! kdterms (get dterm '{dterms dterm}) dterm)
	(add! genls*index (get* dterm 'genls) dterm)
	(do-choices (drule (get dterm 'drules))
	  (add! drules (drule-cues drule) drule))))
    new))

(define (knodule name (pool knodule:pool) (opts #{}))
  (try (tryif (knodule? name) name)
       (get knodules name)
       (let ((existing (find-frames knodule:indices 'knoname name)))
	 (if (exists? existing)
	     (restore-knodule existing)
	     (new-knodule name pool (qc opts))))))

(define (knodule! . args)
  (let ((kno (apply knodule args)))
    (set! default-knodule kno)
    kno))

(define (->knodule object)
  (try (tryif (knodule? object) object)
       (tryif (and (oid? object) (test object 'knodule))
	 (get knodules (get object 'knodule)))
       (get knodules object)
       (restore-knodule
	(find-frames knodule:indices 'knoname object))))

;;; Creating and referencing dterms

(define default-knodule #f)
(varconfig! knodule default-knodule knodule)

(define (kno/dterm term (knodule default-knodule))
  (try (get (knodule-dterms knodule) term)
       (new-dterm term knodule)))
(define (new-dterm term knodule)
  (let ((f (frame-create knodule:pool
	     'knodule (knodule-oid knodule)
	     'dterm term 'dterms term
	     (knodule-language knodule) term
	     '%id term)))
    (store! (knodule-dterms knodule) term f)
    (hashset-add! (knodule-alldterms knodule) f)
    (index-frame knodule:index f '{dterm dterms knodule})
    (index-frame (knodule-index knodule) f '{dterm dterms})
    (index-frame (knodule-index knodule)
	f (knodule-language knodule))
    f))

(define (kno/dref term (knodule default-knodule) (create #t))
  (try (get (knodule-dterms knodule) term)
       (tryif create (kno/dterm term knodule))))

(define (kno/probe term (knodule default-knodule))
  (get (knodule-dterms knodule) term))

(define (kno/ref term (knodule default-knodule) (lang) (tryhard #f))
  (default! lang (knodule-language knodule))
  (try (find-frames (knodule-index knodule) lang term)
       (tryif tryhard
	      (find-frames (knodule-index knodule)
		lang (choice (metaphone term #t)
			     (string->packet (disemvowel term)))))))

(define (kno/set-dterm! dtf dterm (keepold #t))
  (let ((knodule (->knodule (get dtf 'knodule))))
    (if keepold
	(add! dtf 'dterms (get dtf 'dterm))
	(drop! (knodule-dterms knodule) (get dtf 'dterm) dtf))
    (store! dtf 'dterm dterm)
    (add! dtf 'dterms dterm)
    (add! (knodule-dterms knodule) dterm dtf)))

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

(define (kno/index-string f slotid (value) (knodule) (index))
  (default! value (get f slotid))
  (default! knodule (get knodules (get f 'knodule)))
  (default! index (knodule-index knodule))
  (add! index (cons slotid (kno/string-indices value)) f))

;;; Find and edit operations on dterms

(defambda (kno/find . args)
  (let* ((n-args (length args))
	 (knodule (if (even? n-args) default-knodule (car args)))
	 (args (if (even? n-args) args (cdr args))))
    (if (< n-args 4)
	(find-frames (knodule-index knodule)
	  (kno/slotid (car args)) (cadr args))
	(if (< n-args 5)
	    (find-frames (knodule-index knodule)
	      (kno/slotid (car args)) (cadr args)
	      (kno/slotid (third args)) (fourth args))
	    (apply find-frames (knodule-index knodule)
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
	 (knodule (get knodules (get dterm 'knodule))))
    (when (exists? new)
      (add! dterm slotid new)
      (index-frame (knodule-index knodule) dterm slotid new)
      (unless (exists? cur)
	(index-frame (knodule-index knodule) dterm 'has slotid))
      ((get infer-onadd slotid) dterm slotid new)
      (detail%watch "KNO/ADD!" dterm slotid new))))

(defambda (kno/drop! dterm slotid value)
  (let* ((slotid (kno/slotid slotid))
	 (drop (intersection value (get dterm slotid)))
	 (knodule (get knodules (get dterm 'knodule))))
    (when (exists? drop)
      (drop! dterm slotid drop)
      (drop! (knodule-index knodule) (cons slotid drop) dterm)
      (if (fail? (get dterm slotid))
	  (drop! (knodule-index knodule) (cons 'has slotid) dterm))
      ((get infer-ondrop slotid) dterm slotid drop))))

(defambda (kno/replace! dterm slotid value (toreplace {}))
  (for-choices dterm
    (for-choices (slotid (kno/slotid slotid))
      (let ((replace (difference (try toreplace (get dterm slotid))
				 value)))
	(let ((new (difference value (get dterm slotid)))
	      (todrop replace)
	      (knodule (get knodules (get dterm 'knodule))))
	  (when (exists? todrop)
	    (drop! dterm slotid todrop)
	    ((get infer-ondrop slotid) dterm slotid todrop)
	    (drop! (knodule-index knodule) (cons slotid todrop)
		   todrop))
	  (when (exists? new)
	    (add! dterm slotid new)
	    (index-frame (knodule-index knodule) dterm slotid new)
	    ((get infer-onadd slotid) dterm slotid new)))))))

;;; Special inference methods

(define (add-genl! f s g)
  (let ((g* (get* g 'genls))
	(g*cur (get f 'genls*))
	(knodule (get knodules (get f 'knodule))))
    (add! f 'genls* g)
    (add! f 'genls* (difference g* g*cur))
    (add! (knodule-genls* knodule) (difference g* g*cur) f)))

(define (drop-genl! f s g)
  ;; This is called after the drop happens, so g*cur actually reflects
  ;;  the update.
  (let* ((g* (get* g 'genls))
	 (g*cur (get f 'genls*))
	 (knodule (get knodules (get f 'knodule)))
	 (g*drop (difference g* g*cur))
	 (g*index (knodule-genls* knodule)))
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
  (let ((knodule (get knodules (get frame 'knodule))))
    ;; Index expanded vales (including metaphone hashes)
    (add! (knodule-index knodule)
	  (cons slotid (kno/string-indices value))
	  frame)
    ;; Update the phrasemap
    (when (compound? value)
      (let ((wordv (words->vector value))
	    (phrasemap (try (get (knodule-phrasemaps knodule) slotid)
			    (new-phrasemap knodule slotid))))
	(add! phrasemap (elts wordv) wordv)
	(add! phrasemap (list (first wordv)) wordv)))))

(defambda (drop-phrase! frame slotid value (mirror))
  (let ((knodule (get knodules (get frame 'knodule)))
	(excur (kno/string-indices (get frame slotid)))
	(exdrop (kno/string-indices value))
	(index (knodule-index knodule)))
    ;; Update the index, noting that some expanded values
    ;;  may still apply after the drop
    ;; Note that we won't bother updating the phrasemap because
    ;;  it should only be used heuristically
    (drop! (knodule-index knodule)
	   (cons slotid (difference exdrop excur))
	   frame)))

(store! infer-onadd langids add-phrase!)
(store! infer-ondrop langids drop-phrase!)

(defslambda (new-phrasemap knodule langid)
  (let ((phrasemap (get (knodule-phrasemaps knodule) langid)))
    (when (fail? phrasemap)
      (set! phrasemap (make-hashtable))
      (store! (knodule-phrasemaps knodule) langid phrasemap))
    phrasemap))

(define (kno/phrasemap knodule (langid))
  (default! langid (knodule-language knodule))
  (try (get (knodule-phrasemaps knodule) langid)
       (new-phrasemap knodule langid)))

;;; DRULES

(define (add-drule! frame slotid value)
  (add! (knodule-drules (get knodules (get frame 'knodule)))
	(drule-cues value)
	value))
(define (drop-drule! frame slotid value)
  (drop! (knodule-drules (get knodules (get frame 'knodule)))
	 (drule-cues value)
	 value))
(store! infer-onadd 'drules add-drule!)
(store! infer-ondrop 'drules drop-drule!)

;;; IADD!

(define (get-index f)
  (try (knodule-index (get knodules (get f 'knodule)))
       knodule:index))

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




