;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'knodules)

;;; Core file for the knodules implementation
;;; Provides data structures, core tables, and basic
;;;  KR functions

(use-module '{texttools ezrecords varconfig logger reflection})
(use-module 'knodules/drules)

(module-export!
 '{knodule/ref get-knodule
   kno/dterm kno/dref kno/ref kno/probe knodule?
   kno/add! kno/drop! kno/replace! kno/onadd! kno/ondrop! kno/find
   kno/index-string kno/string-indices
   kno/phrasemap
   kno/slotid kno/slotids kno/slotnames kno/relcodes
   knodule-name knodule-opts knodule-language
   knodule-oid knodule-pool knodule-index
   knodule-alldterms knodule-allterms knodule-dterms knodule-drules
   knodule-prime
   default-knodule knodules kno/set-dterm!
   knodule:pool knodule:index knodule:indices
   knodule! ->knodule iadd!
   kno/restore kno/dump 
   kno/dumper kno/undumper
   langids})

(define-init %loglevel %notice%)

;;;; Global tables, variables, and structures (with their configs)

(define-init knodules (make-hashtable))

(define-init knodule:pool (make-mempool "knodules" @2009/0 (* 4 1024 1024)))
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

(define-init knodule:index (make-hashtable))
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

(define-init knodule:indices knodule:index)
(varconfig! KNO:INDICES knodule:indices open-index choice)

(define-init knodule:language 'en)
(varconfig! KNO:LANG knodule:language)

;;; Various useful global tables

(define-init langids (file->dtype (get-component "langids.dtype")))
(define-init langnames (file->dtype (get-component "langnames.table")))
(define-init kno/slotids {})
(define-init kno/slotnames (make-hashtable))
(define-init kno/relcodes (make-hashtable))
(define-init onadd (make-hashtable))
(define-init ondrop (make-hashtable))

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

(defrecord (knodule
	    #[OPAQUE #t
	      CONSFN (lambda (tag name . ignore) (knodule/ref name))
	      CORELEN 1])
  name
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
  ;; All terms/hooks, etc
  (allterms (make-hashtable))
  ;; All dterms in this knodule (a hashset)
  (alldterms (make-hashset))
  ;; Rules for disambiguating words into dterms
  (drules (make-hashtable))
  ;; 'Prime' dterms are important for this knodule
  (prime (make-hashset))
  ;; Special add/drop slot handlers
  (onadd (make-hashtable))
  (ondrop (make-hashtable)))

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
	(add! kdterms (get dterm '{dterms dterm})
	      dterm)
	(add! kdterms (stdcap (get dterm '{dterms dterm})) dterm)
	(add! genls*index (get* dterm 'genls) dterm)
	(do-choices (drule (get dterm 'drules))
	  (add! drules (drule-cues drule) drule))))
    new))

(define (knodule/ref name (pool knodule:pool) (opts #{}))
  (try (tryif (knodule? name) name)
       (get knodules name)
       (let ((existing (find-frames knodule:indices 'knoname name)))
	 (if (exists? existing)
	     (restore-knodule existing)
	     (new-knodule name pool (qc opts))))))
(define get-knodule knodule/ref)

(define (knodule! . args)
  (let ((kno (apply knodule/ref args)))
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
(varconfig! knodule default-knodule ->knodule)

(define (kno/dterm term (knodule default-knodule))
  (detail%watch "KNO/DTERM" term knodule)
  (try (get (knodule-dterms knodule) (stdcap term))
       (new-dterm term knodule)))
(define (new-dterm term knodule)
  (let* ((lang (knodule-language knodule))
	 (f (frame-create (or (knodule-pool knodule) knodule:pool)
	      'knodule (knodule-oid knodule)
	      'dterm term 'dterms term lang term
	      '%id term))
	 (termkeys (choice term) (stdcap term)))
    (info%watch "NEW-DTERM" f term lang knodule)
    (store! (knodule-dterms knodule) termkeys f)
    (hashset-add! (knodule-alldterms knodule) f)
    (index-frame (choice knodule:index (knodule-index knodule))
	f '{dterm dterms} termkeys)
    (index-frame knodule:index f 'knodule)
    (index-frame (knodule-index knodule) f lang)
    (add-phrase! f lang term)
    f))

(define (kno/dref term (knodule default-knodule) (create #t))
  (detail%watch "KNO/DREF" term knodule create)
  (try (get (knodule-dterms knodule) (stdcap term))
       (tryif create (kno/dterm term knodule))))

(define (kno/probe term (knodule default-knodule))
  (get (knodule-dterms knodule) (stdcap term)))

(define (kno/ref term (knodule default-knodule) (lang) (tryhard #f))
  (default! lang (knodule-language knodule))
  (try (find-frames (knodule-index knodule) lang term)
       (find-frames (knodule-index knodule) lang (stdcap term))
       (tryif tryhard
	 (find-frames (knodule-index knodule) lang (stdcap term))
	 (find-frames (knodule-index knodule)
	   lang (choice (metaphone term #t)
			(string->packet (disemvowel term))
			(downcase term)
			(capitalize term))))))

(define (kno/set-dterm! dtf dterm (keepold #t))
  (let* ((knodule (->knodule (get dtf 'knodule)))
	 (termkeys (choice dterm (stdcap dterm)))
	 (conflict (get (knodule-dterms knodule) (stdcap dterm)))
	 (old (get dtf 'dterm))
	 (oldkeys (difference (choice old (stdcap old)) termkeys)))
    (when (and (exists? conflict) (not (identical? conflict dtf)))
      (error |DTerm conflict| kno/set-dterm!
	     conflict " is already defined for " dterm
	     " and isn't " dtf))
    (unless keepold
      (drop! (knodule-dterms knodule) oldkeys dtf)
      (drop! (choice knodule:index (knodule-index knodule))
	     (cons '{dterm dterms} oldkeys)
	     dtf))
    (store! dtf 'dterm dterm)
    (add! dtf 'dterms dterm)
    (add! (knodule-dterms knodule) termkeys dtf)
    (index-frame (choice knodule:index (knodule-index knodule))
	dtf '{dterm dterms} termkeys)))

;;; Dumping knodule structures

(define (kno/dumper x (kno #f))
  (if (oid? x)
      (if (test x 'knodule)
	  (if (test x 'knodule (knodule-oid kno))
	      (make-compound '|ldterm| (get x 'dterm))
	      (make-compound '|dterm| (get x 'dterm)
			     (get (get x 'knodule) 'knoname)))
	  (if (test x 'knoname)
	      (make-compound '|knoid| (get x 'knoname))
	      x))
      (if (pair? x)
	  (cons (kno/dumper (qc (car x)) kno)
		(kno/dumper (qc (cdr x)) kno))
	  (if (vector? x)
	      (map (lambda (elt) (kno/dumper elt kno)) x)
	      (if (table? x)
		  (let ((copy (if (hashtable? x)
				  (make-hashtable)
				  (frame-create #f))))
		    (do-choices (key (getkeys x))
		      (store! copy (kno/dumper key kno)
			      (kno/dumper (get x key) kno)))
		    copy)
		  (if (knodule? x)
		      (make-compound '|knodule| (knodule-name x))
		      x))))))
(define (kno/undumper x (kno #f))
  (if (compound-type? x)
      (if (compound-type? x '|ldterm|)
	  (kno/dterm (compound-ref x 0) kno)
	  (if (compound-type? x '|dterm|)
	      (kno/dterm (compound-ref x 0) (knodule/ref (compound-ref x 1)))
	      (if (compound-type? x '|knodule|)
		  (knodule/ref (compound-ref x 0))
		  (if (compound-type? x '|knoid|)
		      (knodule-oid (knodule/ref (compound-ref x 0)))
		      x))))
      (if (pair? x)
	  (cons (kno/undumper (qc (car x)) kno)
		(kno/undumper (qc (cdr x)) kno))
	  (if (vector? x)
	      (map (lambda (elt) (kno/undumper elt kno)) x)
	      (if (table? x)
		  (let ((copy (if (hashtable? x)
				  (make-hashtable)
				  (frame-create #f))))
		    (do-choices (key (getkeys x))
		      (store! copy (kno/undumper key kno)
			      (kno/undumper (get x key) kno)))
		    copy)
		  x)))))

(define (kno/dump kno)
  (let* ((dterms (knodule-dterms kno))
	 (index (knodule-index kno))
	 (knodes (get dterms (getkeys dterms)))
	 (genls* (knodule-genls* kno))
	 (phrasemaps (knodule-phrasemaps kno))
	 (prime (knodule-prime kno))
	 (knoid (knodule-oid kno)))
    (make-compound '|knodule|
		   (knodule-name kno) 
		   (choice (knodule-language kno)
			   (hashset-elts (knodule-opts kno)))
		   (for-choices (knode (pick knodes 'knodule knoid))
		     (kno/dumper (oid-value knode) kno))
		   (for-choices (knode (reject knodes 'knodule knoid))
		     (kno/dumper (oid-value knode) kno))
		   (for-choices (key (getkeys index))
		     (cons (qc (kno/dumper key kno))
			   (qc (kno/dumper (get index key) kno))))
		   (for-choices (key (getkeys genls*))
		     (cons (qc (kno/dumper key kno))
			   (qc (kno/dumper (get genls* key) kno))))
		   (for-choices (langid (getkeys phrasemaps))
		     (let ((phrases (get phrasemaps langid)))
		       (cons langid (qc (get phrases (getkeys phrases))))))
		   (kno/dumper (hashset-elts (knodule-alldterms kno)) kno)
		   {} ;;; drules
		   (kno/dumper (hashset-elts (knodule-prime kno)) kno))))

(define (kno/restore data)
  (let* ((elt (if (compound-type? data) compound-ref elt))
	 (knodule (knodule/ref (elt data 0) knodule:pool (qc (elt data 1))))
	 (dtermtable (knodule-dterms knodule))
	 (knoid (knodule-oid knodule))
	 (pool (knodule-pool knodule))
	 (knoindex (knodule-index knodule))
	 (genls* (knodule-genls* knodule))
	 (phrasemaps (knodule-phrasemaps knodule)))
    (do-choices (knodef (elt data 2))
      (let* ((dterm (get knodef 'dterm))
	     (dterms (get knodef 'dterms))
	     (oid (try (get dtermtable dterm)
		       (get dtermtable (stdcap dterm))
		       (get dtermtable dterms)
		       (get dtermtable (stdcap dterms)))))
	(when (fail? oid)
	  (set! oid (frame-create pool))
	  (store! dtermtable (choice dterm dterms
				     (stdcap (choice dterm dterms)))
		  oid))))
    (do-choices (knodef (elt data 2))
      (set-oid-value! (get dtermtable (stdcap (get knodef 'dterm)))
		      (kno/undumper knodef knodule)))
    (do-choices (knodef (elt data 3))
      (set-oid-value! (kno/dterm (get knodef 'dterm)
				 (kno/undumper (get knodef 'knodule)))
		      (kno/undumper knodef knodule)))
    (do-choices (ixpair (elt data 4))
      (store! knoindex (kno/undumper (car ixpair) knodule)
	      (kno/undumper (cdr ixpair) knodule)))
    (do-choices (g* (elt data 5))
      (store! genls* (kno/undumper (car g*) knodule)
	      (kno/undumper (cdr g*) knodule)))
    (do-choices (langphrases (elt data 6))
      (let ((table (make-hashtable)))
	(store! phrasemaps (car langphrases) table)
	(do-choices (phrasev (cdr langphrases))
	  (add! table (elts phrasev) phrasev)
	  (add! table (list (first phrasev)) phrasev))))
    (hashset-add! (knodule-alldterms knodule)
		  (kno/undumper (elt data 7) knodule))
    (hashset-add! (knodule-prime knodule)
		  (kno/undumper (elt data 9) knodule))
    knodule))

;;; String indexing

(define (dedash string)
  (choice (tryif (position #\- string)
	    (choice (string-subst string "-" " " )
		    (string-subst string "-" "")))
	  (tryif (position #\. string)
	    (textsubst string
		       #((subst "." "") {(spaces) (eos) (eol)})))))

(defambda (kno/string-indices value (phonetic #f))
  (let* ((values (stdspace value))
	 (expvalues (choice values (basestring values) ))
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

(defambda (kno/add! dterm slotid value)
  (detail%watch "KNO/ADD!" dterm slotid value)
  (let* ((slotid (kno/slotid slotid))
	 (cur (get dterm slotid))
	 (new (difference value cur))
	 (knodule (get knodules (get dterm 'knodule)))
	 (index (knodule-index knodule))
	 (methods (choice (get onadd slotid)
			  (get (knodule-onadd knodule) slotid))))
    (when (exists? new)
      (detail%watch "KNO/ADD!" dterm slotid new)
      (add! dterm slotid new)
      (index-frame index dterm slotid new)
      (unless (exists? cur) (index-frame index dterm 'has slotid))
      (when (exists? methods) (methods dterm slotid new)))))

(defambda (kno/drop! dterm slotid value)
  (let* ((slotid (kno/slotid slotid))
	 (drop (intersection value (get dterm slotid)))
	 (knodule (get knodules (get dterm 'knodule)))
	 (methods (choice (get ondrop slotid)
			  (get (knodule-ondrop knodule) slotid))))
    (when (exists? drop)
      (detail%watch "KNO/DROP!" dterm slotid drop)
      (drop! dterm slotid drop)
      (drop! (knodule-index knodule) (cons slotid drop) dterm)
      (if (fail? (get dterm slotid))
	  (drop! (knodule-index knodule) (cons 'has slotid) dterm))
      (when (exists? methods) (methods dterm slotid drop)))))

(defambda (kno/replace! dterm slotid value)
  (for-choices dterm
    (for-choices (slotid (kno/slotid slotid))
      (let* ((current (get dterm slotid))
	     (toadd (difference value current))
	     (todrop (difference current value))
	     (knodule (get knodules (get dterm 'knodule)))
	     (addmethods (choice (get onadd slotid)
				 (get (knodule-onadd knodule) slotid)))
	     (dropmethods (choice (get ondrop slotid)
				  (get (knodule-ondrop knodule) slotid))))
	(when (exists? todrop)
	  (drop! dterm slotid todrop)
	  (when (exists? dropmethods) (dropmethods dterm slotid todrop))
	  (drop! (knodule-index knodule) (cons slotid todrop)
		 todrop))
	(when (and (fail? value) (exists? current))
	  (drop! (knodule-index knodule) (cons 'HAS slotid) todrop))
	(when (exists? toadd)
	  (add! dterm slotid toadd)
	  (index-frame (knodule-index knodule) dterm slotid toadd)
	  (when (exists? addmethods) (addmethods dterm slotid toadd)))
	(when (and (exists? value) (fail? current))
	  (add! (knodule-index knodule) (cons 'HAS slotid) todrop))))))

;;; Setting methods

(define (kno/onadd! slotid method (knodule #f) (name) (table))
  (default! name (procedure-name method))
  (default! table (if knodule (knodule-onadd knodule) onadd))
  (if (not name) (add! table slotid method)
      (let* ((current (get table slotid))
	     (existing (pick current procedure-name name)))
	(cond ((and method (fail? existing)) (add! table slotid method))
	      ((fail? existing))
	      ((identical? existing method))
	      ((not method) (drop! table slotid existing))
	      (else (drop! table slotid existing) (add! table slotid method))))))

(define (kno/ondrop! slotid method (knodule #f) (name) (table))
  (default! name (procedure-name method))
  (default! table (if knodule (knodule-ondrop knodule) ondrop))
  (if (not name) (drop! table slotid method)
      (let* ((current (get table slotid))
	     (existing (pick current procedure-name name)))
	(cond ((and method (fail? existing)) (add! table slotid method))
	      ((fail? existing))
	      ((identical? existing method))
	      ((not method) (drop! table slotid existing))
	      (else (drop! table slotid existing) (add! table slotid method))))))

;;; Special inference methods

(define (add-genl! f s g)
  (let ((g* (get* g 'genls))
	(g*cur (get f 'genls*))
	(knodule (get knodules (get f 'knodule))))
    (iadd! f 'genls* g)
    (iadd! f 'genls* (difference g* g*cur))
    (iadd! (find-frames (knodule-index knodule) 'genls* f)
	   'genls* (difference g* g*cur))
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

(kno/onadd! 'genls add-genl!)
(kno/ondrop! 'genls drop-genl!)

;;; Specls (just the inverse)

(define (add-specl! f s v)
  (add! v 'genls f)
  (add-genl! v 'genls f))

(define (drop-specl! f s g)
  (drop! g 'genls f)
  (drop-genl! g 'genls f))

(kno/onadd! 'specls add-specl!)
(kno/ondrop! 'specls drop-specl!)

;;; Symmetric

(define (add-symmetric! frame slotid value (mirror))
  (default! mirror slotid)
  (unless (test value mirror frame)
    (kno/add! value mirror frame)))

(define (drop-symmetric! frame slotid value (mirror))
  (default! mirror slotid)
  (when (test value mirror frame)
    (kno/drop! value mirror frame)))

(kno/onadd! '{mirror equivalent identical} add-symmetric!)
(kno/ondrop! '{mirror equivalent identical} drop-symmetric!)

;;; Natural language terms

(defambda (add-phrase! frame slotid value)
  (detail%watch "ADD-PHRASE" frame slotid value)
  (let* ((knodule (get knodules (get frame 'knodule)))
	 (index (knodule-index knodule)))
    ;; Index expanded vales (including metaphone hashes)
    (add! index (cons slotid (kno/string-indices value)) frame)
    ;; Update the phrasemap
    (when (compound? value)
      (let ((wordv (words->vector value))
	    (phrasemap (try (get (knodule-phrasemaps knodule) slotid)
			    (new-phrasemap knodule slotid))))
	(add! phrasemap (elts wordv) wordv)
	(add! phrasemap (list (first wordv)) wordv)))
    (add! (knodule-allterms knodule) value frame)))

(defambda (drop-phrase! frame slotid value (mirror))
  (let* ((knodule (get knodules (get frame 'knodule)))
	 (excur (kno/string-indices (get frame slotid)))
	 (exdrop (kno/string-indices value))
	 (index (knodule-index knodule)))
    ;; Update the index, noting that some expanded values
    ;;  may still apply after the drop
    ;; Note that we won't bother updating the phrasemap because
    ;;  it should only be used heuristically
    (drop! (knodule-index knodule)
	   (cons slotid (difference exdrop excur))
	   frame)
    (drop! (knodule-allterms knodule) value frame)))

(kno/onadd! langids add-phrase!)
(kno/ondrop! langids drop-phrase!)

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
(kno/onadd! 'drules add-drule!)
(kno/ondrop! 'drules drop-drule!)

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
