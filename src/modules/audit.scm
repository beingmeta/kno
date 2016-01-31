;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc. All rights reserved

(in-module 'audit)

;;; This provides for both audited edits and for assertions and
;;; retractions which respects audited values.

(use-module '{brico brico/indexing logger})

(define-init %loglevel %notice%)

;;; Index updates

(define audit-index #f)
(define audit-pool #f)

(define (get-index-facts f slotid value (nonlocal #t))
  (cond ((and (oid? slotid) (test slotid 'type 'language))
	 (choice (cons slotid (index-string/keys value))
		 (cons (get frag-map slotid)
		       (index-frags/keys value))))
	((and (oid? slotid) (test slotid 'type 'lexslot))
	 (cons slotid (index-string/keys value)))
	((eq? slotid @?genls)
	 (choice (cons slotid value)
		 (cons @?genls* (?? @?specls* value))
		 (tryif nonlocal
			(cons (cons @?implies
				    (choice value (?? @?specls* value)))
			      (qc (?? @?implies f))))
		 (cons @?entails value)))
	((eq? slotid @?specls)
	 (choice (cons slotid value)
		 (cons @?specls* (?? @?genls* value))
		 (cons @?entailedby value)))
	((eq? slotid @?implies)
	 (choice (cons slotid value)
		 (cons slotid (list value))
		 (cons slotid (?? @?specls* value))
		 (cons @?entails value)))
	(else (cons slotid value))))

(define (index+! f slotid value)
  (when (and audit-index
	     (exists in-pool? f (get audit-index '%pools))
	     (test audit-index slotid))
    (let ((newfacts (get-index-facts f slotid value))
	  (pools (get audit-index '%pools))
	  (keybyindex (frame-create #f))
	  (factsbyindex (frame-create #f)))
      (do-choices (fact newfacts)
	(if (pair? (car fact))
	    (let* ((key (car fact))
		   (slotid (car key))
		   (index (get audit-index slotid) ))
	      (add! keybyindex index key)
	      (add! factsbyindex index fact))
	    (let* ((key fact)
		   (slotid (car key))
		   (index (get audit-index slotid)))
	      (add! keybyindex index key)
	      (add! factsbyindex index fact))))
      (do-choices (index (getkeys keybyindex))
	(prefetch-keys! index (get keybyindex index)))
      (do-choices (index (getkeys factsbyindex))
	(do-choices (fact (get factsbyindex index))
	  (if (pair? (car fact))
	      (add! index (car fact) (cdr fact))
	      (unless (test index fact f) (add! index fact f))))))))

(define (index-! f slotid value)
  (when (and audit-index
	     (exists in-pool? f (get audit-index '%pools))
	     (test audit-index slotid))
    (let ((dropfacts (difference (get-index-facts f slotid value #f)
				 (get-index-facts f slotid (get f slotid))))
	  (pools (get audit-index '%pools))
	  (keybyindex (frame-create #f))
	  (factsbyindex (frame-create #f)))
      (do-choices (fact dropfacts)
	(if (pair? (car fact))
	    (let* ((key (car fact))
		   (slotid (car key))
		   (index (get audit-index slotid)))
	      (add! keybyindex index key)
	      (add! factsbyindex index fact))
	    (let* ((key fact)
		   (slotid (car key))
		   (index (get audit-index slotid)))
	      (add! keybyindex index key)
	      (add! factsbyindex index fact))))
      (do-choices (index (getkeys keybyindex))
	(prefetch-keys! index (get keybyindex index)))
      (do-choices (index (getkeys factsbyindex))
	(do-choices (fact (get factsbyindex index))
	  (if (pair? (car fact))
	      (drop! index (car fact) (cdr fact))
	      (when (test index fact f) (drop! index fact f))))))))

(module-export! '{index+! index-})

;;; Configuring the audit index

(define (dbfile->auditindex file)
  (notify "Generating audit index for " (write file))
  (let ((v (file->dtype file))
	(ai (frame-create #f)))
    (when (and (test v 'pools) (test v 'indices))
      (notify "Assuming " file " is a USEDB database description")
      (do-choices (poolspec (get v 'pools))
	(add! ai '%pools
	      (if (string? poolspec)
		  (if (or (position #\@ poolspec) (has-prefix poolspec "/"))
		      (use-pool poolspec)
		      (use-pool (get-component poolspec file)))
		  (tryif (pool? poolspec) poolspec))))
      (do-choices (ixspec (get v 'indices))
	(when (and (string? ixspec) (not (position #\@ ixspec)))
	  (let ((ix (if (has-prefix ixspec "/")
			(open-index ixspec)
			(open-index (get-component ixspec file)))))
	    (add! ai (elts (onerror (hash-index-slotids ix) {})) ix)))))
    ai))

(define (config-auditindex var (val))
  (if (bound? val)
      (if (string? val)
	  (let ((ai (dbfile->auditindex val)))
	    (set! audit-index ai)
	    ai)
	  (begin (set! audit-index val) val))
      audit-index))
(config-def! 'auditindex config-auditindex)

(define (config-auditpool var (val))
  (if (bound? val)
      (if (string? val)
	  (let ((ai (use-pool val)))
	    (set! audit-index ai)
	    ai)
	  (begin (set! audit-pool val) val))
      audit-index))
(config-def! 'auditpool config-auditpool)

;;; Auditing

(define auditor #f)

(define auditor-config
  (slambda (var (val))
    (if (bound? val)
	(if (and (string? val) (position #\@ val))
	    (set! auditor val)
	    (error TYPE "Invalid auditor, must be email address: " val))
	auditor)))
(config-def! 'auditor auditor-config)

(define audit+!
  (ambda (frame slotid value (invert #t))
    (if (not auditor) (error NOCONFIG "No auditor configured"))
    (do-choices (frame frame)
      (do-choices (slotid slotid)
	(assert! frame slotid value)
	(unless (test frame 'sensecat)
	  (when (overlaps? slotid '{@?always @?genls @?isa hypernym})
	    (store! frame 'sensecat (get value 'sensecat))
	    (make%id! frame)))
	(add! frame '%adds
	      (vector slotid (qc value)
		      auditor (timestamp)
		      (config 'sessionid)))
	(when audit-index
	  (index+! frame slotid value)
	  (when (and invert (oid? value) (test slotid 'inverse))
	    (index+! value (get slotid 'inverse) frame)))))))
(define audit-!
  (ambda (frame slotid value (invert #t))
    (if (not auditor) (error NOCONFIG "No auditor configured"))
    (do-choices (frame frame)
      (do-choices (slotid slotid)
	(retract! frame slotid value)
	(add! frame '%drops
	      (vector slotid (qc value)
		      auditor (timestamp)
		      (config 'sessionid)))
	(when audit-index
	  (index-! frame slotid value)
	  (when (and invert (oid? value) (test slotid 'inverse))
	    (audit-! value (get slotid 'inverse) frame #f)))))))

(define audit!
  (ambda (frame slotid arg3 (arg4))
    (if (not auditor) (error NOCONFIG "No auditor configured"))
    (if (and (eq? arg3 'not) (bound? arg4))
	(audit-! frame slotid arg4)
	(if (bound? arg4)
	    (begin (audit+! frame slotid arg3)
		   (audit-! frame slotid arg4))
	    (do-choices frame
	      (do-choices slotid
		(let* ((current (get frame slotid))
		       (add (difference arg3 current))
		       (drop (difference current arg3)))
		  (audit+! frame slotid add)
		  (audit-! frame slotid drop))))))))

(define (audit-get frame slotid)
  (second (pick (get frame '%adds) first slotid)))
(define (audit-get-not frame slotid)
  (second (pick (get frame '%drops) first slotid)))

(module-export! '{audit+! audit-! audit! audit-get audit-get-not})

;;; AUTO procedures

;;; These do asserts and retracts providing that the specified
;;;  value has not been audited.

(define (check-audit audit slotid value)
  (and (overlaps? slotid (first audit))
       (overlaps? value (second audit))))

(define auto+!
  (ambda (frame slotid value (invert #t))
    (do-choices (frame frame)
      (do-choices (slotid slotid)
	(do-choices (value value)
	  (if (exists check-audit (get frame '%drops) slotid value)
	      (%debug "Deferring assertion due to audit: "
		      slotid "(" frame ")=" value)
	      (begin (assert! frame slotid value)
		     (unless (test frame 'sensecat)
		       (when (overlaps? slotid '{@?always @?genls @?isa hypernym})
			 (store! frame 'sensecat (get value 'sensecat))
			 (make%id! frame)))
		     (when audit-index
		       (index+! frame slotid value)
		       (when (and invert (oid? value) (test slotid 'inverse))
			 (index+! value (get slotid 'inverse) frame))))))))))

(define auto-!
  (ambda (frame slotid value (invert #t))
    (do-choices (frame frame)
      (do-choices (slotid slotid)
	(do-choices (value value)
	  (if (exists check-audit (get frame '%adds) slotid value)
	      (%debug "Deferring retraction due to audit: "
		      slotid "(" frame ")=" value)
	      (begin (retract! frame slotid value)
		     (when audit-index
		       (index-! frame slotid value)
		       (when (and invert (oid? value) (test slotid 'inverse))
			 (index-! value (get slotid 'inverse) frame)))		     )))))))

(define auto!
  (ambda (frame slotid arg3 (arg4))
    (if (and (eq? arg3 'not) (bound? arg4))
	(auto-! frame slotid arg4)
	(if (not (bound? arg4))
	    (auto+! frame slotid arg3)
	    (error SYNTAX "Bad AUTO! call")))))

(module-export! '{auto+! auto-! auto!})

;;; Reapplying audits

(define (reaudit frame)
  (let ((audits
	 (choice (for-choices (add (get frame '%adds))
		   (tryif (> (length add) 2)
			  (vector 'ADD (elt add 3) add)))
		 (for-choices (drop (get frame '%drops))
		   (tryif (> (length drop) 2)
			  (vector 'drop (elt drop 3) drop))))))
    (doseq (audit (sorted audits first))
      (if (eq? (elt audit 0) 'add)
	  (assert! frame (elt (elt audit 2) 0)
		   (elt (elt audit 2) 1))
	  (if (eq? (elt audit 0) 'drop)
	      (retract! frame (elt (elt audit 2) 0)
			(elt (elt audit 2) 1)))))))

(module-export! 'reaudit)

(define (audit-genl! f genls)
  (audit-! f @?genls (difference (get f @?genls) genls))
  (audit+! f @?genls genls)
  (let ((scat (try (difference (get genls 'sensecat) 'NOUN.TOPS)
		   (pick-one (get (?? @?genls genls) 'sensecat)))))
    (audit-! f 'sensecat (difference (get f 'sensecat) scat))
    (audit+! f 'sensecat scat))
  (make%id! f))

(module-export! 'audit-genl!)

(defambda (newterm term genls (opts '()) (gloss #f) . slotids)
  (if (not auditor) (error NOCONFIG "No auditor configured"))
  (if (and (not gloss) (null? slotids))
      (audit+! genls (getopt opts 'language default-language)
	       term)
      (let* ((pool (getopt opts 'pool audit-pool))
	     (index (getopt opts 'index audit-index))
	     (language (getopt opts 'language default-language))
	     (f (frame-create pool '%created (cons auditor (timestamp)))))
	(audit+! f language term)
	(audit+! f @?genls genls)
	(let ((scat (try (difference (get genls 'sensecat) 'NOUN.TOPS)
			 (pick-one (get (?? @?genls genls) 'sensecat))))
	      (type (get genls 'type)))
	  (audit+! f 'sensecat scat)
	  (audit+! f 'type type))
	(when gloss
	  (assert! f (get gloss-map
			  (getopt opts 'language default-language))
		   gloss))
	(do ((slotids slotids (cddr slotids)))
	    ((null? slotids) f)
	  (audit+! f (car slotids) (cadr slotids)))
	(when (in-pool? f brico-pool)
	  (add! f 'words (get f @?en))
	  (store! f 'ranked
		  (append (vector term)
			  (rsorted  (get f @?en) length))))
	(make%id! f)
	f)))

(module-export! 'newterm)

(defambda (audit-genls! f genl (name #f))
  (for-choices f
    (when (in-pool? f brico-pool)
      (let ((h+ (difference genl (get f 'hypernym)))
	    (h- (difference (get f 'hypernym) genl)))
	(drop! f 'hypernym h-)
	(add! f 'hypernym h+)
	(drop! (pick h- brico-pool) 'hyponym f)
	(add! (pick h+ brico-pool) 'hyponym f)))
    (let ((g+ (difference genl (get f @?genls)))
	  (g- (difference (get f @?genls) genl)))
      (add! (pick f brico-pool) 'hypernym g+)
      (drop! (pick f brico-pool) 'hypernym g-)
      (audit+! f @?genls g+)
      (audit-! f @?genls g-)
      (when (exists? (get genl 'sensecat))
	(store! f 'sensecat (get genl 'sensecat)))
      (when (exists? (get genl 'type))
	(store! f 'type (get genl 'type)))
      (drop! f 'type 'individual)
      (unless name (drop! f 'type 'name))
      (when (capitalized? (get f @?en)) (low%frame! f))
      (make%id! f)
      f)))

(defambda (audit-isa! f isa)
  (for-choices f
    (when (in-pool? f brico-pool)
      (let ((h+ (difference isa (get f 'hypernym)))
	    (h- (difference (get f 'hypernym) isa)))
	(drop! f 'hypernym h-)
	(add! f 'hypernym h+)
	(drop! (pick h- brico-pool) 'hyponym f)
	(add! (pick h+ brico-pool) 'hyponym f)))
    (audit+! f @?isa (difference isa (get f @?isa)))
    (audit-! f @?isa (difference (get f @?isa) isa))
    (audit-! f @?genls (get f @?genls))
    (when (exists? (get isa 'sensecat))
      (store! f 'sensecat (get isa 'sensecat)))
    (when (exists? (get isa 'type))
      (store! f 'type (get isa 'type)))
    (add! f 'type '{individual name})
    (make%id! f)
    f))

(module-export! '{audit-genls! audit-isa!})
