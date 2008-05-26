;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'audit)

(use-module '{brico brico/indexing logger})

;;; This provides for both audited edits and for assertions and
;;; retractions which respects audited values.

(define version "$Id$")

(define %loglevel %notice!)

;;; Index updates

(define audit-index #f)

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
		   (vector (elt add 3) 'ADD add))
		 (for-choices (add (get frame '%adds))
		   (vector (elt add 3) 'drop add)))))
    (doseq (audit (sorted audits first))
      (if (eq? (elt audit 0) 'add)
	  (assert! frame (elt (elt audit 2) 0)
		   (elt (elt audit 2) 1))
	  (if (eq? (elt audit 0) 'drop)
	      (assert! frame (elt (elt audit 2) 0)
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
      (let ((f (frame-create (getopt opts 'pool xbrico-pool)
		 @?genls genls '%created (cons auditor (timestamp)))))
	(assert! f (getopt opts 'language default-language) term)
	(let ((scat (try (difference (get genls 'sensecat) 'NOUN.TOPS)
			 (pick-one (get (?? @?genls genls) 'sensecat)))))
	  (store! f 'sensecat scat))
	(when gloss
	  (assert! f (get gloss-map
			  (getopt opts 'language default-language))
		   gloss))
	(do ((slotids slotids (cddr slotids)))
	    ((null? slotids)
	     (make%id! f)
	     f)
	  (assert! f (car slotids) (cadr slotids))))))

(module-export! 'newterm)

(defambda (audit-genls! f genl (name #f))
  (for-choices f
    (audit+! f @?genls (difference genl (get f @?genls)))
    (audit-! f @?genls (difference (get f @?genls) genl))
    (store! f 'sensecat (get genl 'sensecat))
    (store! f 'type (get genl 'type))
    (drop! f 'type 'individual)
    (unless name (drop! f 'type 'name))
    (when (capitalized? (get f @?en)) (low%frame! f))
    (make%id! f)
    f))

(defambda (audit-isa! f isa)
  (for-choices f
    (audit+! f @?isa (difference isa (get f @?isa)))
    (audit-! f @?isa (difference (get f @?isa) isa))
    (audit-! f @?genls (get f @?genls))
    (store! f 'sensecat (get isa 'sensecat))
    (store! f 'type (get isa 'type))
    (add! f 'type '{individual name})
    (make%id! f)
    f))

(module-export! '{audit-genls! audit-isa!})
