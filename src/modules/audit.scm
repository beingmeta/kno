;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'audit)

(use-module 'brico)

;;; This provides for both audited edits and for assertions and
;;; retractions which respects audited values.

(define version "$Id$")

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
  (ambda (frame slotid value)
    (if (not auditor) (error NOCONFIG "No auditor configured"))
    (do-choices (frame frame)
      (do-choices (slotid slotid)
	(assert! frame slotid value)
	(add! frame '%adds
	      (vector slotid (qc value)
		      auditor (timestamp)
		      (config 'sessionid)))))))
(define audit-!
  (ambda (frame slotid value)
    (if (not auditor) (error NOCONFIG "No auditor configured"))
    (do-choices (frame frame)
      (do-choices (slotid slotid)
	(retract! frame slotid value)
	(add! frame '%drops
	      (vector slotid (qc value)
		      auditor (timestamp)
		      (config 'sessionid)))))))

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

(module-export! '{audit+! audit-! audit! audit-get})

;;; AUTO procedures

;;; These do asserts and retracts providing that the specified
;;;  value has not been audited.

(define (check-audit audit slotid value)
  (and (overlaps? slotid (first audit))
       (overlaps? value (second audit))))

(define auto+!
  (ambda (frame slotid value)
    (do-choices (frame frame)
      (do-choices (slotid slotid)
	(do-choices (value value)
	  (if (exists check-audit (get frame '%drops) slotid value)
	      (notify "Deferring assertion due to audit: "
		      slotid "(" frame ")=" value)
	      (assert! frame slotid value)))))))

(define auto-!
  (ambda (frame slotid value)
    (do-choices (frame frame)
      (do-choices (slotid slotid)
	(do-choices (value value)
	  (if (exists check-audit (get frame '%adds) slotid value)
	      (notify "Deferring retraction due to audit: "
		      slotid "(" frame ")=" value)
	      (retract! frame slotid value)))))))

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



