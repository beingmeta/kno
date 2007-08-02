(in-module 'audito)

;;; This provides for both audited edits and automatic assignment which respects
;;;  audited values.

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
	(if (not (bound? arg4))
	    (audit+! frame slotid arg3)
	    (error SYNTAX "Bad AUDIT! call")))))

(module-export! '{audit+! audit-! audit!})

;;; AUTO procedures

;;; These do asserts and retracts providing that the specified
;;;  value has not been audited.

(define (check-audit audit slotid value)
  (and (overlaps? slotid (first audit))
       (overlaps? value (second audit))))

(define auto+!
  (ambda (frame slotid value)
    (if (not auditor) (error NOCONFIG "No auditor configured"))
    (do-choices (frame frame)
      (do-choices (slotid slotid)
	(if (exists check-audit (get frame '%adds) slotid value)
	    (notify "Deferring assertion due to audit: "
		    slot "(" frame ")=" value)
	    (assert! frame slotid value))))))

(define auto+!
  (ambda (frame slotid value)
    (if (not auditor) (error NOCONFIG "No auditor configured"))
    (do-choices (frame frame)
      (do-choices (slotid slotid)
	(if (exists check-audit (get frame '%adds) slotid value)
	    (notify "Deferring assertion due to audit: "
		    slot "(" frame ")=" value)
	    (assert! frame slotid value))))))

(define auto-!
  (ambda (frame slotid value)
    (if (not auditor) (error NOCONFIG "No auditor configured"))
    (do-choices (frame frame)
      (do-choices (slotid slotid)
	(if (exists check-audit (get frame '%adds) slotid value)
	    (notify "Deferring retraction due to audit: "
		    slot "(" frame ")=" value)
	    (retract! frame slotid value))))))

(define auto!
  (ambda (frame slotid arg3 (arg4))
    (if (not auditor) (error NOCONFIG "No auditor configured"))
    (if (and (eq? arg3 'not) (bound? arg4))
	(auto-! frame slotid arg4)
	(if (not (bound? arg4))
	    (auto+! frame slotid arg3)
	    (error SYNTAX "Bad AUTO! call")))))

(module-export! '{auto+! auto-! auto!})

