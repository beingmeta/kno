;;; -*- Mode: fdscript; -*-

(define time-pool (use-pool "/build/timeinc/data/timeinc.pool"))

(use-pool (append "/build/timeinc/data/"
		  {"timeinc"
		   "time_content" "ew_content"
		   "people_content" "fortune_content"}
		  ".pool"))
(define content-indices
  (open-index (append "/build/timeinc/data/"
		      {"timeinc"
		       "time_content" "ew_content"
		       "people_content" "fortune_content"}
		      ".index")))

(use-pool "/home/sources/gnosys/gnosys.pool")
(use-index "/home/sources/gnosys/gnosys.index")

(use-pool "bground@gemini")
(use-index "bground@gemini")

(define doc/parsed @/gnosys/4{DOC/PARSED})
(define gn/concepts @?gn/proper-names)
(define gn/proper-names @?gn/proper-names)
(define gn/noun-phrases @?gn/noun-phrases)

(define (phase1-resolve-name name)
  ;; (lineout "name=" name)
  (let ((candidates (choice (?? @?english (choice name (stdstring name)))
			    (?? 'names (stdstring name)))))
    (try (singleton candidates)
	 (singleton (get-basis candidates @?genls))
	 (singleton (get-basis candidates @?specls)))))
(define (phase1-resolve-term term)
  ;; (lineout "term=" term)
  (let ((candidates (?? @?english term)))
    (try (singleton candidates)
	 (singleton (get-basis candidates @?genls))
	 (singleton (get-basis candidates @?specls)))))
(define (phase1 node)
  (lineout "phase1 " node)
  (choice (get node {@?gn/concepts @?gn/refpoints})
	  (phase1-resolve-name (get node @?gn/proper-names))
	  (phase1-resolve-term
	   (get node {@?gn/noun-phrases @?gn/nouns @?gn/verbs}))))

(define (phase2-resolve-name name slotid context)
  ;; (lineout "name=" name "; slotid=" slotid "; context=" context)
  (let ((candidates (?? 'names (stdstring name)
			slotid context)))
    (try (intersection candidates context)
	 (singleton candidates)
	 (singleton (get-basis candidates @?genls))
	 (singleton (get-basis candidates @?specls)))))
(define (phase2-resolve-term term slotid context)
  ;; (lineout "term=" name "; slotid=" slotid "; context=" context)
  (let ((candidates (?? @?english term slotid context)))
    (try (intersection candidates context)
	 (singleton candidates)
	 (singleton (get-basis candidates @?genls))
	 (singleton (get-basis candidates @?specls)))))
(define (phase2 node context)
  (choice (try (phase2-resolve-name (get node @?gn/proper-names)
				    @?part-of* (qc context))
	       (phase2-resolve-name (get node @?gn/proper-names)
				    @?genls* (qc context))
	       (phase2-resolve-name (get node @?gn/proper-names)
				    @?isa (qc context)))
	  (phase2-resolve-term
	   (get node {@?gn/noun-phrases @?gn/nouns @?gn/verbs})
	   (qc @?genls @?member-of @?part-of @?parts)
	   (qc context))))


(define (refanalyze-doc doc)
  (let ((context (phase1 (get doc @?doc/contents))))
    ;; (lineout "Phase 1 context: " context)
    (doseq (node (get doc doc/parsed))
      ;; (lineout "context=" context)      
      (let ((delta (phase2 node (qc context))))
	;; (lineout "From " node ": " delta)
	(set+! context (choice context delta))))
    context))
(define (irefanalyze-doc doc)
  (let ((context (ipeval (phase1 (get doc @?doc/contents)))))
    ;; (lineout "Phase 1 context: " context)
    (doseq (node (get doc doc/parsed))
      (let ((delta (ipeval (phase2 node (qc context)))))
	;; (lineout "From " node ": " delta)
	(set+! context (choice context delta))))
    context))





