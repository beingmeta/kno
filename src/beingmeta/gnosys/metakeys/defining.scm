(in-module 'gnosys/metakeys/defining)

(use-module '{gnosys gnosys/metakeys brico texttools})

;;; Configuration

(define new-concepts-source #f)
(define new-concepts-index #f)
(define new-concepts-pool #f)

(define newconcepts-config
  (slambda (var (val unbound))
    (cond ((eq? val 'unbound) new-concepts-source)
	  ((equal? new-concepts-source val))
	  (else (message "Setting new concepts DB to " val)
		(set! new-concepts-pool (use-pool val))
		(set! new-concepts-index (use-index val))
		(set! new-concepts-source val)))))
(config-def! 'newconcepts newconcepts-config)

(module-export! '{new-concepts-index new-concepts-pool})
(module-export! '{parse-title})

;;; Parsing comounds

(define organization (?? @?genls*  @/brico/13c05))

(define (parse-title name language)
  (try (handle-place+org+role name language)
       (handle-org+role name language)
       (handle-place+role name language)
       (handle-role+ name language #f)
       (handle-place+org name language)))

(define (name->metakey name language)
  (tryif (compound? name)
	 (try (parse-dterm name language)
	      (let* ((titled (parse-title name language))
		     (personal-name (car titled))
		     (isa (second titled))
		     (codefines (tryif (and (exists? titled) (> (length titled) 2))
				       (third titled))))
		(cond ((exists? personal-name)
		       (when (and (exists? isa) (exists? codefines))
			 (set+! isa (newisa (qc isa) (qc codefines) language)))
		       (newperson personal-name (qc isa) (qc codefines) language))
		      ((and (exists? isa) (exists? codefines))
		       (second (newisa (qc isa) (qc codefines) language)))
		      (else (fail)))))))

(module-export! 'name->metakey)

;;; Support functions

(define (get-name-prefix name category (language @?english) (scan 0))
  (let ((name (if (capitalized? name) name (capitalize name))))
    (try (let ((concepts (if (symbol? category)
			     (pick (choice (?? language name)
					   (?? 'names (stdstring name)))
			       'sense-category category)
			     (pick (choice (?? language name)
					   (?? 'names (stdstring name)))
			       {@?isa @?genls*} category))))
	   (tryif (exists? concepts) (cons (vector name (qc concepts)) #f)))
	 (let ((space (position #\Space name scan)))
	   (if space
	       (try (get-name-prefix name (qc category) language (1+ space))
		    (cons (vector (subseq name 0 space)
				  (if (symbol? category)
				      (pick (choice (?? language (subseq name 0 space))
						    (?? 'names (stdstring (subseq name 0 space))))
					'sense-category category)
				      (pick (choice (?? language (subseq name 0 space))
						    (?? 'names (stdstring (subseq name 0 space))))
					{@?isa @?genls*} category)))
			  (1+ space)))
	       (cons (pick (?? language name) 'sense-category category)
		     #f))))))

(define (get-role-prefix name (scan 0))
  (try (let ((concepts (?? 'title name)))
	 (tryif (exists? concepts) (cons (vector name (qc concepts)) #f)))
       (let ((space (position #\Space name scan)))
	 (if space
	     (try (get-role-prefix name (1+ space))
		  (let ((role (?? 'title (subseq name 0 space))))
		    (tryif (exists? role)
			   (cons (vector (subseq name 0 space) (qc role))
				 (1+ space)))))
	     (fail)))))

(define (handle-role+ string (language @?english) (anonymousok #t))
  (let ((prefix (get-role-prefix string)))
    (cond ((fail? prefix) prefix)
	  ((cdr prefix) (list (subseq string (cdr prefix)) (car prefix)))
	  (anonymousok (list (qc) (car prefix)))
	  (else (fail)))))

(define (handle-org+role string (language @?english))
  (let* ((prefix (get-name-prefix string (qc organization) language))
	 (role (tryif (cdr prefix) (handle-role+ (subseq string (cdr prefix)) language))))
    (if (fail? role) role
	`(,(first role) ,(second role) ,(car prefix)))))

(define (check-loc-prefix entry)
  (cons (vector (first (car entry))
		(qc (pick (get (second (car entry)) 'pertainym) 'sense-category 'noun.location)))
	(cdr entry)))
(define (handle-place+role string (language @?english))
  (let* ((prefix (try (get-name-prefix string 'noun.location language)
		      (check-loc-prefix
		       (get-name-prefix string 'adj.pert language))))
	 (role (tryif (cdr prefix) (handle-role+ (subseq string (cdr prefix)) language))))
    (if (fail? role) role
	`(,(first role) ,(second role) ,(car prefix)))))
(define (handle-place+org string (language @?english))
  (let* ((prefix (try (get-name-prefix string 'noun.location language)
		      (check-loc-prefix
		       (get-name-prefix string 'adj.pert language))))
	 (org (get-name-prefix (subseq string (cdr prefix)) (qc organization) language)))
    (if (fail? org) org `({} ,(car org) ,(car prefix)))))
(define (handle-place+org+role string (language @?english))
  (let* ((prefix (try (get-name-prefix string 'noun.location language)
		      (check-loc-prefix
		       (get-name-prefix string 'adj.pert language))))
	 (org (tryif (cdr prefix)
		     (get-name-prefix (subseq string (cdr prefix)) (qc organization) language)))
	 (role (tryif (cdr org)
		      (handle-role+ (subseq string (+ (cdr prefix) (cdr org))) language))))
    (if (fail? role) role
	`(,(first role) ,(second role) ,(car org) ,(car prefix)))))

(define (newisa isaspec codefinespec language)
  (let* ((primary-name (append (first codefinespec) " " (first isaspec)))
	 (isa (second isaspec))
	 (codefines (second codefinespec))
	 (secondary-names
	  (append (first codefinespec) " " (get isa language))))
    (try (vector primary-name
		 (?? 'names (stdstring (choice primary-name secondary-names))
		     @?isa isa @?defterms codefines))
	 (let ((f (frame-create new-concepts-pool
		    '%id primary-name
		    'names (choice primary-name secondary-names)
		    'ranked (cons primary-name (choice->list secondary-names))
		    'sense-category (get isa 'sense-category)
		    'type (get isa 'type)
		    'gloss primary-name
		    @?kindof isa @?defterms codefines)))
	   (message "Created ISA frame " f)
	   (index-frame new-concepts-index f 'names (stdstring (get f 'names)))
	   (index-frame new-concepts-index f '{type sense-category})
	   (index-frame new-concepts-index f @?isa (get isa @?kindof*))
	   (index-frame new-concepts-index f @?defterms (get codefines @?kindof*))
	   (vector primary-name f)))))

(define (getword concepts language)
  (try (pick-one (first (get concepts 'ranked)))
       (pick-one (get concepts 'title))
       (pick-one (get concepts language))))

(define (newperson name isaspec codefinespec language)
  (let* ((segname (segment name))
	 (family-name (elt segname -1))
	 (given-name (if (> (length segname) 1) (elt segname 0) {}))
	 (isa (second isaspec))
	 (codefines (second codefinespec))
	 (partof (pick codefines 'sense-category 'noun.location)))
    (when (and (> (length segname) 2) (has-suffix given-name "."))
      (set! given-name (choice (second segname)
			       (append (first segname) " " (second segname)))))
    (when (and (> (length segname) 2) (lowercase? (elt segname -2)))
      (set! family-name (append (elt segname -2) " " (elt segname -1))))
    (let* ((alt-names (choice name (append given-name " " family-name)))
	   (alt-titled (append (capitalize (get isa language)) " " alt-names))
	   (alt-located (append (first codefinespec) " " alt-titled))
	   (all-names (choice family-name alt-names alt-titled alt-located))
	   (primary (stdspace (append (try (pick-one (first codefinespec)) "")
				      " "
				      (try (pick-one (first isaspec)) "") " "
				      name))))
      (let* ((stdnames
	      (stdstring (choice family-name
				 (append given-name " " family-name))))
	     (existing
	      (try (?? 'names stdnames @?isa isa @?defterms codefines)
		   (?? 'names stdnames @?isa isa)))
	     (f (try existing
		     (frame-create new-concepts-pool
		       '%id primary
		       'normative primary
		       'names all-names
		       'given given-name
		       'family family-name
		       'type 'name
		       'gloss primary
		       @?isa isa @?defterms codefines @?partof partof)))
	     (new-names (tryif (exists? existing)
			       (difference all-names (get existing 'names))))
	     (newisa (tryif (exists? existing)
			    (difference isa (get existing @?isa))))
	     (newcodefs (tryif (exists? existing)
			       (difference codefines (get existing @?defterms))))
	     (newpartof (tryif (exists? existing)
			       (difference partof (get existing @?partof)))))
	(cond ((fail? existing)
	       (message "Created NAME frame " f)
	       (index-frame new-concepts-index f 'names (stdstring all-names))
	       (index-frame new-concepts-index f '{type sense-category})
	       (index-frame new-concepts-index f @?isa (get isa @?kindof*))
	       (index-frame new-concepts-index f @?partof (get partof @?partof*))
	       (index-frame new-concepts-index f @?defterms (get codefines @?kindof*)))
	      (else (assert! existing 'names new-names)
		    (index-frame new-concepts-index
			existing 'names (stdstring new-names))
		    (assert! existing @?isa newisa)
		    (index-frame new-concepts-index
			existing @?isa (get newisa @?genls*))
		    (assert! existing @?defterms newcodefs)
		    (index-frame new-concepts-index
			existing @?defterms (get newcodefs {@?genls* @?partof*}))
		    (assert! existing @?defterms newpartof)
		    (index-frame new-concepts-index
			existing @?partof (get newpartof @?partof*))))
	f))))





