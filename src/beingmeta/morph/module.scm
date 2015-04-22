(in-module 'morph)

(module-export!
 '{get-langmod
   get-noun-root get-verb-root get-gerund
   get-stop-words get-freqs
   stop-word?})

(define language-names
  '{("english" . "morph/en")
    ("en" . "morph/en")
    ("french" . "morph/fr")
    ("francais" . "morph/fr")
    ("fr" . "morph/fr")
    ("spanish" . "morph/es")
    ("espanol" . "morph/es")
    ("es" . "morph/es")
    ("dutch" . "morph/nl")
    ("nederlands" . "morph/nl")
    ("nl" . "morph/nl")
    ("finnish" . "morph/fi")
    ("suomi" . "morph/fi")
    ("fi" . "morph/fi")
    ("german" . "morph/de")
    ("deutsch" . "morph/de")
    ("deutsche" . "morph/de")
    ("de" . "morph/de")})

(define langmods (make-hashtable))

(define (get-langmod-name language)
  (cond ((symbol? language)
	 (get-langmod-name (symbol->string language)))
	((string? language)
	 (try (get language-names (stdstring language))
	      (append "morph/" (stdstring language))))
	((oid? language)
	 (append "morph/" (get language 'iso639/1)))
	(else (fail))))

(define (get-langmod-inner language)
  (cond ((environment? language) language)
	((or (symbol? language) (string? language) (oid? language))
	 (get-module (string->lisp (get-langmod-name language))))
	((table? language) language)
	(else (fail))))

(define (get-langmod language)
  (or (try (get langmods language)
	   (let ((gotten (get-langmod-inner language)))
	     (store! langmods language (try gotten #f))
	     gotten))
      {}))

(define fast-langmod
  (macro expr
    `(or (try (get ,langmods ,(cadr expr))
	      (get-langmod ,(cadr expr))))))

(define (get-noun-root term language (test #f))
  (let ((module (fast-langmod language)))
    (if (and (exists? module) module
	     (symbol-bound? 'noun-root module))
	((get module 'noun-root) term test)
	term)))
(define (get-verb-root term language (test #f))
  (let ((module (fast-langmod language)))
    (if (and (exists? module) module
	     (symbol-bound? 'verb-root module))
	((get module 'verb-root) term test)
	term)))
(define (get-gerund term language)
  (let ((module (fast-langmod language)))
    (if (and (exists? module) module
	     (symbol-bound? 'gerund module))
	((get module 'gerund) term)
	(fail))))

(define (get-freqs language)
  (let ((module (fast-langmod language)))
    (if (and (exists? module) module
	     (symbol-bound? 'wordfreqs module))
	(get module 'wordfreqs)
	(fail))))
(define (get-stop-words language)
  (let ((module (fast-langmod language)))
    (if (and (exists? module) module
	     (symbol-bound? 'stop-words module))
	(get module 'stop-words)
	(fail))))

(define (stop-word? word language)
  (let ((module (fast-langmod language)))
    (if (and (exists? module) module
	     (symbol-bound? 'stop-words module))
	(hashset-get (get module 'stop-words) word)
	(<= (length word) 2))))
