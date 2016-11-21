;;; This program builds the data files used by the scheme parser.
;;; These consist of a simple lexicon of part of speech information
;;; and tables mapping irregular verb and noun inflections to their roots
;;; All the tables built are hashtables dumped as DTypes.

(load-component "../english.scm")

(when (file-exists? (get-component ".config"))
  (load-config (get-component ".config")))

(config! 'cachelevel 2)
(use-module '{texttools optimize mttools})
(use-module '{brico morph/en})
(define custom-dictionary (make-hashtable))
(define dictionary (make-hashtable 9000000))
(define fragments (make-hashset))

(define nums-data (get-component "nums.dtype"))
(define custom-data (get-component "custom.data"))
(define pos-data (get-component "mobypos/part-of-speech.txt"))
;;(define wn-name-map (file->dtype (get-component "wn-name-map.dtype")))
(define noun-phrases (file->dtypes (get-component "noun-phrases.dtype")))
(define proper-names (file->dtypes (get-component "names.dtype")))

(config! 'TRACETHREADEXIT #f)

(define (likely-noun-phrase? string)
  (and (string? string)
       (compound? string)
       (not (char-punctuation? (elt string 0)))
       (not (position #\( string))
       (not (position #\: string))
       (not (textsearch '(isupper) string 2))
       (not (textsearch '(isdigit) string 2))
       (exists?
	(get (get dictionary
		  (subseq string (1+ (rposition #\Space string))))
	     'noun))))

(define bricophrases {}) ;; (file->dtype "bricophrases.dtype")

;;; Reading MOBY data

(define word-type-alist
  '{(#\N . Noun) ; 	Noun
    (#\h . Noun-phrase) ; 	Noun Phrase
    (#\V . Verb) ; 	Verb (usu participle)
    (#\t . TRANSITIVE-VERB) ; 	Verb (transitive)
    (#\i . INTRANSITIVE-VERB) ; 	Verb (intransitive)
    (#\A . Adjective) ; 	Adjective
    (#\v . Adverb) ; 	Adverb
    (#\C . Conjunction) ; 	Conjunction
    (#\P . Preposition) ; 	Preposition
    (#\! . Interjection) ; 	Interjection
    (#\r . Pronoun) ; 	Pronoun
    (#\D . Definite-article) ; 	Definite Article
    (#\I . Indefinite-article) ; 	Indefinite Article
    (#\o . Nominative) ; 	Nominative
    })

(define (process-moby-line line table)
  (let* ((word-end (position #\Tab line))
	 (pos-end (position #\| line word-end))
	 (word (stdspace (subseq line 0 word-end)))
	 (tags (get word-type-alist (elts (subseq line word-end pos-end)))))
    (add! table word tags)
    (add! table tags word)))

(define (read-moby-data filename)
  (let* ((table (make-hashtable))
	 (in (open-input-file filename))
	 (line (getline in)))
    (until (eof-object? line)
      (process-moby-line line table)
      (set! line (getline in)))
    table))

;;; POS from BRICO

;; For debugging
;; (define freqs #f)

(define use-pools {brico-pool xbrico-pool})

(define (use-pos-data-from-brico)
  (message "Using BRICO for part of speech information")
  (let ((freqtable (make-hashtable))
	(weight-table (make-hashtable))
	(pos-table (make-hashtable)))
    (message "Using sense frequency information")
    (do-choices-mt (f (pick (?? 'type 'wordform 'has 'freq) use-pools)
		      (config 'nthreads 3)
		      mt/fetchoids 20000)
      (let ((word (get f 'word)))
	(hashtable-increment! freqtable
	  (cons word (intersection (get f 'type)
				   '{noun verb adjective adverb}))
	  (* 2 (get f 'freq)))
	(when (has-suffix word "ing")
	  (let ((vroot (verb-root word)))
	    (hashtable-increment! freqtable (cons word 'verb)
	      (* 2 (get f 'freq))))))
      (swapout f))
    ;; (set! freqs freqtable)
    (message "Assembling weights table")
    (do-choices (key (getkeys freqtable))
      (add! weight-table (car key) (cons (cdr key) (get freqtable key))))
    (message "Using raw concept information")
    (do-choices-mt (f (pick (?? 'has 'words 'has 'sensecat) use-pools)
		      (config 'nthreads 3) mt/fetchoids 20000)
      (add! pos-table (get f 'words)
	    (intersection (get f 'type) '{noun verb adjective adverb}))
      (swapout f))
    (message "Updating dictionary")
    (do-choices-mt (key (getkeys pos-table) (config 'nthreads 3) #f 10000)
      (let* ((weights (get weight-table key))
	     (other-pos (difference (get pos-table key) (car weights)))
	     (threshold (quotient (largest (cdr weights)) 2))
	     (delta (if (capitalized? key) 0
			(if (test pos-table (capitalize key))
			    1 0))))
	(when (capitalized? key)
	  (if (test custom-dictionary (downcase key))
	      (if (overlaps? (car (get custom-dictionary (downcase key)))
			     '{preposition
			       adverb
			       pronoun
			       subject-pronoun
			       object-pronoun
			       possessive-pronoun})
		  (add! dictionary key '(proper-name . 2))
		  (add! dictionary key '(proper-name . 1)))
	      (add! dictionary key '(proper-name . 0))))
	(do-choices (wt weights)
	  (add! dictionary key
		(cons (car wt)
		      (+ (if (> (cdr wt) threshold) 0
			     (if (> (cdr wt) 4) 1 2))
			 delta))))
	(if (test dictionary key)
	    (add! dictionary key (cons other-pos 3))
	    (if (and (lowercase? key) (test dictionary (capitalize key)))
		(add! dictionary key (cons other-pos 1))
		(add! dictionary key (cons other-pos 0))))))
    (message "Reducing singletons")
    (do-choices (key (getkeys dictionary))
      (when (singleton? (get dictionary key))
	(store! dictionary key (cons (car (get dictionary key)) 0))))))

;;; Custom stuff

(define be-verbs  "ain't am are aren't be been is isn't was wasn't were weren't")
(define do-verbs  "did didn't do don't does doesn't")
(define have-verbs  "had hadn't has hasn't have haven't having")
(define modals
  "can can't could couldn't may might must shall should shouldn't  will won't would wouldn't")

(define (extractwords string)
  (elts (remove "" (segment string " "))))

(define be-aux
  (elts (segment "is was am are" " ")))
(define other-aux
  (elts (segment "will would won't should shouldn't shall can could
couldn't may might mightn't must mustn't have had has don't doesn't
wouldn't hath hast shalt" " ")))
(define possessive-pronouns
  (elts (segment "his her their my your our" " ")))

(define (fix-weight-pair x)
  (if (pair? x)
      (if (pair? (cdr x))
	  (cons (car x) (cadr x))
	  x)
      (fail)))

(define (custom-fixes custom-file)
  (let ((dict (choice dictionary custom-dictionary)))
    (store! dict (extractwords be-verbs)
	    '{(be-verb . 0) (aux . 3) (be-aux . 0) (noun . 7) (verb . 2)})
    (store! dict (extractwords do-verbs) '(aux . 0))
    (store! dict (extractwords "did do does") '(verb . 1))
    (store! dict (extractwords have-verbs) '{(aux . 0) (verb . 1)})
    (store! dict (extractwords modals) '(aux . 0))
    (store! dict (choice {"the" "a" "an"}
			 (capitalize {"the" "a" "an"}))
	    '{(determiner . 0) (definite-article . 0)})
    ;;  (store! dict possessive-pronouns
    ;;	      '{(determiner 0) (definite-article 0)})
    (let ((in (open-input-file custom-file)))
      (let ((expr (read in)))
	(until (eof-object? expr)
	  (if (string? (car expr))
	      (store! dict (car expr) (fix-weight-pair (elts (cdr expr))))
	      (if (symbol? (car expr))
		  (add! dict (elts (cddr expr))
			(cons (car expr) (cadr expr)))))
	  (set! expr (read in)))))))

(define (convert-dashes word)
  (if (position #\- word)
      (choice word
	      (stringout
		  (doseq (elt (segment word "-") i)
		    (printout (if (> i 0) " ") elt))))
      word))

(define (read-table filename)
  (let* ((in (open-input-file filename))
	 (table (make-hashtable))
	 (line (getline in)))
    (until (eof-object? line)
      (let ((segmented (segment line " ")))
	(add! table
	      (convert-dashes (first segmented))
	      (convert-dashes (elts (rest segmented)))))
      (set! line (getline in)))
    table))

(define (enumerate-variants words)
  (cond ((null? words) '())
	((exists? (get (get dictionary (car words)) *closed-tags*))
	 (cons (choice (car words) (downcase (car words)) (upcase (car words)))
	       (enumerate-variants (cdr words))))
	(else (cons (car words) (enumerate-variants (cdr words))))))

(define (name-variants name)
  (list->phrase (enumerate-variants (getwords name))))

(define (use-wikiphrase phrase dictionary custom)
  (let* ((ppos (position #\( phrase))
	 (base (if ppos (stdspace (subseq phrase 0 ppos))
		   (stdspace phrase))))
    (when (compound? base)
      (let* ((namep (textsearch '(isupper) base (position #\Space base)))
	     (tag (if namep 'proper-name 'noun-phrase))
	     (real-base (if namep (name-variants base) (downcase base))))
	(cond ((exists? (get custom base)))
	      ((exists? (get dictionary base))
	       (add! dictionary real-base (cons tag 1)))
	      (else
	       (add! dictionary real-base (cons tag 0))))))))

(define month-names
  '("Jan" "Jan." "January"
    "Feb" "Feb." "February"
    "Mar" "Mar." "March"
    "Apr" "Apr." "April"
    "May"
    "Jun" "Jun." "June"
    "Jul" "Jul." "July"
    "Aug" "Aug." "August"
    "Sep" "Sep." "Sept" "Sept." "September"
    "Oct" "Oct." "October"
    "Nov" "Nov." "November"
    "Dec" "Dec." "December"))

(define (add-time-terms!)
  (let* ((base1 (append {"this" "last" "next"} " "
			{"week" "fortnight" "month" "year"
			 "Monday" "Tuesday" "Wednesday" "Thursday"
			 "Friday" "Saturday" "Sunday"}))
	 (base2 (append {"this" "yesterday" "tomorrow"
			 "Monday" "Tuesday" "Wednesday" "Thursday"
			 "Friday" "Saturday" "Sunday"} " "
			{"morning" "afternoon" "evening"}))
	 (combined (choice "last night" "tomorrow night" "tonight"
			   base1 base2))
	 (modified (append {"early" "late" "later" "earlier"} " "
			   combined)))
    (add! (choice dictionary custom-dictionary)
	  (choice modified combined)
	  '{(time-word . 0)}))
  (dolist (month-name month-names)
    (dotimes (date 31)
      (add! (choice dictionary custom-dictionary)
	    (choice (stringout month-name " " (1+ date))
		    (stringout (1+ date) " "month-name)
		    {"Monday" "Tuesday" "Wednesday" "Thursday"
		     "Friday" "Saturday" "Sunday"})
	    '(time-word . 0)))))

(define (subst-apostrophes word)
  (stringout (do ((start 0 (1+ end))
		  (end (position #\' word) (position #\' word (1+ end))))
		 ((not end) (printout (subseq word start)))
	       (printout (subseq word start end))
	       (printout "\u2019"))))

(define (fix-apostrophes)
  (do-choices (key (getkeys custom-dictionary))
    (when (position #\' key)
      (store! (choice custom-dictionary dictionary)
	      (subst-apostrophes key)
	      (get custom-dictionary key)))))

(define (get-frags compound)
  (let ((wordvec (words->vector compound))
	(frags {}))
    (dotimes (i (- (length wordvec) 1))
      (set+! frags (->list (subseq wordvec 0 (+ 1 i)))))
    frags))

(define (add-number-terms!)
  (let ((nums (file->dtypes nums-data)))
    (add! (choice dictionary custom-dictionary)
	  nums '{(COUNT-ADJECTIVE . 0) (NOUN . 2) (NUMBER . 1)})))

(define modifier-abbrevs-init
  "Gov.Sen.Rep.Dr.Lt.Col.Gen.Mr.Mrs.Miss.Ms.Co.Jan.Feb.Mar.Apr.Jun.Jul.Aug.Sep.Sept.Oct.Nov.Dec.Rev.Fr.Sis.")
(define name-abbrevs-init
  "Corp.Calif.Mass.Ariz.Wash.Mich.Kans.Colo.Neva.Penn.Okla.Co.Inc.Corp.LLC.Ltd.")
(define abbrevs-init (choice modifier-abbrevs-init name-abbrevs-init))
(define (init-abbrevs)
  (add! dictionary (difference (elts (segment modifier-abbrevs-init ".")) "")
	'{(proper-modifier . 0) (proper-name . 2)})
  (add! dictionary (difference (elts (segment name-abbrevs-init ".")) "")
	'{(proper-name . 1) })
  (add! dictionary
	(append (difference (elts (segment modifier-abbrevs-init ".")) "")
		".")
	'(proper-modifier . 0))
  (add! dictionary
	(append (difference (elts (segment name-abbrevs-init ".")) "")
		".")
	'{(proper-modifier . 1) (proper-name . 0)}))

(define (custom-closed!)
  (do-choices (term (getkeys custom-dictionary))
    (when (and (lowercase? term)
	       (exists? (get (get custom-dictionary term)
			     *closed-tags*)))
      (add! (capitalize term)
	    '{(proper-name . 4) (proper-modifier . 4)}))))

(define this-directory (dirname (get-component "import-moby.scm")))

(define (build-dictionary)
  (message "Integrating custom information")
  (custom-fixes custom-data)
  (add-time-terms!)
  (add-number-terms!)
  (fix-apostrophes)
  (init-abbrevs)
  (use-pos-data-from-brico)
  (do-choices (phrase bricophrases)
    (unless (or (has-prefix phrase "list of ")
		(length ())))
    (if (or (test custom-dictionary phrase) (test dictionary phrase))
        (unless (test (get (choice custom-dictionary dictionary) phrase)
		      'noun-phrase)
          (add! dictionary phrase '(noun-modifier . 1))
          (add! dictionary phrase '(noun-phrase . 1)))
        (begin (add! dictionary phrase '(noun-modifier . 1))
               (add! dictionary phrase '(noun-phrase . 0)))))
  (drop! dictionary "") (drop! custom-dictionary "")
  (drop! dictionary "-") (drop! custom-dictionary "-")
  (drop! dictionary "--") (drop! custom-dictionary "--")
  (do-choices (key (getkeys dictionary))
    (when (and (has-suffix key ".") (> (length key) 2))
      (add! dictionary (subseq key 0 -1) '(prefix . 0))
      (add! dictionary (cons (subseq key 0 -1) ".")
	    (vector (subseq key 0 -1) ".")))
    (when (compound? key)
      (let ((wordv (words->vector key)))
	(when (> (length wordv) 1)
	  (add! dictionary (first wordv) '(prefix . 0))
	  (add! dictionary (cons (first wordv) (second wordv))
		wordv)))))
  (add! dictionary noun-phrases '(noun-phrase . 0))
  (add! dictionary proper-names '(proper-name . 0)))

(define (optimizer)
  (optimize! use-pos-data-from-brico build-dictionary get-frags))

(define (main (directory #f))
  (unless directory (set! directory this-directory))
  (optimizer)
  (message "Writing lexicon information into " directory)
  (build-dictionary)
  (message "Writing out data")
  ;; (dtype->file (hashset-elts fragments) (append directory "/fragments.hashset"))
  (dtype->file custom-dictionary (append directory "/custom.table"))
  (dtype->file dictionary (append directory "/dictionary.table"))
  (dtype->file (read-table (append this-directory "/wn16/noun.exc"))
	       (append directory "/noun-roots.table"))
  (dtype->file (read-table (append this-directory "/wn16/verb.exc"))
	       (append directory "/verb-roots.table"))
  (dtype->file (read-table (append this-directory "/wn16/adj.exc"))
	       (append directory "/adj-roots.table"))
  (dtype->file (read-table (append this-directory "/wn16/adv.exc"))
	       (append directory "/adv-roots.table")))



