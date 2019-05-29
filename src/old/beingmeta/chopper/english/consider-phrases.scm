(load-component "../english.scm")
(use-module 'texttools)

(define histogram (file->dtype "/build/timeinc/phrases.histogram"))
(define phrases (hashtable-skim histogram 5))
(define added-phrases (make-hashset))
(define bad-words
  {"so" "say" "el" "en" "ha" "ho" "is" "it" "km" "la" "na" "nt"
   "up" "v." "va" "vs" "ate" "bad" "cos" "del" "don" "dry" "due"
   "eta" "far" "fly" "fox" "gay" "get" "guy" "hay" "his" "hip"
   "ill" "ind" "ing" "lay" "low" "mai" "mil" "moi" "muy" "no." "old"
   "p.m" "pan" "por" "put" "run" "sap" "saw" "sat" "say" "sci" "sec" "see"
   "shy" "six" "ski" "try" "way" "won" "yes"
   "one" "two" "three" "four" "five" "six" "seven" "eight" "nine" "ten"
   "eleven" "twelve" "thirteen" "fourteen" "fifteen" "sixteen"
   "seventeen" "eighteen" "nineteen" "specific" "altogether" "this"
   "next" "more" "less" "last" "back" "other" "saying"})
(define bad-heads
  {"boost" "bolster" "percent" "people" "part"})
;; joy? lee? net? oil? run? sum? sun? use?
;; children play tag
(define (reject-word? word)
  (or (= (length word) 1)
      (char-numeric? (elt word 0)) 
      (has-prefix word {"twenty" "thirty" "forty" "fifty" "sixty"
			"seventy" "eighty" "ninety"})
      (overlaps? word bad-words)))

(define (reject-components phrase)
  (let ((words (segment phrase)))
    (or (some? reject-word? words)
	(overlaps? (car words) bad-heads)
	(not (noun (elt words -1))))))

;; enough;nice;half;
(define (reject-first-word head)
  (or (and (noun-modifier head)
	   (> (noun-modifier head) 1))
      (overlaps? (get dictionary head) '{definite-article preposition})))

(define (consider-phrase phrase)
  (cond ((exists? (get dictionary phrase)))
	((exists? (noun-root phrase)))
	((exists? (verb-root phrase)))
	((search "www." phrase)
	 (message "- WWW Rejecting: " phrase))
	((has-suffix phrase "political career")
	 (message "- OBIT Rejecting: " phrase))
	((has-suffix phrase "'s")
	 (message "- APOSTROPHE Rejecting: " phrase))
	((reject-components phrase)
	 (message "- COMPONENTS Rejecting: " phrase))
	((exists? (get histogram (english-noun-root phrase)))
	 (message "+ PLURAL-EXISTS Adding: " phrase)
	 (hashset-add! added-phrases (english-noun-root phrase)))
	((> (get histogram phrase) 25)
	 (hashset-add! added-phrases phrase)
	 (message "+ 25-CASES Adding: " phrase))
	(else (let ((words (segment phrase " ")))
		(cond ((has-suffix (car words) "ing")
		       (message "- INGHEAD Rejecting: " phrase))
		      ((reject-first-word (car words))
		       (message "- FIRSTWORD Rejecting: " phrase))
		      ((not (noun (elt words -1)))
		       (message "- LASTWORD Rejecting: " phrase))
		      ((preposition (elt words -1))
		       (message "- PREPOSITION Rejecting: " phrase))
		      (else 
		       (hashset-add! added-phrases phrase)
		       (message "+ DEFAULT Adding: " phrase)))))))

(define (suffget suf)
  (let ((sufplus (append " " suf)))
    (filter-choices (term phrases)
      (has-suffix term sufplus))))

(define (main)
  (do-choices (phrase phrases)
    (consider-phrase phrase)))
