(in-module 'speling)

(use-module '{texttools})

(module-export!
 '{speling/fix speling/add! speling/xgrams speling/getscores})

(define spelings (make-hashtable))

(define (speling/xgrams string)
  "Gets keys from a string to use for finding correct spellings"
  (let ((half (quotient (length string) 2)))
    (choice ({trigrams bigrams} string)
	    (string-append "<" ({trigrams bigrams} (subseq string 0 half)))
	    (string-append ">" ({trigrams bigrams} (subseq string half))))))

(define (speling/add! term (table spelings) (keys))
  "Adds an entry to the spelling dictionary"
  (default! keys term)
  (add! table keys term)
  (add! table (metaphone keys #t) term)
  (add! table (speling/xgrams keys) term))

(define (speling/fix string (table spelings) (threshold 0.5))
  "Tries to correct a spelling against a dictionary"
  (try (get table string)
       (get table (metaphone string #t))
       (let ((morphed (morphrule string (get table 'morphrules))))
	 (try (get table (metaphone morphed #t))
	      (let* ((scores (speling/getscores (choice string morphed)))
		     (maxposs (- (get scores string)))
		     (maxval (table-maxval scores)))
		(tryif (> maxval (* threshold maxposs))
		  (table-max scores)))))))

(defambda (speling/getscores strings (table spelings))
  "Gets a set of scored terms matching a string against a spelling dictionary"
  (let ((xgrams (speling/xgrams strings))
	(weightfn (try (get table 'weightfn) #f))
	(scores (make-hashtable)))
    (do-choices (xgram xgrams)
      (let* ((hits (get table xgram))
	     (weight (/~ (if weightfn (weightfn xgram) (length xgram))
			 (1+ (ilog (choice-size hits))))))
	(hashtable-increment! scores hits weight)
	(hashtable-increment! scores strings (- weight))))
    scores))

