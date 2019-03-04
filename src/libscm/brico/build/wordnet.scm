;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'brico/build/wordnet)

(use-module '{texttools})
(use-module '{logger varconfig optimize stringfmts})
(use-module '{flexdb})
(use-module '{brico})

(module-export! '{link-release! check-release-links
		  import-synsets read-synset finish-import
		  core.index wordnet.index wordforms.index
		  line->synset ref-synset
		  fix-wordform fix-wnsplit
		  read-sense-index})

(define wnrelease 'wn31)
(define wordnet-release @1/94d47) ;; 3.0 @1/46074

(config-def! 'brico:wordnet 
  (lambda (var (val))
    (cond ((not (bound? val)) wordnet-release)
	  ((and (oid? val) (test val 'release))
	   (set! wnrelease (get val 'release))
	   (set! wordnet-release val))
	  ((oid? val) (error |InvalidRelease| val))
	  ((and (symbol? val) (test (?? '%id val) 'release))
	   (set! wordnet-release (?? '%id val))
	   (set! wnrelease (get wordnet-release 'release)))
	  ((and (string? val)
		(or (test (?? '%id val) 'release)
		    (test (?? '%id (string->symbol (upcase val))) 'release)))
	   (set! wordnet-release (?? '%id val))
	   (set! wnrelease (get wordnet-release 'release)))
	  (else (error |InvalidRelease| val)))))
(define en @1/2c1c7)

(define sensecats
  #(ADJ.ALL ADJ.PERT ADV.ALL NOUN.TOPS NOUN.ACT NOUN.ANIMAL NOUN.ARTIFACT 
    NOUN.ATTRIBUTE NOUN.BODY NOUN.COGNITION NOUN.COMMUNICATION NOUN.EVENT 
    NOUN.FEELING NOUN.FOOD NOUN.GROUP NOUN.LOCATION NOUN.MOTIVE NOUN.OBJECT 
    NOUN.PERSON NOUN.PHENOMENON NOUN.PLANT NOUN.POSSESSION NOUN.PROCESS 
    NOUN.QUANTITY NOUN.RELATION NOUN.SHAPE NOUN.STATE NOUN.SUBSTANCE NOUN.TIME 
    VERB.BODY VERB.CHANGE VERB.COGNITION VERB.COMMUNICATION VERB.COMPETITION 
    VERB.CONSUMPTION VERB.CONTACT VERB.CREATION VERB.EMOTION VERB.MOTION 
    VERB.PERCEPTION VERB.POSSESSION VERB.SOCIAL VERB.STATIVE VERB.WEATHER ADJ.PPL))

(define core.index #f)
(define wordnet.index #f)
(define wordforms.index #f)

(when brico-dir
  (set! core.index (open-index (mkpath brico-dir "core.index")))
  (set! wordnet.index (open-index (mkpath brico-dir "wordnet.index")))
  (set! wordforms.index wordnet.index))

;;; Reading WordNet sense indexes (index.sense)

(define key-types #(NOUN VERB ADJECTIVE ADVERB SATELLITE))

(define pos-cat-pat #((bos) (not> "%") "%" (label poscode (isdigit+) #t)))

(define (sense-index-reader in (line))
  (set! line (getline in))
  (and (exists? line) (string? line)
       (let* ((s (segment line " "))
	      (key (first s))
	      (poscode (get (text->frames pos-cat-pat key) 'poscode))
	      (pos (elt key-types (-1+ poscode))))
	 (when (fail? poscode) (lineout line))
	 `#[key ,(first s) 
	    type ,pos
	    synset ,(string->number (second s)) 
	    sense_no ,(string->number (third s))
	    freq ,(string->number (fourth s))
	    line ,line])))

(define (read-sense-index in)
  (when (string? in) (set! in (open-input-file in)))
  (let ((results {})
	(sense (sense-index-reader in))
	(count 1))
    (while sense
      (set+! results sense)
      (set! count (1+ count))
      (set! sense (sense-index-reader in)))
    results))

(define (assign-sensekey entry release)
  (let ((found (find-frames wordnet.index 
		 'synsets `(,release ,(get entry 'synset) ,(get entry 'type)))))
    (unless (test found '%sensekeys)
      (store! found '%sensekeys #[])
      (add! (get found '%sensekeys) release (get entry 'key)))))
(module-export! 'assign-sensekey)

(define (link-release! sense-index (release wnrelease) (mods (make-hashtable)))
  (let ((senses (read-sense-index sense-index))
	(wnoids (find-frames wordnet.index 'has 'sensekeys))
	(synsetmap (make-hashtable))
	(sensemap (make-hashtable)))
    (prefetch-keys! wordnet.index (cons 'sensekeys (get senses 'key)))
    (unless mods (lock-oids! wnoids))
    (prefetch-oids! wnoids)
    (do-choices (sense senses)
      (add! sensemap (get sense 'key) sense)
      (let* ((oid (find-frames wordnet.index 'sensekeys (get sense 'key)))
	     (type (intersection (get oid 'type) '{noun verb adjective adverb}))
	     (synset-key (list release (get sense 'synset) type)))
	(add! synsetmap oid synset-key)))
    (do-choices (oid (getkeys synsetmap))
      (let ((key (get synsetmap oid)))
	(when (and (ambiguous? key) (not (test oid 'wnsplit)))
	  (logwarn |SplitConcept| 
	    "The concept " oid " has been split into " key)
	  (cond (mods
		 (add! mods oid (cons 'wnsplit key))
		 (add! mods oid (cons 'type 'deprecated)))
		(else
		 (add! oid 'wnsplit key)
		 (add! oid 'type 'deprecated)
		 (index-frame wordnet.index oid 'has 'wnsplit)
		 (index-frame {wordnet.index core.index} oid 'type 'deprecated))))
	(when (singleton? key)
	  (cond (mods (add! mods oid (cons 'synsets key)))
		(else
		 (add! oid 'synsets key)
		 (index-frame wordnet.index oid 'synsets key))))))
    `#[sensemap ,sensemap synsetmap ,synsetmap mods ,mods]))

(define (check-release-links wnrelease)
  "This returns any cases where the synset mapping for WNRELEASE is ambiguous"
  (let ((tagged (find-frames wordnet.index 'has '{sensekeys %synsets})))
    (prefetch-oids! tagged)
    (filter-choices (synset tagged)
      (ambiguous? (pick (get synset 'synsets) wnrelease)))))

;;;; Reading WordNet synset data files (data.pos)

(define wordnet-debug #f)
(varconfig! brico:wndebug wordnet-debug)

(define typecodes
  #["n" noun "v" verb "a" adjective "s" {adjective_satellite adjective} "r" adverb])
(define ptrcodes
  #[noun
    #["!" antonym "@" hypernym "@i" isa "~" hyponym "~i" instances
      "#m" memberof "#s" ingredientof "#p" partof
      "%m" members "%s" ingredients "%p" parts
      "=" attribute "+" derivations
      ";c" topic_domain "-c" topic_refs
      ";r" region_domain "-r" region_refs
      ";u" usage_domain "-u" usage_refs]
    verb
    #["!" antonym "@" hypernym "~" hyponym "*" entails ">" causes
      "^" seealso "$" verbgroup "+" derivations
      ";c" topic_domain ";r" region_domain ";u" usage_domain]
    adjective
    #["!" antonym "&" similar "<" participle "\\" pertainym 
      "=" attribute "^" seealso 
      ";c" topic_domain ";r" region_domain ";u" usage_domain]
    adverb
    #["!" antonym "\\" adjective 
      ";c" topic_domain ";r" region_domain ";u" usage_domain]])

(define (decnum string) (string->number string 10))
(define (hexnum string) (string->number string 16))

(define (line->synset line)
  (let* ((glosspos (position #\| line))
	 (vec (->vector (textslice (slice line 0 glosspos) '(isspace+) #f)))
	 (n-words (hexnum (elt vec 3)))
	 (wordvec (slice vec 4 (+ 4 (* n-words 2))))
	 (type (try (get typecodes (elt vec 2)) (elt vec 2)))
	 (ptrmap (get ptrcodes type))
	 (wordforms (make-vector n-words)))
    (dotimes (i n-words)
      (vector-set! wordforms i 
		   (cons (string-subst (elt wordvec (* 2 i)) "_" " ")
			 (hexnum (elt wordvec (1+ (* 2 i)))))))
    (let ((info
	   (frame-create #f
	     'synset (decnum (elt vec 0))
	     'fileno (decnum (elt vec 1))
	     'sensecat (elt sensecats (decnum (elt vec 1)))
	     'type type 'n-words n-words
	     'wordforms wordforms 'words (car (elts wordforms))
	     'gloss (tryif glosspos (stdspace (slice line (1+ glosspos))))))
	  (n-ptrs (decnum (elt vec (+ 4 (* n-words 2)))))
	  (ptroff (+ 5 (* n-words 2))))
      (dotimes (i n-ptrs)
	(let* ((ptr (slice vec (+ ptroff (* i 4)) (+ 4 ptroff (* i 4))))
	       (relation (get ptrmap (elt ptr 0)))
	       (target (decnum (elt ptr 1)))
	       (target-pos (try (get typecodes (elt ptr 2)) (elt ptr 2)))
	       (lexinfo (elt ptr 3))
	       (source-wordno (hexnum (slice lexinfo 0 2)))
	       (target-wordno (hexnum (slice lexinfo 2))))
	  (add! info 'pointers 
		(frame-create #f
		  'code (elt ptr 0) 'relation relation
		  'type target-pos 'synset target
		  'source (tryif (> source-wordno 0) (car (elt wordforms (1- source-wordno))))
		  'sourceno (tryif (> source-wordno 0) source-wordno)
		  'targetno (tryif (> target-wordno 0) target-wordno)
		  'targetref
		  (tryif wordnet-debug (?? 'synsets `(version ,target ,target-pos)))))))
      (when (> (length vec) (+ ptroff (* 4 n-ptrs)))
	(let* ((n-frames (decnum (elt vec (+ ptroff (* 4 n-ptrs)))))
	       (verb-frames (slice vec (+ 1 ptroff (* 4 n-ptrs)))))
	  (dotimes (i n-frames)
	    (let* ((fnum (decnum (elt verb-frames (+ 1 (* i 3)))))
		   (wnum (hexnum (elt verb-frames (+ 2 (* i 3)))))
		   (vframe (try (find-frames wordnet.index 'vframenum fnum) fnum)))
	      (if (zero? wnum)
		  (add! info 'vframes vframe)
		  (add! info 'vframes 
			(cons (elt wordforms (-1+ wnum)) vframe)))))))
      info)))

(define (link-synset synset)
  (let ((oid (ref-synset (get synset 'synset) (get synset 'type))))
    (when (exists? oid) (store! synset 'oid oid)))
  (do-choices (ptr (get synset 'pointers))
    (add! synset (get ptr 'relation)
	  (ref-synset (get ptr 'synset) (get ptr 'type))))
  synset)

;;;; Reading synsets from disk

(define (wn/open dir)
  `#[noun ,(open-input-file (mkpath dir "dict/data.noun"))
     verb ,(open-input-file (mkpath dir "dict/data.verb"))
     adjective ,(open-input-file (mkpath dir "dict/data.adj"))
     adverb ,(open-input-file (mkpath dir "dict/data.adv"))])

(define (wn/get version synsetid type)
  (let ((f (get version type)))
    (setpos f synsetid)
    (line->synset (getline f))))

(module-export! '{wn/open wn/get})

;;;; Mapping synset descriptions into existing BRICO concepts

(define (ref-synset num type (version wnrelease))
  (let* ((frame (find-frames wordnet.index
		  'synsets `(,version ,num ,type))))
    (when (fail? frame)
      (set! frame 
	(frame-create brico.pool
	  'type type 'synsets `(,version ,num ,type)))
      (lognotice |NewSynsetRef| type " " num)
      (index-frame {wordnet.index core.index} frame 'type type)
      (index-frame wordnet.index frame 'synsets))
    frame))

(define (import-synset info temp.index done.set)
  (let* ((type (intersection (get info 'type) '{noun verb adjective adverb}))
	 (existing (find-frames wordnet.index
		     'synsets `(,wnrelease ,(get info 'synset) ,type)))
	 (frame (try existing
		     (frame-create brico.pool
		       'type (get info 'type)
		       'synsets `(,wnrelease ,(get info 'synset) ,type)))))
    (store! frame '%id
	    (cons (try (get info 'sensecat) wnrelease)
		  (choice->list (get info 'words))))
    (add! frame 'source wordnet-release)
    (hashset-add! done.set frame)
    (store! frame 'words (get info 'words))
    (store! frame 'gloss (get info 'gloss))
    (store! frame 'sensecat (get info 'sensecat))
    (store! frame 'ranked (map car (get info 'wordforms)))
    (store! frame 'n-words (get info 'n-words))
    (drop!  frame (get (get info 'pointers) 'relation))
    (drop!  frame '{hypernym hyponym})
    (doseq (word.rank (get info 'wordforms) i)
      (let ((existing (find-frames wordforms.index 
			'of frame 'sensenum (1+ i)
			'word (car word.rank))))
	(when (exists? existing) 
	  (add! existing 'source wordnet-release)
	  (store! existing 'rank 0))
	(if (zero? (cdr word.rank))
	    (add! frame 'norms (car word.rank))
	    (try existing
		 (let ((form (get-wordform frame (1+ i) (car word.rank))))
		   (store! form 'rank (cdr word.rank))
		   form)))))
    (do-choices (ptr (get info 'pointers))
      ;; Pointers can go synset to synset or synset.word to synset.word
      ;;  This (and get-wordform, below) is where we sort all that out.
      (let* ((relation (get ptr 'relation))
	     (source (if (test ptr 'sourceno)
			 (get-wordform frame (get ptr 'sourceno) (get ptr 'source))
			 frame))
	     (target-synset (ref-synset (get ptr 'synset) (get ptr 'type)))
	     (target (if (test ptr 'targetno)
			 (tryif (get done.set target-synset)
			   (get-wordform target-synset (get ptr 'targetno)))
			 target-synset)))
	(cond ((exists? target) (add! frame relation target))
	      (else (store! ptr 'sourceword source)
		    (store! ptr 'source frame)
		    (store! ptr 'target target-synset)
		    (add! frame '%wordlinks ptr)
		    (index-frame temp.index frame 'has '%wordlinks)))))
    (when wordnet.adjunct
      (store! frame '%%wordnet info)
      (index-frame temp.index frame 'has '%%wordnet))
    (when (fail? existing)
      (logwarn |NewSynset| frame)
      (index-frame core.index frame 'type)
      (index-frame wordnet.index frame 'synsets))))

(define (get-wordform meaning sensenum (word))
  (default! word (elt (get meaning 'ranked) (-1+ sensenum)))
  (unless (equal? word (elt (get meaning 'ranked) (-1+ sensenum)))
    (error |InconsistentSenseNum| get-wordform 
	   "Given " (write word) " as sense #" sensenum " of " meaning ", "
	   "but sense # " sensenum " is actually " 
	   (write (elt (get meaning 'ranked) (-1+ sensenum)))))
  (try (find-frames wordforms.index
	 'of meaning 'sensenum sensenum
	 'word word)
       (let ((form (frame-create brico.pool
		     'type 'wordform 'of meaning 'word word
		     'source wordnet-release
		     'sensenum sensenum)))
	 (add! meaning 'wordforms form)
	 (index-frame wordforms.index form '{type sensenum word of})
	 form)))

(define (finish-import temp.index done.set)
  (do-choices (frame (find-frames temp.index 'has '%wordlinks))
    (do-choices (ptr (get frame '%wordlinks))
      (add! (try (get ptr 'sourceword) (get ptr 'source))
	(get ptr 'relation)
	(get-wordform (get ptr 'target) (get ptr 'targetno))))
    (drop! frame '%wordlinks))
  (do-choices (frame (find-frames temp.index 'has '%%wordnet))
    (let ((info (get frame '%%wordnet)))
      (store! frame '%wordnet (link-synset info))
      (drop! frame '%%wordnet))))

;;; Fixups

(define (fix-wordform wf)
  (unless (test wf 'sensenum)
    (store! wf 'sensenum
	    (or (position (get wf 'word) (get (get wf 'of) 'ranked)) {}))
    (index-frame wordforms.index wf 'sensenum)))

(define (fix-sensenum wf (num))
  (set! num (position (get wf 'word) (get (get wf 'of) 'ranked)))
  (when (exists? num)
    (if num
	(store! wf 'sensenum (1+ num))
	(drop! wf 'sensenum))))

(define (fix-wnsplit frame)
  (store! frame 'wnsplit 
	  (for-choices (split (get frame 'wnsplit))
	    (try (find-frames wordnet.index 'synsets split)
		 split))))

;;;; Reading synset data files

(define (dataline in (line))
  (default! line (getline in))
  (if (fail? line) #eof
      (if (has-prefix line " ")
	  (dataline in)
	  line)))

(define (read-synset file off)
  (let ((in (open-input-file file)))
    (setpos! in off)
    (line->synset (getline in))))

(define (import-synsets file (temp.index (make-hashtable)) (done.set (make-hashset)))
  (let* ((in (open-input-file file))
	 (line (dataline in)))
    (while (and line (not (eq? line #eof)))
      (import-synset (line->synset line) temp.index done.set)
      (set! line (dataline in)))))

;;;; The default order

(comment
 (link-release! "dict/index.sense" 'wn31)
 (when (exists? (check-release-links 'wn31))
   (error |InconsistentLinks|))
 (import-synsets "dict/data.noun")
 (import-synsets "dict/data.verb")
 (import-synsets "dict/data.adj")
 (import-synsets "dict/data.adv")
 (fix-wordform (difference
		(find-frames wordnet.index 
		  'type 'wordform)
		(find-frames wordnet.index 
		  'has 'sensenum)))
 (fix-wnsplit (find-frames wordnet.index 'has 'wnsplit)))

;;; Various old code used to set up the database

;;; How verb frames were registered
#|
(doseq (line (remove "" (segment (filestring "vframes.text") "\n")))
  (let* ((pos (position #\Space line))
	 (num (string->number (trim-spaces (slice line 0 pos))))
	 (text (trim-spaces (slice line pos)))
	 (vframe (find-frames wordnet.index 'text text)))
    (if (fail? vframe) (logwarn |NoMap| text)
	(begin (store! vframe 'vframenum num) 
	  (index-frame wordnet.index vframe 'vframenum)))))
|#

;;; This is the code which was used to fix the sense-keys based on the
;;; wn1.6 index.sense file.
#|
(define (sense->synset sense) (find-frames {wordnet.index sensekeys.index} '%wn16 (get sense 'synset) 'sense-keys (get sense 'key)))
(define sensekeys.table (make-hashtable))
(define wn16.senses (read-sense-index "wordnet/WordNet-1.6/dict/index.sense"))
(do-choices (sense wn16.senses)
  (let ((synset (sense->synset sense)))
    (if (fail? synset)
	(logwarn |NoSynset| "For " sense)
	(if (ambiguous? synset)
	    (logwarn |AmbigSynset| sense " ==> " synset)
	    (add! sensekeys.table synset (get sense 'key))))))
(do-choices (key (getkeys sensekeys.table))
  (store! key 'sensekeys (get sensekeys.table key))
  (index-frame wordnet.index key 'sensekeys))
|#

;;; Creating a sensekey index when you don't have one
#|
(define sense-keys.index
  (db/ref (mkpath brico-dir "sensekeys.index")
	  #[indextype hashindex size #m2mib 
	    keyslot sensekeys
	    register #t create #t]))

(define (index-sense-keys)
  (do-choices (f (?? 'has 'sense-keys))
    (add! sense-keys.index (cdr (get f 'sense-keys)) f)))

(unless (exists? (find-frames sense-keys.index 'sense-keys "flutter%1:04:00::"))
  (logwarn |IndexingSenseKeys| "Generating sense-key index")
  (optimize! index-sense-keys)
  (index-sense-keys))
|#

