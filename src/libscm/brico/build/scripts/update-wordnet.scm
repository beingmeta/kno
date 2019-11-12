(use-module '{texttools})
(use-module '{logger varconfig optimize stringfmts})

(config! 'cachelevel 2)
(config! 'dbloglevel %info%)
(config! 'bricosource "./current/")

(use-module '{knodb})
(use-module '{brico})
(use-module '{brico/indexing brico/build/wordnet})

(config! 'brico:wordnet 'wn31)

(define en @1/2c1c7)
(define wordnet-dir (abspath "wordnet/WordNet-3.1/"))
(varconfig! wordnet wordnet-dir)

(define %loglevel %warn%)

(define global-index #f)
(define global-done #f)

(define (main)
  (poolctl brico.pool 'readonly #f)
  (when wordnet.adjunct (poolctl wordnet.adjunct 'readonly #f))
  (indexctl core.index 'readonly #f)
  (indexctl wordnet.index 'readonly #f)
  (indexctl wordforms.index 'readonly #f)
  (when (exists? (check-release-links 'wn30))
    (error |InconsistentLinks|))
  (link-release! (mkpath wordnet-dir "dict/index.sense") 'wn31 #f)
  (let ((temp.index (make-hashtable)) (done.set (make-hashset)))
    (set! global-index temp.index) (set! global-done done.set)
    (import-synsets (mkpath wordnet-dir "dict/data.noun") temp.index done.set)
    (import-synsets (mkpath wordnet-dir "dict/data.verb") temp.index done.set)
    (import-synsets (mkpath wordnet-dir "dict/data.adj") temp.index done.set)
    (import-synsets (mkpath wordnet-dir "dict/data.adv") temp.index done.set)
    (finish-import temp.index done.set))
  (commit))

(define (legacy-fixes)
  (fix-wordform (difference
		 (find-frames wordnet.index 'type 'wordform)
		 (find-frames wordnet.index 'has 'sensenum)))
  (fix-wnsplit (find-frames wordnet.index 'has 'wnsplit)))

(when (config 'optimize #t)
  (optimize! '{brico brico/indexing brico/build/wordnet}))
