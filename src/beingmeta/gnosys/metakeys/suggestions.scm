(in-module 'gnosys/metakeys/suggestions)

(use-module '{brico metakeys metakeys/disambiguate})

(module-export! 'get-suggestions)

;;; Corpus-based disambiguation

(define (ilog n base)
  (do ((i 1 (1+ i))
       (e base (* base e)))
      ((> e n) i)))

(define (get-corpus-scores index concepts images)
  (let ((corpus-scores (make-hashtable 65536)))
    (ipeval (find-frames index @?gn/concepts (get concepts @?genls*)))
    (hashtable-increment!
     corpus-scores (find-frames index @?gn/concepts concepts))
    (hashtable-increment!
     corpus-scores
     (find-frames index @?gn/concepts (get concepts @?genls*)))
    corpus-scores))

(define (get-corpus-samples index concept scores)
  (pick-n (largest (find-frames index @?gn/concepts concept)
		   scores)
	  3))
(define (get-corpus-samples* index concept scores)
  (pick-n (largest (find-frames index @?gn/concepts (get concept @?genls*))
		   scores)
	  3))

(define (context-weight concept index images)
  (let* ((all (find-frames index @?gn/concepts concept))
	 (common (intersection images all)))
    (/ (* 1.0 (choice-size common)) (choice-size all))))

(define (get-suggestions index concepts images)
  (let* ((corpus-scores (get-corpus-scores index (qc concepts) (qc images)))
	 (best-images (hashtable-max corpus-scores))
	 (candidates (get (pick-n (hashtable-max corpus-scores) 5)
			  @?gn/xconcepts)))
    (reverse
     (sorted (difference candidates concepts)
	     (lambda (x) (context-weight x index (qc best-images)))))))

(define (get-meaning-scores index meaning corpus-scores)
  (let ((instances (find-frames index @?gn/concepts meaning))
	(total 0))
    (do-choices (instance instances)
      (set! total (+ total (get corpus-scores instance))))
    (if (zero? total) total
	(/ total (choice-size instances)))))


