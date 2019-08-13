;;; -*- Mode: Scheme; -*-

(use-module '{logger stringfmts texttools optimize})

(define (randfn max (min #f))
  (if (or (not min) (zero? min))
      (lambda () (random max))
      (lambda () (+ min (random (- max min))))))

(define rand64K (randfn 65536))
(define rand256K (randfn (* 4 65536)))

(define baseoids
  (let ((baseoids {}))
    (dotimes (i (config 'nbaseoids 16))
      (set+! baseoids (make-oid (random (* 65536 65536)) 0)))
    baseoids))

(define offrange (* 1024 1024))

(define oidfns {})

(defambda (randoidfn (baseoids baseoids) (offrange offrange))
  (let ((f (lambda () (oid+ (pick-one baseoids) (random offrange)))))
    (config! 'profiled f)
    (set+! oidfns f)
    f))

(define (populate-data-file file nitems genfn)
  (let ((port (open-output-file file)))
    (fileout port
      (dotimes (i (if (number? nitems) nitems (nitems)))
	(lineout (genfn))))))

(define (populate-data prefix nfiles nitems (genfn rand256k))
  (unless (file-directory? (dirname prefix)) (mkdirs (dirname prefix)))
  (dotimes (fileno nfiles)
    (populate-data-file (glom prefix (padnum fileno 4)) nitems genfn)))
(config! 'profiled {populate-data populate-data-file})

(define (read-data-file file)
  (let* ((in (open-input-file file))
	 (items {})
	 (item (read in)))
    (until (eof-object? item)
      (set+! items item)
      (set! item (read in)))
    items))

(define (load-data prefix)
  (let ((files (pick (getfiles (dirname prefix))
		 basename has-prefix (basename prefix)))
	(table (make-hashtable)))
    (do-choices (file files)
      (store! table file (read-data-file file)))
    table))

(define (closest-pairs data)
  (let ((files (getkeys data))
	(best {})
	(best-score #f))
    (do-choices (file files)
      (do-choices (other (difference files file))
	(let ((score (|| (intersection (get data file) (get data other)))))
	  (cond ((not best-score)
		 (set! best (cons file other))
		 (set! best-score score))
		((= score best-score)
		 (set+! best (cons file other)))
		((> score best-score)
		 (set! best (cons file other))
		 (set! best-score score))))))
    [count (|| best) best best score best-score]))

(when (config 'optimized #t)
  (optimize!))

