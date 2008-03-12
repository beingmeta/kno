(in-module 'ctt)

;;; This module provides for targeted calltracking (as opposed to the
;;; general calltracking provided by the interpreter)

(use-module 'logger)

(module-export! '{ctt cttsimple cttdatum cttreport cttsummary})

;;; Configuration

(define ctt-loglevel %notice!)

(define (config-ctt/loglevel var (value))
  (if (bound? value)
      (if (number? value)
	  (set! ctt-loglevel value)
	  (set! ctt-loglevel (getloglevel value)))
      ctt-loglevel))
(config-def! 'ctt/loglevel config-ctt/loglevel)

(define ctt-sumfreq 8)

(define (config-ctt/sumfreq var (value))
  (if (bound? value)
      (if (number? value)
	  (set! ctt-sumfreq value)
	  (error "loglevel not a number"))
      ctt-sumfreq))
(config-def! 'ctt/sumfreq config-ctt/sumfreq)

(define %volatile '{%ctt-loglevel %ctt-sumfreq})

;;; CTT state info

(define ctt-state (make-hashtable))

(defslambda (get-ctt-state label)
  (try (get ctt-state label)
       (let ((state (cons 0 (make-vector 64 0))))
	 (store! ctt-state label state)
	 state)))

;;; Datum functions

(defslambda (cttsumdata state start end (vec #f))
  (set! vec (cdr state))
  (set-car! state (1+ (car state)))
  (dotimes (i (min (length vec) (length start) (length end)))
    (vector-set! vec i (+ (elt vec i) (- (elt end i) (elt start i))))))

(define (cttdatum start (end (ct/sense)) (label #f) (state #f))
  (when label
    (unless state
      (set! state (try (get ctt-state label) (get-ctt-state label))))
    (cttsumdata state start end))
  (logif+ #t ctt-loglevel
	  label "[" (car state) "]: "
	  (let ((sensors (ct/sensors)))
	    (dotimes (i (min (length sensors)  (length start) (length end)))
	      (printout (if (> i 0) "; ") (elt sensors i) "=" (- (elt end i) (elt start i))))))
  (when (zero? (remainder (car state) ctt-sumfreq))
    (logif+ #t ctt-loglevel
	    label "[" (car state) "]: "
	    (let ((sensors (ct/sensors)))
	      (dotimes (i (min (length sensors)  (length start) (length end)))
		(printout (if (> i 0) "; ") (elt sensors i) "=" (elt (cdr state) i)))))))

(define (cttreport start (end (ct/sense)) (label #f))
  (logif+ #t ctt-loglevel
	  label ": "
	  (let ((sensors (ct/sensors)))
	    (dotimes (i (min (length sensors)  (length start) (length end)))
	      (printout (if (> i 0) "; ") (elt sensors i) "="
			(- (elt end i) (elt start i)))))))

;;; Top level macros

(define ctt
  (macro expr
    (let ((name (if (> (length expr) 2) (third expr)
		    (first expr)))
	  (body-expr (second expr)))
      `(let ((_ctt_start (ct/sense)))
	 (prog1 ,body-expr (,cttdatum (%watch _ctt_start) (ct/sense) ',name))))))

(define cttsimple
  (macro expr
    (let ((name (if (> (length expr) 2) (third expr)
		    (first expr)))
	  (body-expr (second expr)))
      `(let ((_ctt_start (ct/sense)))
	 (prog1 ,body-expr (,cttreport _ctt_start (ct/sense) ',name))))))

;;; Summary functions

(defambda (cttsummary (label #f) (sortfn (lambda (x) (first (cdr (get ctt-state x))))))
  (doseq (label (rsorted (or label (getkeys ctt-state)) sortfn))
    (let* ((state (get ctt-state label))
	   (sensors (ct/sensors))
	   (n (car state))
	   (vec (cdr state)))
      (lineout ";; " label "/sum[" (car state) " calls] "
	       (dotimes (i (min (length vec) (length sensors)))
		 (printout (if (> i 0) (if (zero? (remainder i 5)) ";\n;;\t" "; "))
			   (elt sensors i) "=" (elt vec i))))
      (lineout ";; " label "/mean[" (car state) " calls] "
	       (dotimes (i (min (length vec) (length sensors)))
		 (printout (if (> i 0) (if (zero? (remainder i 5)) ";\n;;\t" "; "))
			   (elt sensors i)  "=" (/~ (elt vec i) n)))))))


