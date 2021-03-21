;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 beingmeta, llc.

;;; Optimizing code structures for the interpreter, including
;;;  use of constant OPCODEs and relative lexical references
(in-module 'optimize)

(define-init standard-modules
  (choice (get (get-module 'kno/reflect) 'getmodules)
	  'scheme 'xscheme 'fileio 'history 'logger))
(define-init check-module-usage #f)

;; This module optimizes an expression or procedure by replacing
;; certain variable references with their values directly, which
;; avoids many environment lookups.  The trick is to not replace
;; anything which will change and so produce an equivalent expression
;; or function which just runs faster.

(use-module 'kno/reflect)
(use-module 'varconfig)
(use-module 'logger)

(module-proxy! 'kno/reflect
	       lambda-entry lambda-body  lambda-args lambda-env
	       procedure-fileinfo procedure-filename
	       reflect/attribs reflect/get)

(define-init %loglevel %warn%)

;; OPTLEVEL interpretations
;; 0: no optimization
;; 1: fcnrefs + opcodes
;; 2: substs + lexrefs
;; 3: rewrites
;; 4: bind opcodes
;; 5: no sourcerefs
(define-init optlevel 4)
(varconfig! optimize:level optlevel)
(varconfig! optlevel optlevel)

(define-init module-warnings (make-hashtable))
(define-init all-warnings #f)

(define-init fcnrefs-default {})
(define-init pfcnrefs-default {})
(define-init opcodes-default {})
(define-init bindops-default {})
(define-init lexrefs-default {})
(define-init substs-default {})
(define-init rewrite-default {})
(define-init aliasfns-default {})
(define-init aliasprims-default {})

(varconfig! optimize:fcnrefs fcnrefs-default)
(varconfig! optimize:opcodes opcodes-default)
(varconfig! optimize:bindops bindops-default)
(varconfig! optimize:substs  substs-default)
(varconfig! optimize:lexrefs lexrefs-default)
(varconfig! optimize:rewrite rewrite-default)
(varconfig! optimize:aliasfns aliasfns-default)
(varconfig! optimize:aliasprims aliasprims-default)

(define-init persist-default #f)
(varconfig! optimize:persist persist-default)

(define-init keep-source-default #t)
(varconfig! optimize:keepsource keep-source-default)

(define-init use-apply-opcodes #f)
(varconfig! optimize:applyops use-apply-opcodes)

(define (optmode-macro optname thresh varname)
  (macro expr
    `(getopt ,(cadr expr) ',optname
	     (try ,varname (if (>= ,thresh 0)
			       (>= (getopt ,(cadr expr) 'optlevel optlevel)
				   ,thresh)
			       (>= (getopt ,(cadr expr) 'optlevel optlevel)
				   (- ,thresh)))))))
(define optmode
  (macro expr
    (let ((optname (get-arg expr 1))
	  (opthresh (get-arg expr 2))
	  (optvar (get-arg expr 3)))
      `(optmode-macro ',optname ,opthresh ',optvar))))

(define use-opcodes? (optmode opcodes 2 opcodes-default))
(define use-pfcnrefs? (optmode pfcnrefs 2 pfcnrefs-default))
(define use-fcnrefs? (optmode fcnrefs 3 fcnrefs-default))
(define use-bindops? (optmode bindops 4 bindops-default))
(define use-substs? (optmode substs 2 substs-default))
(define use-lexrefs? (optmode lexrefs 2 lexrefs-default))
(define rewrite? (optmode 'rewrite 3 rewrite-default))
(define aliasprims? (optmode 'aliasprims 2 aliasprims-default))
(define aliasfns? (optmode 'aliasfns 4 aliasfns-default))
(define keep-source? (optmode keepsource 2 keep-source-default))

(define use-consblock #f)
(varconfig! optimize:consblock use-consblock)

;;; Converts optimized bodies with a single element into just that element; e.g.
;;;  the body ((#OP_PLUS x . y)) becomes just (#OP_PLUS x . y) which eval_body
;;;  processes correctly
(define simplify-bodies #f)
(varconfig! optimize:simplify-bodies simplify-bodies)

(define keep-preambles #f)
(varconfig! optimize:preambles keep-preambles)

;;; Controls whether optimization warnings are emitted in real time
;;; (when encountered)
(define-init optwarn #t)
(define optwarn-config
  (slambda (var (val 'unbound))
    (cond ((eq? val 'unbound) optwarn)
	  (val (set! optwarn #t))
	  (else (set! optwarn #f)))))
(config-def! 'optwarn optwarn-config)
(defslambda (codewarning warning)
  (debug%watch "CODEWARNING" warning)
  (thread/set! 'codewarnings (choice warning (thread/get 'codewarnings))))

(define (annotate optimized source opts)
  (if (keep-source? opts)
      (if (and (pair? optimized)
	       (eq? (car optimized) sourceref-opcode)
	       (pair? (cdr optimized))
	       (eq? source (cadr optimized)))
	  optimized
	  (cons* sourceref-opcode source (qc optimized)))
      optimized))

(define (optimizable? arg)
  (and (not (fcnid? arg)) (compound-procedure? arg)))

(define (needs-eval? x)
  (cond ((fail? x) #f)
	((ambiguous? x) (exists needs-eval? x))
	((or (symbol? x) (pair? x) (schemap? x)) #t)
	(else #f)))

(define (fcn/lookup table fn)
  (try (get table fn)
       (tryif (and (procedure-name fn) (get-module fn))
	 (get table (cons (downcase (procedure-name fn)) (get-module fn))))))

(define dont-touch-decls '{%unoptimized %volatile %nosubst})

;;; What we export

(module-export! '{optimize! optimized optimized? use+
		  optimize-procedure! optimize-module!
		  reoptimize! optimize-bindings!
		  optimize/list-warnings
		  optimize/count-warnings
		  deoptimize! 
		  deoptimize-procedure!
		  deoptimize-module!
		  deoptimize-bindings!})

;;; Tables

(define-init static-refs (make-hashtable))

(define-init fcnids (make-hashtable))

(define %volatile '{optwarn useopcodes %loglevel})

(varconfig! optimize:checkusage check-module-usage)

;;; Utility functions

(define-init special-form-optimizers (make-hashtable))
(define-init procedure-optimizers (make-hashtable))

(define (arglist->vars arglist)
  (if (pair? arglist)
      (cons (if (pair? (car arglist))
		(car (car arglist))
		(car arglist))
	    (arglist->vars (cdr arglist)))
      (if (null? arglist)
	  '()
	  (list arglist))))
(define (get-lexref sym bindlist (base 0))
  (if (null? bindlist) #f
      (if (pair? (car bindlist))
	  (let ((pos (position sym (car bindlist))))
	    (if pos (%lexref base pos)
		(get-lexref sym (cdr bindlist) (1+ base))))
	  (if (null? (car bindlist))
	      (get-lexref sym (cdr bindlist) (1+ base))
	      (if (eq? (car bindlist) sym) sym
		  (get-lexref sym (cdr bindlist) base))))))

(define (isbound? var bindlist)
  (if (null? bindlist) #f
      (if (pair? (car bindlist))
	  (position var (car bindlist))
	  (if (null? (car bindlist))
	      (isbound? var (cdr bindlist))
	      (or (eq? (car bindlist) var)
		  (isbound? var (cdr bindlist)))))))

;;; Converting non-lexical function references

(define (fcnref value sym env opts (from))
  (default! from (and (symbol? sym) (wherefrom sym env)))
  (cond ((not (or (applicable? value) (special-form? value))) sym)
	((not (cons? value)) sym)
	((or (and from (test from dont-touch-decls sym))
	     (testopt opts dont-touch-decls sym))
	 sym)
	((and sym from (module? from)
	      (or (special-form? value) (primitive? value))
	      (or (aliasprims? opts) (aliasfns? opts)))
	 value)
	((and sym from (module? from)
	      (or (special-form? value) (primitive? value))
	      (use-fcnrefs? opts))
	 (get-fcnid sym from value))
	((and sym from (module? from) (applicable? value)
	      (aliasfns? opts)
	      (or (%test env '%constants sym)
		  (test from '%constants sym)
		  (testopt opts '%constants sym)))
	 value)
	((and sym from (module? from) (applicable? value)
	      (getopt opts 'aliasfns aliasfns-default)
	      (use-fcnrefs? opts))
	 (get-fcnid sym from value))
	((not (symbol? sym)) value)
	((or (%test env '%constants sym)
	     (and from (%test from '%constants sym))
	     (if (or (special-form? value) (primitive? value))
		 (or (getopt opts 'aliasprims aliasprims-default)
		     (getopt opts 'aliasfns aliasfns-default))
		 (getopt opts 'aliasfns aliasfns-default)))
	 value)
	((or (not from) (not (test from sym value))) sym)
	((or (not (use-fcnrefs? opts))
	     (test env '%volatile sym)
	     (test from '%volatile sym))
	 `(,(try (tryif (use-opcodes? opts) #OP_SYMREF)
		 (tryif (use-fcnrefs? opts)
		   (force-fcnid %modref))
		 %modref)
	   ,(or from env) ,sym))
	((and (test from '%fcnids) (fail? (get from '%fcnids))) sym)
	((module? from) (get-fcnid sym from value))
	(else sym)))

;;; FCNIDs

(define-init get-fcnids
  (slambda (env)
    (try (get env '%fcnids)
	 (let ((table (make-hashtable)))
	   (store! env '%fcnids table)
	   table))))

(define new-fcnid
  (slambda (symbol env value internal)
    (if (not (symbol? symbol))
	(try (get fcnids value)
	     (let ((fcnid (fcnid/register value)))
	       (store! fcnids symbol fcnid)
	       fcnid))
	(try (if internal
		 (get (get internal '%fcnids) symbol)
		 (get fcnids (cons env symbol)))
	     (let ((fcnid (fcnid/register value)))
	       (if internal
		   (store! (try (get internal '%fcnids) (get-fcnids internal))
		       symbol fcnid)
		   (store! fcnids (cons env symbol) fcnid))
	       fcnid)))))

(define (probe-fcnid symbol env (internal))
  (default! internal (module-bindings env))
  (if internal
      (try (get (get internal '%fcnids) symbol)
	   (get (get-fcnids internal) symbol))
      (get fcnids (cons env symbol))))

(define (get-fcnid symbol env value (internal) (update #f))
  (default! internal (module-bindings env))
  (get-fcnid-internal symbol env value internal update))
(define (get-fcnid-internal symbol env value internal update)
  (if (cons? value)
      (begin
	(default! internal (module-bindings env))
	(let* ((known (probe-fcnid symbol env internal))
	       (fcnid (try known (new-fcnid symbol env value internal))))
	  (when (and update (exists? known)
		     (or (fail? (fcnid/ref fcnid)) 
			 (not (eq? (fcnid/ref fcnid) value))))
	    (fcnid/set! fcnid value))
	  fcnid))
      value))
(define (update-fcnid! symbol env value (internal))
  (default! internal (module-bindings env))
  (get-fcnid symbol env value internal #t))

(define (force-fcnid value)
  (if (cons? value)
      (try (get fcnids value)
	   (new-fcnid value #f value #f))
      value))

(define (static-ref ref)
  (if (static? ref) ref
      (try (get static-refs ref)
	   (make-static-ref ref))))

(defslambda (make-static-ref ref)
  (try (get static-refs ref)
       (let ((copy (static-copy ref)))
	 (if (eq? copy ref) ref
	     (begin (store! static-refs ref copy)
	       copy)))))

;;; Opcode mapping

;;; When opcodes are introduced, the idea is that a given primitive 
;;;  may map into an opcode to be zippily executed in the evaluator.
;;; The opcode-map is a table for identifying the opcode for a given procedure.
;;;  It's keys are either primitive (presumably) procedures or pairs
;;;   of procedures and integers (number of subexpressions).  This allows
;;;   opcode substitution to be limited to simpler forms and allows opcode
;;;   execution to make assumptions about the number of arguments.  Note that
;;;   this means that this file needs to be synchronized with src/scheme/eval.c

(define-init opcodes1 (make-hashtable))
(define-init opcodes2 (make-hashtable))
(define-init opcodesn (make-hashtable))
(define-init predicate-typemap (make-hashtable))
(define-init numeric-opcodes (make-hashtable))
(define-init table-opcodes (make-hashtable))

(define-init opcode-map (make-hashtable))

(define (get-headop value head arity env bound opts)
  (if (use-opcodes? opts)
      (try (tryif length (get opcode-map (cons value arity)))
	   (tryif (and arity (procedure? value) (procedure-name value)) 
	     (get opcode-map (cons (procedure-name value) arity)))
	   (get opcode-map value)
	   (get opcode-map value)
	   (tryif (and (procedure? value) (procedure-name value)) 
	     (get opcode-map (procedure-name value)))
	   (fcnref value head env opts))
      (fcnref value head env opts)))

(define name2op
  (and (bound? name->opcode) name->opcode))

(define (def-opcode prim code (n-args #f))
  (if n-args
      (add! opcode-map (cons prim n-args) code)
      (add! opcode-map prim code)))

(define branch-opcode #OP_BRANCH)
(define not-opcode    #OP_NOT)
(define until-opcode  #OP_UNTIL)
(define begin-opcode  #OP_BEGIN)
(define quote-opcode  #OP_QUOTE)
(define assign-opcode #OP_ASSIGN)
(define symref-opcode #OP_SYMREF)
(define bind-opcode   #OP_BIND)
(define void-opcode   #OP_VOID)
(define and-opcode    #OP_AND)
(define or-opcode     #OP_OR)
(define try-opcode    #OP_TRY)
(define xref-opcode   #OP_XREF)
(define xpred-opcode   #OP_XPRED)
(define choiceref-opcode #OP_CHOICEREF)
(define break-opcode  #OP_BREAK)

(define sourceref-opcode #OP_SOURCEREF)

(define-init fncall-opcodes
  #(#OP_CALL0 #OP_CALL1 #OP_CALL2 #OP_CALL3 #OP_CALL4 #OP_CALL5 #OP_CALL6 #OP_CALL7
    #OP_CALL8 #OP_CALL9 #OP_CALL10 #OP_CALL11 #OP_CALL12 #OP_CALL13 #OP_CALL14 #OP_CALL15))

(store! opcodes1 SINGLETON  #OP_SINGLETON)
(store! opcodes1 CAR        #OP_CAR)
(store! opcodes1 CDR        #OP_CDR)
(store! opcodes1 LENGTH     #OP_LENGTH)
(store! opcodes1 QCHOICE    #OP_QCHOICE)
(store! opcodes1 CHOICE-SIZE #OP_CHOICESIZE)
(store! opcodes1 PICK-ONE    #OP_PICKONE)
(store! opcodes1 IFEXISTS    #OP_IFEXISTS)
(store! opcodes1 SOMETRUE    #OP_SOMETRUE)
(store! opcodes1 NOT         #OP_NOT)
(store! opcodes1 FALSE?      #OP_NOT)
(store! opcodes1 QUOTE       #OP_QUOTE)

(store! procedure-optimizers {1- -1+}
  (lambda (fn expr env bound opts)
    (tryif (= (length expr) 2)
      (cons* #OP_MINUS (optimize (cadr expr) env bound opts) 1))))
(store! procedure-optimizers 1+
  (lambda (fn expr env bound opts)
    (tryif (= (length expr) 2)
      (cons* #OP_PLUS (optimize (cadr expr) env bound opts) 1))))

(store! procedure-optimizers PICKOIDS
  (lambda (fn expr env bound opts)
    (tryif (= (length expr) 2)
      (cons* #OP_PICK_TYPE #oid_type (optimize (cadr expr) env bound opts)))))
(store! procedure-optimizers PICKNUMS
  (lambda (fn expr env bound opts)
    (tryif (= (length expr) 2)
      (cons* #OP_PICK_TYPE #number_type (optimize (cadr expr) env bound opts)))))
(store! procedure-optimizers PICKNUMS
  (lambda (fn expr env bound opts)
    (tryif (= (length expr) 2)
      (cons* #OP_PICK_TYPE #string_type (optimize (cadr expr) env bound opts)))))
(store! procedure-optimizers PICKMAPS
  (lambda (fn expr env bound opts)
    (tryif (= (length expr) 2)
      (cons* #OP_PICK_TYPE #keymap_type (optimize (cadr expr) env bound opts)))))

(store! predicate-typemap EMPTY?     #empty_type)
(store! predicate-typemap FAIL?      #empty_type)
(store! predicate-typemap AMBIGUOUS? #choice_type)
(store! predicate-typemap SINGLETON? #singleton_type)
(store! predicate-typemap EXISTS?    #exists_type)
(store! predicate-typemap NUMBER?    #number_type)
(store! predicate-typemap VECTOR?    #vector_type)
(store! predicate-typemap PAIR?      #pair_type)
(store! predicate-typemap SEQUENCE?  #sequence_type)
(store! predicate-typemap NULL?      (cons #OP_EQP '()))
(store! predicate-typemap STRING?    #string_type)
(store! predicate-typemap OID?       #oid_type)
(store! predicate-typemap SYMBOL?    #symbol_type)
(store! predicate-typemap FIXNUM?    #fixnum_type)
(store! predicate-typemap FLONUM?    #flonum_type)
(store! predicate-typemap EXACT?     #exact_type)
(store! predicate-typemap INEXACT?   #inexact_type)
(store! predicate-typemap POOL?      #pool_type)
(store! predicate-typemap INDEX?     #index_type)
(store! predicate-typemap SLOTID?    #slotid_type)
(store! predicate-typemap TABLE?     #table_type)
(store! predicate-typemap APPLICABLE?   #applicable_type)

(store! opcodes1 FIRST #OP_FIRST)
(store! opcodes1 SECOND #OP_SECOND)
(store! opcodes1 THIRD #OP_THIRD)
(store! opcodes1 CADR #OP_CADR)
(store! opcodes1 CDDR #OP_CDDR)
(store! opcodes1 CADDR #OP_CADDR)
(store! opcodes1 CDDDR #OP_CDDDR)
(store! opcodes1 ->NUMBER #OP_2NUMBER)

(store! opcodes1 ELTS #OP_ELTS)
(store! opcodes1 GETKEYS #OP_GETKEYS)
(store! opcodes1 GETVALUES #OP_GETVALUES)
(store! opcodes1 GETASSOCS #OP_GETASSOCS)

(store! opcodesN UNION #OP_UNION)
(store! opcodesN INTERSECTION #OP_INTERSECTION)
(store! opcodesN DIFFERENCE   #OP_DIFFERENCE)
(store! opcodesN BEGIN #OP_BEGIN)
(store! opcodesN TRY #OP_TRY)

(store! numeric-opcodes = #OP_NUMEQ)
(store! numeric-opcodes > #OP_GT)
(store! numeric-opcodes >= #OP_GTE)
(store! numeric-opcodes < #OP_LT)
(store! numeric-opcodes <= #OP_LTE)
(store! numeric-opcodes + #OP_PLUS)
(store! numeric-opcodes - #OP_MINUS)
(store! numeric-opcodes * #OP_MULT)
(store! numeric-opcodes /~ #OP_FLODIV)
(store! numeric-opcodes /  #OP_DIVIDE)

(store! opcodes2 IDENTICAL?   #OP_IDENTICALP)
(store! opcodes2 OVERLAPS?    #OP_OVERLAPSP)
(store! opcodes2 CONTAINS?    #OP_CONTAINSP)
(store! opcodes2 EQ?          #OP_EQP)
(store! opcodes2 EQV?         #OP_EQVP)
(store! opcodes2 EQUAL?       #OP_EQUALP)
(store! opcodes2 ELT          #OP_SEQELT)
(store! opcodes2 CONS         #OP_CONSPAIR)

(store! table-opcodes GET        #OP_GET)
(store! table-opcodes TEST       #OP_TEST)
(store! table-opcodes ASSERT!    #OP_ASSERT)
(store! table-opcodes ADD!       #OP_ADD)
(store! table-opcodes DROP!      #OP_DROP)
(store! table-opcodes RETRACT!   #OP_RETRACT)
(store! table-opcodes %GET       #OP_PGET)
(store! table-opcodes %TEST      #OP_PTEST)

;;; The core loop

(defambda (optimize expr env bound opts)
  (logdetail |Optimize| expr " given " bound)
  (cond ((and (ambiguous? expr) (use-opcodes? opts))
	 (if (needs-eval? (qc expr))
	     `(#OP_UNION ,@(forseq (each (choice->list expr))
			     (optimize each env bound opts)))
	     (cons #OP_QUOTE expr)))
	((ambiguous? expr)
	 (for-choices (each expr) 
	   (optimize each env bound opts)))
	((fail? expr) expr)
	((symbol? expr) (optimize-variable expr env bound opts))
	((and (schemap? expr) (needs-eval? expr))
	 (optimize-schemap expr env bound opts))
	((not (pair? expr)) expr)
	((or (pair? (car expr)) (ambiguous? (car expr)))
	 ;; If we can't determine a single head for an expression,
	 ;; just assume it's a function application and optimize it
	 ;; that way. In principle, we could optimize ambiguous heads
	 ;; like {+ -} but we won't do that for now.
	 (annotate
	  (optimize-call expr env bound opts)
	  expr opts))
	((and (symbol? (car expr)) 
	      (not (symbol-bound? (car expr) env))
	      (not (get-lexref (car expr) bound 0))
	      (not (test env '%nowarn (car expr))))
	 ;; This is the case where the head is a symbol which we can't
	 ;; resolve it.
	 (codewarning (cons* '|Unbound| (car expr) expr bound))
	 (when optwarn
	   (logwarn |UnboundVariable|
	     "The symbol " (car expr) " in " expr
	     " appears to be unbound given bindings "
	     (apply append bound)))
	 expr)
	(else (optimize-expr expr env bound opts))))

(define (eval-schemap? schemap (needs-eval #f))
  (do-choices (key (getkeys schemap))
    (when (needs-eval? (qc (get schemap key)))
      (set! needs-eval #t)
      (break)))
  needs-eval)

(define (optimize-schemap schemap env bound opts)
  (let* ((copy (deep-copy schemap))
	 (keys (getkeys copy)))
    (do-choices (key keys)
      (store! copy key (optimize (get copy key) env bound opts)))
    copy))

(define (do-optimize-exprs exprs env bound opts)
  (if (not (proper-list? exprs))
      (begin (codewarning (cons '|ImproperList| exprs))
	 (when optwarn
	   (logwarn |ImproperList|
	     "The arguments for the expression " context " are an improper list:\n "
	     ($pprint exprs))))
      (forseq (clause exprs)
	(if (or (pair? clause) (symbol? clause) (schemap? clause))
	    (optimize clause env bound opts)
	    clause))))

(define optimize-exprs
  (macro expr
    (let ((body-expr (get-arg expr 1))
	  (env-expr (get-arg expr 2 'env))
	  (bound-expr (get-arg expr 3 'bound))
	  (opts-expr (get-arg expr 4 'opts)))
      `(,do-optimize-exprs ,body-expr ,env-expr ,bound-expr ,opts-expr))))

(define (do-optimize-body body env bound opts)
  (if (not (proper-list? body))
      (begin (codewarning (cons '|ImproperBody| body))
	(when optwarn
	  (logwarn |ImproperListBody|
	    "The body is an improper list:\n "
	    (void (pprint exprs)))))
      (simplify-body
       (forseq (clause (strip-preamble body))
	 (if (or (pair? clause) (symbol? clause) (schemap? clause))
	     (optimize clause env bound opts)
	     clause)))))

(define (simplify-body body)
  (if (and simplify-bodies
	   (and (pair? body) (null? (cdr body))
		(pair? (car body)) (opcode? (caar body))))
      (car body)
      body))

(define (strip-preamble body)
  (if keep-preambles body
      (if (or (empty-list? body) (empty-list? (cdr body))) body
	  (let ((scan body) (decls #f))
	    (when (and (pair? scan) (schemap? (car scan)))
	      (set! decls (car scan))
	      (set! scan (cdr scan)))
	    (while (and (pair? scan) (pair? (cdr scan)) (string? (car scan)))
	      (set! scan (cdr scan)))
	    (if (and (pair? scan) (pair? (cdr scan)) (schemap? (car scan)))
		(cdr scan)
		scan)))))

(define optimize-body
  (macro expr
    (let ((body-expr (get-arg expr 1))
	  (env-expr (get-arg expr 2 'env))
	  (bound-expr (get-arg expr 3 'bound))
	  (opts-expr (get-arg expr 4 'opts)))
      `(,do-optimize-body ,body-expr ,env-expr ,bound-expr ,opts-expr))))

(defambda (make-fncall fn args env bound opts expr (len))
  (default! len (length args))
  (annotate
   (if (> len 15)
       (cons* #OP_CALLN len (qc fn)
	      (forseq (arg args) (optimize arg env bound opts)))
       (cons* (elt fncall-opcodes len) (qc fn)
	      (forseq (arg args) (optimize arg env bound opts))))
   expr opts))

(defambda (optimize-variable expr env bound opts)
  (let ((lexref (get-lexref expr bound 0))
	(use-opcodes (use-opcodes? opts)))
    (debug%watch "optimize-variable" expr lexref env bound)
    (if lexref
	(if (use-lexrefs? opts) lexref expr)
	(let* ((srcenv (wherefrom expr env))
	       (module (and srcenv (module? srcenv) srcenv))
	       (value (and srcenv (get srcenv expr))))
	  (debug%watch "OPTIMIZE/SYMBOL/module" 
	    expr module srcenv env bound optwarn)
	  ;; Add reference information
	  (when (and module (module? env))
	    (add! env '%symrefs expr)
	    (when (and module (table? module))
	      (add! env '%modrefs
		(pick (get module '%moduleid) symbol?))))
	  (cond ((not srcenv)
		 ;; This means the symbol can't be found
		 (unless (or (test env '%nowarn expr)
			     (testopt opts 'nowarn expr)
			     (testopt opts 'nowarn #t))
		   (codewarning (cons* 'UNBOUND expr bound))
		   (when env
		     (add! env '%warnings (cons* 'UNBOUND expr bound)))
		   (when (or optwarn (not env))
		     (logwarn |Unbound|
		       "The symbol " expr " appears to be unbound "
		       "given bindings " (apply append bound))))
		 expr)
		;; This is where the symbol isn't from a module, but
		;; we're not doing lexrefs, so we just keep the
		;; expression as a symbol
		((not module) expr)
		;; Several ways to disable optimization
		((or (%test srcenv dont-touch-decls expr)
		     (testopt opts dont-touch-decls expr))
		 expr)
		;; If it's a primitive or special form, replace it
		;; with its value
		((and module (singleton? value)
		      (or (primitive? value) (applicable? value))
		      (use-fcnrefs? opts))
		 (fcnref value expr env opts))
		((and (singleton? value)
		      (or (primitive? value) (special-form? value)))
		 value)
		((%test srcenv '%constants expr)
		 ;; If it's a constant, replace it as well, but be
		 ;; careful to quote it if it contains anything which
		 ;; might be evaluated (symbols or pairs)
		 (if (and (singleton? value)
			  (not (exists {pair? symbol? schemap?} value)))
		     value
		     (if use-opcodes
			 (cons quote-opcode (qc value))
			 (list 'quote (qc value)))))
		;; TODO: add 'modrefs' which resolves module.var to a
		;; fcnref and uses that for the symbol
		(else `(,(try (tryif use-opcodes #OP_SYMREF)
			      (tryif (use-fcnrefs? opts)
				(force-fcnid %modref))
			      %modref)
			,module ,expr)))))))

(define (do-rewrite rewriter expr env bound opts)
  (onerror
      (optimize (rewriter expr) env bound opts)
      (lambda (ex)
	(logwarn |RewriteError| 
	  "Error rewriting " expr " with " rewriter)
	(logwarn |RewriteError| "Error rewriting " expr ": " ex)
	expr)))

(define (check-arguments value n-exprs expr)
  (when (and (procedure-min-arity value)
	     (< n-exprs (procedure-min-arity value)))
    (codewarning (list 'TOOFEWARGS expr value))
    (when optwarn
      (logwarn |TooFewArguments|
	"The call to " expr " provides too few arguments "
	"(" n-exprs ") for " value)))
  (when (and (procedure-arity value)
	     (> n-exprs (procedure-arity value)))
    (codewarning (list 'TOOMANYARGS expr value))
    (when optwarn
      (logwarn |TooManyArguments|
	"The call to " expr " provides too many "
	"arguments (" n-exprs ") for " value))))

(define (optimize-expr expr env bound opts)
  (let* ((head (get-arg expr 0))
	 (use-opcodes (use-opcodes? opts))
	 (n-exprs (-1+ (length expr)))
	 (headvalue (if (symbol? head)
			(or (get-lexref head bound 0)
			    (get env head))
			head))
	 (from (and (symbol? head)
		    (not (get-lexref head bound 0))
		    (wherefrom head env))))
    (detail%watch "optimize-expr" expr bound headvalue env head from)
    (debug%watch "optimize-expr" expr bound headvalue from)
    (when (and from (module? env))
      (add! env '%symrefs expr)
      (when (and from (table? from))
	(add! env '%modrefs
	  (pick (get from '%moduleid) symbol?))))
    (cond ((eq? head 'quote) (cons #OP_QUOTE (cadr expr)))
	  ((or (and from (test from dont-touch-decls head))
	       (and env (test env dont-touch-decls head)))
	   expr)
	  ((and from (%test from '%rewrite)
		(%test (get from '%rewrite) head))
	   (annotate (do-rewrite (get (get from '%rewrite) head) 
				 expr env bound opts)
		     expr opts))
	  ((and (ambiguous? headvalue)
		(or (exists special-form? headvalue)
		    (exists macro? headvalue)))
	   (logwarn |CantOptimize|
	     "Ambiguous head includes a macro or special form"
	     expr)
	   expr)
	  ((fail? headvalue)
	   (logwarn |CantOptimize|
	     "The head's value is the empty choice"
	     expr)
	   expr)
	  ((special-form? headvalue)
	   (loginfo |SpecialForm| "Optimizing " headvalue " form")
	   (let ((optimizer
		  (try (get special-form-optimizers headvalue)
		       (tryif (and (procedure-name headvalue)
				   (get-module headvalue))
			 (get special-form-optimizers
			      (cons (downcase (procedure-name headvalue))
				    (get-module headvalue)))))))
	     (annotate
	      (try (optimizer headvalue expr env bound opts)
		   (cons* #OP_EVALFN headvalue expr))
	      expr opts)))
	  ((exists macro? headvalue)
	   (annotate (optimize (macroexpand headvalue expr) env bound opts)
		     expr opts))
	  ((and (singleton? headvalue) (applicable? headvalue))
	   ;; If all of the head values are applicable, we optimize
	   ;;  the call, replacing the head with shortcuts to the
	   ;;  headvalue
	   (optimize-apply headvalue (cdr expr) env bound opts expr from head))
	  ((or (%lexref? headvalue) (pair? head)
	       (symbol? head) (ambiguous? headvalue))
	   ;; If all of the head values are applicable, we optimize
	   ;;  the call, replacing the head with shortcuts to the
	   ;;  headvalue
	   (make-fncall
	    (cond ((%lexref? headvalue) headvalue)
		  ((or (not from) (test from '%nosubst head)) head)
		  ((test from '%volatile head) `(#OP_SYMREF ,module ,head))
		  (else head))
	    (cdr expr)
	    env bound opts
	    expr))
	  (else
	   (when (and optwarn from
		      (not (test from '{%nosubst %volatile} head)))
	     (codewarning (cons* 'NOTFCN expr headvalue))
	     (logwarn |NotAFunction|
	       "The current value of " expr " (" head ") "
	       "doesn't appear to be a applicable given "
	       (apply append bound)))
	   expr))))

(define (optimize-apply fn args env bound opts expr (module) (fname))
  (default! module (procedure-module fn))
  (default! fname (or (procedure-name fn) (car expr)))
  (check-arguments fn (length args) expr)
  (debug%watch "optimize-apply"
    "fn" (or (procedure-name fn) fn)
    args bound module fname)
  (let* ((n-args (length args))
	 (optimizer (fcn/lookup procedure-optimizers fn))
	 (predicate-type (fcn/lookup predicate-typemap fn))
	 (opcode1 (fcn/lookup opcodes1 fn))
	 (opcode2 (fcn/lookup opcodes2 fn))
	 (num-opcode (fcn/lookup numeric-opcodes fn))
	 (table-opcode (fcn/lookup table-opcodes fn))
	 (opcodeN (fcn/lookup opcodesN fn))
	 (optimized
	  (try (optimizer fn expr env bound opts)
	       (tryif (and (= n-args 1) (exists? predicate-type))
		 (cond ((and (pair? predicate-type) (opcode? (car predicate-type)))
			(cons (car predicate-type)
			      (cons (cdr predicate-type) 
				    (optimize (car args) env bound opts))))
		       ((ctype? predicate-type)
			(cons #OP_ISA (cons predicate-type (optimize (car args) env bound opts))))
		       (else (fail))))
	       (tryif (and (= n-args 1) (exists? opcode1))
		 (cons opcode1 (qc (optimize (car args) env bound opts))))
	       (tryif (and (= n-args 2) (exists? opcode2))
		 (cons opcode2 (cons (qc (optimize (car args) env bound opts))
				     (qc (optimize (cadr args) env bound opts)))))
	       (tryif (and (= n-args 2) (exists? num-opcode))
		 (cons num-opcode
		       (cons (qc (optimize (car args) env bound opts))
			     (qc (optimize (cadr args) env bound opts)))))
	       (tryif (exists? table-opcode)
		 (cons table-opcode
		       (forseq (expr args)
			 (optimize expr env bound opts))))
	       (tryif (exists? opcodeN)
		 (cons opcodeN
		       (forseq (expr args)
			 (optimize expr env bound opts))))
	       (make-fncall
		(cond ((or (not module) (fail? fn)
			   (test module '%nosubst fname)
			   (test module '%nosubst (car expr)))
		       fname)
		      ((or (test module '%volatile fname)
			   (test module '%volatile (car expr)))
		       `(#OP_SYMREF ,module ,fname))
		      (else (get-headop fn fname n-args env bound opts)))
		(cdr expr)
		env bound opts
		expr))))
    (annotate optimized expr opts)))

(defambda (optimize-call expr env bound opts)
  (cond ((ambiguous? expr)
	 (for-choices expr (optimize-call expr env bound opts)))
	((not (pair? expr)) expr)
	((symbol? (car expr))
	 `(,(optimize (car expr) env bound opts)
	   ,@(if (pair? (cdr expr))
		 (optimize-args (cdr expr) env bound opts)
		 (cdr expr))))
	((or (pair? (car expr)) (ambiguous? (car expr)))
	 `(,(optimize (car expr) env bound opts)
	   ,@(if (pair? (cdr expr))
		 (optimize-args (cdr expr) env bound opts)
		 (cdr expr)))
	 (optimize-args expr env bound opts))
	(else expr)))

(defambda (optimize-args expr env bound opts)
  (forseq (arg expr)
    (if (or (qchoice? arg) (fail? arg) (eq? arg #default))
	arg
	(optimize arg env bound opts))))

(define (inner-optimize-procedure! proc (opts #f))
  (thread/set! 'codewarnings #{})
  (let* ((env (lambda-env proc))
	 (arglist (lambda-args proc))
	 (body (lambda-body proc))
	 (bound (list (arglist->vars arglist)))
	 (initial (and (pair? body) (car body)))
	 (opts (if (reflect/get proc 'optimize)
		   (if opts
		       (cons (reflect/get proc 'optimize) opts)
		       (reflect/get proc 'optimize))
		   opts))
	 (new-body (optimize-body body env bound opts)))
    (unless (null? body)
      (reflect/store! proc 'optimized (gmtimestamp))
      (reflect/store! proc 'original_args arglist)
      (reflect/store! proc 'original_body body)
      (cond ((not (use-opcodes? opts)))
	    ((not simplify-bodies))
	    ((and (pair? body) (null? (cdr body)) (opcode? (caar body)))
	     (set! body (car body)))
	    (else))
      (if use-consblock 
	  (optimize-lambda-body! proc new-body)
	  (set-lambda-entry! proc new-body))
      (when (pair? arglist)
	(let ((optimized-args (optimize-arglist arglist env opts)))
	  (unless (equal? arglist optimized-args)
	    (set-lambda-args! proc optimized-args))))
      (if (exists? (thread/get 'codewarnings))
	  (logwarn |OptimizeErrors|
	    "for " proc ": "
	    (do-choices (warning (thread/get 'codewarnings))
	      (printout "\n\t" warning)))
	  (lognotice |Optimized| proc)))
    (when (and (exists? (thread/get 'codewarnings))
	       (or all-warnings (exists? (thread/get 'threadwarnings))))
      (store! (try (thread/get 'threadwarnings) all-warnings)
	  proc (thread/get 'codewarnings)))
    (thread/set! 'codewarnings #{})))

(define (optimize-procedure! proc (opts #f))
  (thread/set! 'codewarnings #{})
  (unless (reflect/get proc 'optimized)
    (if (getopt opts 'err #f)
	(inner-optimize-procedure! proc opts)
	(onerror
	    (inner-optimize-procedure! proc opts)
	    (lambda (ex) 
	      (logwarn |OptimizationError|
		"While optimizing "
		(or (procedure-name proc) proc) ", got "
		(error-condition ex) " in " (error-context ex) 
		(if (error-details ex) (printout " (" (error-details ex) ")"))
		(when (error-irritant? ex)
		  (printout "\n" (pprint (error-irritant ex)))))
	      (if persist-default #f ex))))))

(define (optimize-arglist arglist env opts)
  (if (pair? arglist)
      (cons
       (if (and (pair? (car arglist)) (pair? (cdr (car arglist)))
		(singleton? (cadr (car arglist)))
		(or (pair? (cadr (car arglist))) (symbol? (cadr (car arglist)))))
	   `(,(caar arglist) 
	     ,(optimize (cadr (car arglist)) env '() opts))
	   (car arglist))
       (optimize-arglist (cdr arglist) env opts))
      arglist))

(define (deoptimize-procedure! proc)
  (when (reflect/get proc 'optimized)
    (reflect/drop! proc 'optimized)
    (when (reflect/get proc 'original_body)
      (reflect/drop! proc 'original_body)
      (optimize-lambda-body! proc #f))
    (when (reflect/get proc 'original_args)
      (set-lambda-args! proc (reflect/get proc 'original_args))
      (reflect/drop! proc 'original_args))))

(define (procedure-optimized? arg)
  (reflect/get arg 'optimized))

(define (optimized? arg)
  (if (optimizable? arg)
      (procedure-optimized? arg)
      (and (or (hashtable? arg) (slotmap? arg) (schemap? arg)
	       (environment? arg))
	   (exists procedure-optimized? (get arg (getkeys arg))))))

(define (optimize-get-module spec)
  (onerror (or (get-module spec)
	       (irritant spec
		   |GetModuleFailed| optimize-module
		   "Couldn't load module for " spec))
      (lambda (ex) 
	(irritant spec
	    |GetModuleFailed| optimize-module
	    "Couldn't load module for " spec))))

(define-init init-modinfo!
  (defsync (init-modinfo! module)
    (try (get module '%modinfo)
	 (let ((info (frame-create #f
		       'id (get module '%moduleid)
		       'source (get module '%source))))
	   (store! module '%modinfo info)
	   info))))

(define (optimize-module! module (opts #f) (module) 
			  (warnings (make-hashtable))
			  (old-warnings (thread/get 'threadwarnings)))
  (when (symbol? module)
    (set! module (optimize-get-module module)))
  (loginfo |OptimizeModule| module)
  (set! opts
    (if opts
	(try (cons (get module '%optimize_options) opts)
	     opts)
	(try (get module '%optimize_options) #f)))
  (thread/set! 'threadwarnings warnings)
  (let ((bindings (and module (module-binds module)))
	(usefcnrefs (use-fcnrefs? opts))
	(modinfo (get module '%modinfo))
	(optimized {})
	(count 0))
    (when bindings
      (do-choices (var bindings)
	(let ((value (get module var)))
	  (when (and (exists? value) (optimizable? value))
	    (loginfo |OptimizeModule|
	      "Optimizing procedure " var " in " module)
	    (set! count (1+ count))
	    (set+! optimized var)
	    (optimize-procedure! value opts))
	  (when (and usefcnrefs (exists? value) (applicable? value))
	    (update-fcnid! var module value)))))
    (when (fail? modinfo) (set! modinfo (init-modinfo! module)))
    (store! modinfo 'optimized optimized)
    (store! modinfo 'fcnids (try (table-size (get module '%fcnids)) 0))
    (cond ((hashtable? module)
	   (lognotice |OpaqueModule| 
	     "Not optimizing opaque module " (get module '%moduleid)))
	  ((exists symbol? (get module '%moduleid))
	   (let* ((referenced-modules (get module '%modrefs))
		  (used-modules
		   (eval `(within-module
			   ',(pick (get module '%moduleid) symbol?)
			   (,getmodules))))
		  (unused (difference used-modules referenced-modules
				      standard-modules
				      (get module '%moduleid))))
	     (store! modinfo 'referenced-modules referenced-modules)
	     (store! modinfo 'used-modules used-modules)
	     (store! modinfo 'unused-modules unused)
	     (when (and check-module-usage (exists? unused))
	       (logwarn |UnusedModules|
		 "Module " (try (pick (get module '%moduleid) symbol?)
				(get module '%moduleid))
		 " declares " (choice-size unused) " possibly unused modules: "
		 (do-choices (um unused i)
		   (printout (if (> i 0) ", ") um))))))
	  (else))
    (when (> (table-size warnings) 0) (store! modinfo 'warnings warnings))
    (when (and module-warnings (> (table-size warnings) 0))
      (store! warnings '%moduleid (get module '%moduleid))
      (store! module-warnings (get module '%moduleid) warnings))
    (thread/set! 'threadwarnings (qc old-warnings))
    count))

(define (deoptimize-module! module)
  (loginfo "Deoptimizing module " module)
  (when (symbol? module)
    (set! module (optimize-get-module module)))
  (let ((bindings (module-binds module))
	(count 0))
    (do-choices (var bindings)
      (loginfo "Deoptimizing module binding " var)
      (let ((value (get module var)))
	(when (and (exists? value) (optimizable? value))
	  (when (deoptimize-procedure! value)
	    (set! count (1+ count))))))
    count))

(define (optimize-bindings! bindings (opts #f))
  (logdebug "Optimizing bindings " bindings)
  (let ((count 0)
	(skip (getopt opts 'dont-optimize
		      (tryif (test bindings '%dont-optimize)
			(get bindings '%dont-optimize)))))
    (do-choices (var (difference (getkeys bindings) skip '%dont-optimize))
      (logdebug "Optimizing binding " var)
      (let ((value (get bindings var)))
	(if (defined? value)
	    (when (optimizable? value)
	      (set! count (1+ count))
	      (optimize-procedure! value #f))
	    (unless (default? value) 
	      (notify var " is bound but undefined")))))
    count))

(define (deoptimize-bindings! bindings (opts #f))
  (logdebug "Deoptimizing bindings " bindings)
  (let ((count 0))
    (do-choices (var (getkeys bindings))
      (logdetail |Deoptimize| "Deoptimizing binding " var)
      (let ((value (get bindings var)))
	(if (defined? value)
	    (when (optimizable? value)
	      (when (deoptimize-procedure! value)
		(set! count (1+ count))))
	    (if (bound? value)
		(logwarn |Unbound| var " is bound but undefined (#default)")
		(logwarn |Unbound| var " is unbound")))))
    count))

(defambda (module-arg? arg)
  (or (fail? arg) (string? arg)
      (and (pair? arg) 
	   (or (eq? (car arg) 'quote) (eq? (car arg) #op_quote)))
      (and (ambiguous? arg) (fail? (reject arg module-arg?)))))

(define (optimize*! . args)
  (dolist (arg args)
    (cond ((optimizable? arg) (optimize-procedure! arg))
	  ((table? arg) (optimize-module! arg))
	  (else (error '|TypeError| 'optimize*
		       "Invalid optimize argument: " arg)))))

(define (deoptimize*! . args)
  (dolist (arg args)
    (cond ((optimizable? arg) (deoptimize-procedure! arg))
	  ((table? arg) (deoptimize-module! arg))
	  (else (error '|TypeError| 'optimize*
		       "Invalid optimize argument: " arg)))))

(define optimize!
  (macro expr
    (if (null? (cdr expr))
	`(,optimize-bindings! (,%bindings))
	(cons optimize*!
	      (forseq (x (cdr expr))
		(if (module-arg? x)
		    `(,optimize-get-module ,x)
		    x))))))

(define deoptimize!
  (macro expr
    (if (null? (cdr expr))
	`(,deoptimize-bindings! (,%bindings))
	(cons deoptimize*!
	      (forseq (x (cdr expr))
		(if (module-arg? x)
		    `(,optimize-get-module ,x)
		    x))))))

(define use!
  (macro expr
    (let ((modarg (if (and (pair? (cadr expr))
			   (eq? (car (cadr expr)) 'quote))
		      (cadr expr)
		      (list 'quote (cadr expr))))) 
      `(begin
	 (use-module ,modarg)
	 (optimize! ,modarg)
	 (do-choices (modname ,modarg)
	   (let ((syms (difference (%ls modname) '%moduleid))
		 (width (config 'consolewidth 80))
		 (column (+ (length (symbol->string modname)) 5)))
	     (when (exists? syms)
	       (lineout ";; " modname ":"
		 (do-choices (sym syms)
		   (when (> column width)
		     (printout "\n;;   ")
		     (set! column 5))
		   (printout " " sym)
		   (set! column (+ column 1 (length (symbol->string sym)))))))))))))

(define (convert-modarg args)
  (if (null? args) '()
      (let ((result (convert-modarg (cdr args))))
	(if (ambiguous? (car args))
	    (do-choices (spec (car args))
	      (if (symbol? spec)
		  (set! result (cons `',spec result))
		  (set! result (cons spec result))))
	    (if (symbol? (car args))
		(set! result (cons `',(car args) result))
		(if (and (pair? (car args)) (eq? (caar args) 'quote))
		    (do-choices (spec (cadr (car args)))
		      (if (symbol? spec)
			  (set! result (cons `',spec result))
			  (set! result (cons spec result))))
		    (set! result (cons (car args) result)))))
	result)))

(define (handle-module modname)
  (let ((module (get-module modname)))
    (let ((syms (difference (%ls modname) '%moduleid))
	  (width (config 'consolewidth 80))
	  (column (+ (length (symbol->string modname)) 5)))
      (when (exists? syms)
	(lineout ";; " modname ":"
	  (do-choices (sym syms)
	    (when (> column width)
	      (printout "\n;;   ")
	      (set! column 5))
	    (printout " " sym)
	    (set! column (+ column 1 (length (symbol->string sym))))))))
    (optimize! module)
    module))

(define (convert-arg arg)
  (if (symbol? arg)
      (cons #OP_QUOTE arg)
      (if (pair? arg)
	  (if (and (pair? (car arg)) (eq? (caar arg) 'quote))
	      (convert-arg (cadr (car arg)))
	      arg)
	  {})))

(define use+
  (macro expr
    `(use-module
      (choice
       ,@(forseq (spec (map convert-arg (cdr expr)))
	   `(,handle-module ,spec))))))

(defambda (reoptimize! modules)
  (reload-module modules)
  (optimize-module! (get-module modules)))

(define (optimized arg)
  (cond ((optimizable? arg) (optimize-procedure! arg))
	((or (hashtable? arg) (slotmap? arg) (schemap? arg)
	     (environment? arg))
	 (optimize-module! arg))
	((and (fcnid? arg) (compound-procedure? arg)) arg)
	(else (irritant arg |TypeError| OPTIMIZED
			"Not a compound procedure, environment, or module")))
  arg)

;;; Procedure optimizers

(define (optimize-compound-ref proc expr env bound opts
			       (off-arg) (type-arg))
  (set! off-arg (get-arg expr 2 #f))
  (set! type-arg (get-arg expr 3 #f))
  (tryif (fixnum? off-arg)
    (tryif (and (pair? type-arg)
		(overlaps? (car type-arg) {'quote quote}))
      (if (= (length type-arg) 2)
	  `(,xref-opcode ,off-arg ,(cadr type-arg)
			 ,(optimize (get-arg expr 1) env bound opts))
	  (irritant type-arg |SyntaxError| COMPOUND-REF "In compound ref " expr)))
    (tryif (and (pair? type-arg) (not type-arg))
      `(,xref-opcode ,off-arg #f
		     (optimize (get-arg expr 1) env bound opts)))))

(store! procedure-optimizers compound-ref optimize-compound-ref)

(define (optimize-compound-predicate proc expr env bound opts
				     (type-arg))
  (set! type-arg (get-arg expr 2 #f))
  (if (and (pair? type-arg)
	   (overlaps? (car type-arg) {'quote quote}))
      `(,xpred-opcode ,(cadr type-arg) ,(optimize (get-arg expr 1) env bound opts))
      (if type-arg
	  `(,(fcnref compound? (car expr) env opts)
	    ,(optimize (get-arg expr 1) env bound opts)
	    ,(optimize type-arg env bound opts))
	  `(,(fcnref compound? (car expr) env opts)
	    ,(optimize (get-arg expr 1) env bound opts)))))

(store! procedure-optimizers compound? optimize-compound-predicate)

;;; Optimizing break (with a warning)

(define (optimize-break proc expr env bound opts)
  (if (null? (cdr expr)) `(,break-opcode)
      (begin 
	(codewarning (cons* 'TOOMANYARGS expr))
	(when optwarn
	  (logwarn |TooManyArgs|
	    "The (break) function doesn't take any arguments"))
	expr)))

(store! procedure-optimizers BREAK optimize-break)

;;;; Special form handlers

(define (optimize-block handler expr env bound opts)
  (let ((headop (get-headop handler (car expr) (length (cdr expr)) env bound opts)))
    (if (special-form? headop)
	(cons* #OP_EVALFN headop (cons (car expr) (optimize-exprs (cdr expr))))
	(cons (qc headop) (optimize-exprs (cdr expr))))))

(define (optimize-quote handler expr env bound opts)
  (if (use-opcodes? opts)
      (cons #OP_QUOTE (get-arg expr 1))
      expr))

(define (optimize-begin handler expr env bound opts)
  (if (use-opcodes? opts)
      (cons #OP_BEGIN (optimize-exprs (strip-preamble (cdr expr))))
      (optimize-block handler expr env bound opts)))

(define (optimize-if handler expr env bound opts)
  (if (use-opcodes? opts)
      (cons* #OP_BRANCH (optimize (cadr expr) env bound opts)
	     (optimize (elt expr 2) env bound opts)
	     (if (> (length expr) 3)
		 (optimize (elt expr 3) env bound opts)
		 '()))
      (optimize-block handler expr env bound opts)))

(define (optimize-not handler expr env bound opts)
  (if (use-opcodes? opts)
      (cons #OP_NOT (optimize (get-arg expr 1) env bound opts))
      (optimize-block handler expr env bound opts)))

(define (optimize-when handler expr env bound opts)
  (if (and (use-opcodes? opts) (rewrite? opts))
      (list #OP_BRANCH
	    (list #OP_TRY (optimize (cadr expr) env bound opts) #f)
	    (cons #OP_BEGIN
		  (append (optimize-exprs (strip-preamble (cddr expr)))
			  `((,void-opcode)))))
      (optimize-block handler expr env bound opts)))
(define (optimize-unless handler expr env bound opts)
  (if (and (use-opcodes? opts) (rewrite? opts))
      (list #OP_BRANCH
	    (cons #OP_NOT (optimize (cadr expr) env bound opts))
	    (cons #OP_BEGIN
		  (append (optimize-exprs (strip-preamble (cddr expr)))
			  `((,void-opcode)))))
      (optimize-block handler expr env bound opts)))

(define (optimize-until handler expr env bound opts)
  (if (and (use-opcodes? opts) (rewrite? opts))
      `(#OP_BEGIN
	(,#OP_UNTIL
	 ,(optimize (cadr expr) env bound opts)
	 ,@(optimize-body (cddr expr)))
	(,void-opcode))
      (optimize-block handler expr env bound opts)))
(define (optimize-while handler expr env bound opts)
  (if (and (use-opcodes? opts) (rewrite? opts))
      `(#OP_BEGIN
	(,#OP_UNTIL
	 ,(cons not-opcode (optimize (cadr expr) env bound opts))
	 ,@(optimize-body (cddr expr)))
	(,void-opcode))
      (optimize-block handler expr env bound opts)))

(define (optimize-let handler expr env bound opts)
  (if (and (use-opcodes? opts) (use-bindops? opts))
      (let* ((bindexprs (cadr expr))
	     (vars (map car bindexprs))
	     (vals (map cadr bindexprs))
	     (not-vars (forseq (var vars) #f))
	     (outer (cons not-vars bound))
	     (inner (cons vars bound)))
	`(#OP_BIND
	  ,(->vector vars)
	  ,(forseq (valexpr vals)
	     (optimize valexpr env outer opts))
	  . ,(forseq (bodyexpr (cddr expr))
	       (optimize bodyexpr env inner opts))))
      (let* ((bindexprs (cadr expr))
	     (new-bindexprs
	      (forseq (x bindexprs) `(,(car x) ,(optimize (cadr x) env bound opts))))
	     (body (cddr expr)))
	`(,handler ,new-bindexprs
		   ,@(let ((bound (cons (map car bindexprs) bound)))
		       (optimize-body body))))))

(define (optimize-doexpression handler expr env bound opts)
  (let* ((bindspec (cadr expr))
	 (body (cddr expr))
	 (item-var (cond ((symbol? bindspec) bindspec)
			 ((or (not (pair? bindspec)) (not (< 1 (length bindspec) 5))
			      (not (symbol? (car bindspec))))
			  (irritant bindspec |SyntaxError|))
			 (else (car bindspec))))
	 (val-expr (if (symbol? bindspec) bindspec
		       (if (and (pair? bindspec) (>= (length bindspec) 2))
			   (get-arg bindspec 1)
			   (irritant bindspec |SyntaxExpr|))))
	 (count-var (or (and (pair? bindspec) (get-arg bindspec 2 #f)) '|#|))
	 (whole-var (or (and (pair? bindspec) (get-arg bindspec 3 #f)) '|@|))
	 (inner (cons (list item-var count-var whole-var) bound)))
    `(#OP_EVALFN ,handler ,(car expr)
		 (,item-var ,(optimize val-expr env bound opts) ,count-var ,whole-var)
		 . ,(optimize-body body env inner opts))))
(define (optimize-do2expression handler expr env bound opts)
  (let ((bindspec (cadr expr)) 
	(body (cddr expr)))
    `(#OP_EVALFN ,handler
		 ,(car expr) ,(cond ((symbol? bindspec)
				     `(,bindspec ,(optimize bindspec env bound opts)))
				    ((pair? bindspec)
				     `(,(car bindspec)
				       ,(optimize (cadr bindspec) env bound opts)
				       ,@(cddr bindspec)))
				    ((symbol? bindspec)
				     `(,bindspec
				       ,(optimize bindspec env bound opts)))
				    (else (error 'syntax "Bad do-* expression")))
		 ,@(let ((bound
			  (if (symbol? bindspec)
			      (cons (list bindspec) bound)
			      (if (= (length bindspec) 3)
				  (cons (list (first bindspec)
					      (third bindspec)) bound)
				  (cons (list (first bindspec)) bound)))))
		     (forseq (b body) (optimize b env bound opts))))))

(define (optimize-dotimes handler expr env bound opts)
  (let* ((bindspec (cadr expr)) 
	 (varname (car bindspec))
	 (limit-expr (cadr bindspec))
	 (newbound (cons (list varname '|dotimes_limit|) bound))
	 (iter-ref (get-lexref varname newbound))
	 (limit-ref (get-lexref '|dotimes_limit| newbound))
	 (body (cddr expr)))
    `(#OP_BIND #(,varname |dotimes_limit|) 
	       #(0 ,(optimize limit-expr env bound opts))
	       . (#OP_UNTIL
		  (#OP_GTE ,iter-ref ,limit-ref)
		  ,@(forseq (clause body) (optimize clause env newbound opts))
		  (#OP_RESET_ENV)
		  (#OP_ASSIGN ,iter-ref #t #OP_PLUS ,iter-ref . 1)))))

(define (optimize-doseq handler expr env bound opts)
  (let* ((bindspec (cadr expr))
	 (varname (car bindspec))
	 (val-expr (cadr bindspec))
	 (iter-var (get-arg bindspec 2 '|doseq_i|))
	 (new-bindings (cons `(,varname ,iter-var |_doseq_subject| |_doseq_limit|)
			     bound))
	 (elt-ref (get-lexref varname new-bindings))
	 (iter-ref (get-lexref iter-var new-bindings))
	 (seq-ref (get-lexref '|_doseq_subject| new-bindings))
	 (limit-ref (get-lexref '|_doseq_limit| new-bindings))
	 (body (cddr expr)))
    `(#OP_BIND #(,varname ,iter-var |_doseq_subject| |_doseq_limit|)
	       (#f 0 ,(optimize val-expr env (cons '(#f #f #f #f) bound) opts) 0)
	       . ((#OP_BRANCH 
		   ,(cons* #OP_ISA #pair_type seq-ref)
		   (#OP_EVALFN ,doseq . (doseq (,varname ,seq-ref ,iter-var) ,@body))
		   (#OP_BEGIN
		    (#OP_ASSIGN ,limit-ref #f #OP_LENGTH . ,seq-ref)
		    (#OP_UNTIL
		     (#OP_GTE ,iter-ref ,limit-ref)
		     (#OP_ASSIGN ,elt-ref #f #OP_SEQELT ,seq-ref . ,iter-ref)
		     ,@(forseq (clause body) (optimize clause env new-bindings  opts))
		     (#OP_RESET_ENV)
		     (#OP_ASSIGN ,iter-ref #f #OP_PLUS 1 . ,iter-ref))
		    (#OP_VOID)))))))

(define (optimize-dosubsets handler expr env bound opts)
  (let ((bindspec (cadr expr)) 
	(body (cddr expr)))
    `(,handler (,(car bindspec)
		,(optimize (cadr bindspec) env bound opts)
		,@(cddr bindspec))
	       ,@(let ((bound (if (= (length bindspec) 4)
				  (cons (list (first bindspec) (fourth bindspec))
					bound)
				  (cons (list (first bindspec)) bound))))
		   (optimize-body body)))))

(define (optimize-let*-bindings bindings env bound opts)
  (if (null? bindings) '()
      `((,(car (car bindings))
	 ,(optimize (cadr (car bindings)) env bound opts))
	,@(optimize-let*-bindings
	   (cdr bindings) env
	   (cons (append (car bound) (list (car (car bindings))))
		 (cdr bound))
	   opts))))

(define (optimize-let* handler expr env bound opts)
  (if (and (use-opcodes? opts) (use-bindops? opts))
      (let* ((bindexprs (cadr expr))
	     (vars (map car bindexprs))
	     (vals (map cadr bindexprs))
	     (not-vars (forseq (var vars) #f))
	     (outer (cons not-vars bound))
	     (inner (cons vars bound)))
	`(#OP_BIND
	  ,(->vector vars)
	  ,(forseq (valexpr vals i)
	     ;; We implement the let/let* semantics in the compiler by
	     ;; have each init be evaulated with a different list of
	     ;; local bindings
	     (optimize valexpr env 
		       (cons (append (slice vars 0 i)
				     (slice not-vars i))
			     bound)
		       opts))
	  . ,(forseq (bodyexpr (cddr expr))
	       (optimize bodyexpr env inner opts))))
      (let ((bindspec (cadr expr))
	    (body (cddr expr)))
	`(,handler
	  ,(optimize-let*-bindings (cadr expr) env (cons '() bound) opts)
	  . ,(let ((bound (cons (map car bindspec) bound)))
	       (optimize-body body))))))

#|
(define (foo x y) (let ((x2 (* x x)) (y2 (* y y))) (+ x2 y2)))
(define (foo* x y)
  (let* ((x (begin (%watch x) (* x x))) 
	 (y (begin (%watch x y)) (* x y)))
    (+ x y)))
|#

(define (optimize-assign handler expr env bound opts)
  (let ((var (get-arg expr 1))
	(setval (get-arg expr 2)))
    (let ((loc (or (get-lexref var bound 0) 
		   (if (wherefrom var env)
		       (cons var (wherefrom var env))
		       var)))
	  (optval (optimize setval env bound opts)))
      (if (and (use-opcodes? opts) (rewrite? opts))
	  (cond ((symbol? loc) 
		 ;; If loc is a symbol, we couldn't resolve it to a
		 ;; lexical contour or enviroment
		 `(,handler ,var ,optval))
		((overlaps? handler set!) `
		 (#OP_ASSIGN ,loc #f . ,optval))
		((overlaps? handler set+!)
		 `(#OP_ASSIGN ,loc #OP_UNION . ,optval))
		((and (overlaps? handler default!) (= (length expr) 3)) 
		 ;; Don't convert default! with a `replace` arg to use
		 ;; OP_ASSIGN
		 `(#OP_ASSIGN ,loc #t . ,optval))
		((overlaps? handler default!) 
		 ;; Don't convert default! with a `replace` arg to use
		 ;; OP_ASSIGN
		 `(,default! ,var ,optval . ,(cdddr expr)))
		(else `(,handler ,var ,optval)))
	  `(,handler ,var ,optval)))))

(define (optimize-lambda handler expr env bound opts)
  `(,handler ,(cadr expr)
	     ,@(let ((bound (cons (arglist->vars (cadr expr)) bound))
		     (body (cddr expr)))
		 (forseq (b body)
		   (optimize b env bound opts)))))

(define (old-optimize-cond handler expr env bound opts)
  (cons handler 
	(forseq (clause (cdr expr))
	  (cond ((not (pair? clause))
		 (codewarning (list 'BADCLAUSE expr clause)))
		((eq? (car clause) 'else)
		 `(ELSE ,@(optimize-body (cdr clause))))
		((and (pair? (cdr clause)) (eq? (cadr clause) '=>))
		 `(,(optimize (car clause) env bound opts)
		   =>
		   ,@(optimize-body (cdr clause))))
		(else (optimize-body clause))))))

(define (optimize-cond handler expr env bound opts)
  (try
   (tryif (and (use-opcodes? opts)
	       (not (some? (lambda (clause) 
			     (and (pair? clause) (pair? (cdr clause))
				  (identical? (cadr clause) '=>)))
			   (cdr expr))))
     (convert-cond (cdr expr) env bound opts))
   (cons* #OP_EVALFN handler 
	  `(cond 
	    ,@(forseq (clause (cdr expr) )
		(cond ((not (pair? clause))
		       (codewarning (list 'BADCLAUSE expr clause))
		       (when optwarn
			 (logwarn |BadClause|
			   "The clause " clause " in the CONDitional " 
			   expr " is malformed."))
		       clause)
		      ((eq? (car clause) 'else)
		       `(ELSE ,@(optimize-body (cdr clause))))
		      ((and (pair? (cdr clause)) 
			    (identical? (cadr clause) '=>))
		       `(,(optimize (car clause) env bound opts)
			 =>
			 ,@(optimize-body (cddr clause))))
		      (else (optimize-body clause))))))))

(define (convert-cond clauses env bound opts)
  (if (null? clauses)
      (list void-opcode)
      (let ((clause (car clauses)))
	(cond ((not (pair? clause)) (fail))
	      ((eq? (car clause) 'else)
	       (cons #OP_BEGIN 
		     (forseq (c (cdr clause))
		       (optimize c env bound opts))))
	      (else
	       (cons* #OP_BRANCH 
		      (optimize (car clause) env bound opts)
		      (cond ((empty-list? (cdr clause))
			     `(#OP_VOID))
			    ((empty-list? (cdr (cdr clause)))
			     (optimize (cadr clause) env bound opts))
			    (else 
			     (cons #OP_BEGIN 
				   (forseq (c (strip-preamble (cdr clause)))
				     (optimize c env bound opts)))))
		      (convert-cond (cdr clauses) env bound opts)))))))

(define (optimize-and handler expr env bound opts)
  (if (= (length expr) 1) #t
      (if (use-opcodes? opts)
	  (cons #OP_AND (optimize-exprs (cdr expr)))
	  (optimize-block handler expr env bound opts))))

(define (optimize-or handler expr env bound opts)
  (if (= (length expr) 1) #f
      (if (use-opcodes? opts)
	  (cons #OP_OR (optimize-exprs (cdr expr)))
	  (optimize-block handler expr env bound opts))))

(define (optimize-try handler expr env bound opts)
  (if (use-opcodes? opts)
      (cons #OP_TRY (optimize-exprs (cdr expr)))
      (optimize-block handler expr env bound opts)))

(define (optimize-tryif handler expr env bound opts)
  (if (and (use-opcodes? opts) (rewrite? opts))
      (cons* #OP_BRANCH
	     (optimize (get-arg expr 1) env bound opts)
	     (cons #OP_TRY (optimize-exprs (cddr expr)))
	     {})
      (optimize-block handler expr env bound opts)))

(define (optimize-evaltest handler expr env bound opts)
  (cons* #OP_EVALFN handler expr))

(define (optimize-numeric handler expr env bound opts)
  (cond ((fail? (fcn/lookup numeric-opcodes handler)) (fail))
	((= (length expr) 2)
	 (cond ((overlaps? handler {+ *})
		(cons* #OP_CHECK_TYPE #number_type
		       (optimize (cadr expr) env bound opts)))
	       ((eq? handler -)
		(cons* #OP_MINUS 0 (optimize (cadr expr) env bound opts)))
	       ((eq? handler /)
		(cons* #OP_DIVIDE 1 (optimize (cadr expr) env bound opts)))
	       ((eq? handler /~)
		(cons* #OP_FLODIV 1 (optimize (cadr expr) env bound opts)))
	       (else (fail))))
	((= (length expr) 3)
	 (cond ((exists? (fcn/lookup numeric-opcodes handler))
		(cons* (fcn/lookup numeric-opcodes handler)
		       (optimize (cadr expr) env bound opts)
		       (optimize (caddr expr) env bound opts)))
	       (else (fail))))
	((eq? handler -)
	 (cons* #OP_MINUS (cadr expr) (optimize (cons + (cddr expr)) env bound opts)))
	((eq? handler /)
	 (cons* #OP_DIVIDE (cadr expr) (optimize (cons * (cddr expr)) env bound opts)))
	((eq? handler /)
	 (cons* #OP_FLODIV (cadr expr) (optimize (cons * (cddr expr)) env bound opts)))
	(else (nest-numeric-op (fcn/lookup numeric-opcodes handler) (cdr expr)
			       env bound opts))))

(define (nest-numeric-op op args env bound opts)
  (tryif (and (pair? args) (proper-list? args))
    (if (empty-list? (cdr args))
	(optimize (car args))
	(if (= (length args) 2)
	    (cons* op (optimize (car args) env bound opts)
		   (optimize (cadr args) env bound opts))
	    (cons* op (optimize (car args) env bound opts)
		   (nest-numeric-op op (cdr args) env bound opts))))))

(define (optimize-choice handler expr env bound opts)
  (cons #OP_UNION (forseq (subexpr (cdr expr))
		    (optimize subexpr env bound opts))))

(define (optimize-case handler expr env bound opts)
  `(#OP_EVALFN ,handler ,(car expr)
	       ,(optimize (cadr expr) env bound opts)
	       ,@(forseq (clause (cddr expr))
		   (cons (car clause)
			 (forseq (x (cdr clause))
			   (optimize x env bound opts))))))

(define (optimize-unwind-protect handler expr env bound opts)
  `(#OP_EVALFN ,handler ,(car expr) ,(optimize (cadr expr) env bound opts)
	       ,@(optimize-body (cddr expr))))

(define (optimize-quasiquote handler expr env bound opts)
  `(#OP_EVALFN ,handler ,(car expr) ,(optimize-quasiquote-node (cadr expr) env bound opts)))
(define (optimize-unquote expr env bound opts)
  (if (and (pair? expr) 
	   (or (eq? (car expr) 'unquote) 
	       (eq? (car expr) 'unquote*)))
      (list (car expr) (qc (optimize-unquote (cadr expr) env bound opts)))
      (optimize-embedded-quote (qc expr) env bound opts)))
(define (optimize-embedded-quote expr env bound opts)
  (if (and (pair? expr) (eq? (car expr) 'quote)
	   (pair? (cdr expr)) (empty-list? (cddr expr)))
      (if (qchoice? (cadr expr))
	  `(quote ,(cadr expr))
	  (list 'quote (optimize-quasiquote-node (cadr expr) env bound opts)))
      (optimize expr env bound opts)))

(defambda (optimize-quasiquote-node expr env bound opts)
  (cond ((ambiguous? expr)
	 (for-choices (elt expr)
	   (optimize-quasiquote-node elt env bound opts)))
	((and (pair? expr) 
	      (or (eq? (car expr) 'unquote) (eq? (car expr) 'unquote*)))
	 (optimize-unquote expr env bound opts))
	((and (pair? expr) (eq? (car expr) 'quote) 
	      (pair? (cdr expr)) (empty-list? (cddr expr)))
	 (if (qchoice? (cadr expr)) expr
	     (list 'quote (optimize-quasiquote-node (cadr expr) env bound opts))))
	((pair? expr)
	 (let ((backwards '()) (scan expr))
	   (while (pair? scan)
	     (set! backwards (cons (car scan) backwards))
	     (set! scan (cdr scan)))
	   (if (and (empty-list? scan) (pair? (cdr backwards)) 
		    (or (eq? (cadr backwards) 'unquote)
			(eq? (cadr backwards) 'unquote*)))
	       (reverse (cons* (optimize-embedded-quote (car backwards) env bound opts)
			       (cadr backwards)
			       (forseq (elt (cddr backwards))
				 (optimize-quasiquote-node elt env bound opts))))
	       (let ((converted scan)
		     (scanback backwards))
		 (while (pair? scanback)
		   (set! converted 
		     (cons (optimize-quasiquote-node (car scanback) env bound opts)
			   converted))
		   (set! scanback (cdr scanback)))
		 converted))))
	((vector? expr)
	 (forseq (elt expr)
	   (optimize-quasiquote-node elt env bound opts)))
	((slotmap? expr)
	 (let ((copy (frame-create #f))
	       (slots (getkeys expr)))
	   (do-choices (slot slots)
	     (store! copy (optimize-quasiquote-node slot env bound opts)
		     (try (optimize-quasiquote-node (get expr slot) env bound opts)
			  (get expr slot))))
	   copy))
	(else expr)))

(define (optimize-logmsg handler expr env bound opts)
  (if (or (symbol? (cadr expr)) (number? (cadr expr)) (not (cadr expr)))
      (if (symbol? (caddr expr))
	  `(,handler ,(cadr expr)
		     ,(caddr expr)
		     ,@(optimize-exprs (cdddr expr)))
	  `(,handler ,(cadr expr) ,@(optimize-exprs (cddr expr))))
      `(,handler ,@(optimize-exprs (cdr expr)))))

(define (optimize-logif handler expr env bound opts)
  (if (or (symbol? (caddr expr))  (number? (caddr expr)))
      `(,handler ,(optimize (cadr expr) env bound opts)
		 ,(caddr expr)
		 ,@(optimize-exprs (cdddr expr)))
      `(,handler ,(optimize (cadr expr) env bound opts)
		 ,@(optimize-exprs (cddr expr)))))

(define (optimize-logif+ handler expr env bound opts)
  (let ((test (second expr))
	(level (third expr))
	(condition (fourth expr)))
    (if (or (symbol? level) (number? level))
	(if (symbol? condition)
	    `(,handler ,(optimize test env bound opts)
		       ,level ,condition
		       ,@(optimize-exprs (cdr (cdddr expr))))
	    `(,handler ,(optimize test env bound opts)
		       ,level ,@(optimize-exprs (cdddr expr))))
	`(,handler ,(optimize test env bound opts)
		   ,@(optimize-exprs (cddr expr))))))
						 
;;; Optimizing XHTML expressions

;; This doesn't handle mixed alist ((x y)) and plist (x y) attribute lists
;;  because they shouldn't work anyway
(define (optimize-attribs attribs env bound opts)
  (cond ((not (pair? attribs)) attribs)
	((pair? (car attribs))
	 (forseq (attrib attribs)
	   (cond ((not (pair? attrib)) attrib)
		 ((and (symbol? (car attrib)) (pair? (cdr attrib)))
		  `(,(car attrib) ,(optimize (cadr attrib) env bound opts)))
		 (else (codewarning (cons* '|BadAttribExpression| attrib))
		      attrib))))
	((and (symbol? (car attribs)) (even? (length attribs)))
	 (let ((failed #f) (alist '()) (scan attribs))
	   (cond ((symbol? (car scan))
		  (set! alist (cons (list (car scan) (cadr scan)) alist))
		  (set! scan (cddr scan)))
		 (else
		  (codewarning (cons* '|BadAttribPList| (slice scan 0 2)))
		  (set! scan (cddr scan))
		  (set! failed #t)))
	   (if failed
	       attribs
	       (forseq (attrib alist)
		 (if (pair? (cdr attrib))
		     `(,(car attrib)
		       ,(optimize (cadr attrib) env bound opts))
		     (begin (codewarning (cons* '|BadAttribExpression| attrib))
		       attrib))))))
	(else attribs)))

(define (optimize-markup handler expr env bound opts)
  `(,(car expr) ,@(optimize-exprs (cdr expr))))
(define (optimize-markup* handler expr env bound opts)
  `(,(car expr) ,(optimize-attribs (second expr) env bound opts)
    ,@(optimize-exprs (cddr expr))))

(define (optimize-emptymarkup fcn expr env bound opts)
  `(,(car expr) ,@(optimize-attribs (cdr expr) env bound opts)))

(define (optimize-anchor* fcn expr env bound opts)
  `(,(car expr) ,(optimize (cadr expr) env bound opts)
    ,(optimize-attribs (third expr) env bound opts)
    ,@(optimize-exprs (cdddr expr))))

(define (optimize-xmlblock fcn expr env bound opts)
  `(,(car expr) ,(cadr expr)
    ,(optimize-attribs (third expr) env bound opts)
    ,@(optimize-exprs (cdddr expr))))

(define (optimize-watch fcn expr env bound opts)
  #|
  (if (string? (cadr expr))
      `(,(car expr) ,(cadr expr) 
	,@(optimize-watch-clauses (cddr expr) env bound opts))
      `(,(car expr) ,(optimize (cadr expr) env bound opts)
	"%WATCH"
	,@(optimize-watch-clauses (cddr expr) env bound opts)))
  |#
  expr)

(define (optimize-watch-clauses clauses env bound opts)
  (let ((optimized '())
	(scan clauses))
    (while (pair? scan)
      (cond ((and (string? (car scan)) (null? (cdr scan)))
	     (set! optimized (cons (car scan) optimized))
	     (set! scan (cdr scan)))
	    ((string? (car scan))
	     (set! optimized (cons* (optimize (cadr scan) env bound opts)
				    (car scan)
				    optimized))
	     (set! scan (cddr scan)))
	    (else
	     (set! optimized (cons* (optimize (car scan) env bound opts)
				    (stringout (car scan)) optimized))
	     (set! scan (cdr scan)))))
    (reverse optimized)))

(add! special-form-optimizers %watch optimize-watch)
(add! special-form-optimizers %watchptr optimize-block)

;;; Declare them

(add! special-form-optimizers let optimize-let)
(add! special-form-optimizers let* optimize-let*)
(when (bound? letq)
  (add! special-form-optimizers letq optimize-let)
  (add! special-form-optimizers letq* optimize-let*))
(add! special-form-optimizers (choice lambda ambda slambda)
      optimize-lambda)
(add! special-form-optimizers 
      (choice set! set+! default! define)
      optimize-assign)

(add! special-form-optimizers
      (choice dolist dotimes doseq forseq)
      optimize-doexpression)
;;(store! special-form-optimizers dotimes optimize-dotimes)
;;(store! special-form-optimizers doseq optimize-doseq)

;;(add! special-form-optimizers quote optimize-quote)

(add! special-form-optimizers
      (choice do-choices for-choices filter-choices try-choices)
      optimize-doexpression)

(add! special-form-optimizers tryif optimize-tryif)
(add! special-form-optimizers and optimize-and)
(add! special-form-optimizers or optimize-or)
(add! special-form-optimizers try optimize-try)

(add! special-form-optimizers begin optimize-begin)
(add! special-form-optimizers if optimize-if)
(add! special-form-optimizers when optimize-when)
(add! special-form-optimizers unless optimize-unless)

(add! special-form-optimizers while optimize-while)
(add! special-form-optimizers until optimize-until)

(add! procedure-optimizers {+ - * /} optimize-numeric)
(add! procedure-optimizers choice optimize-choice)

(when (bound? ipeval)
  (add! special-form-optimizers
	(choice ipeval tipeval)
	optimize-block))
(add! special-form-optimizers case optimize-case)
(add! special-form-optimizers evaltest optimize-evaltest)
(add! special-form-optimizers cond optimize-cond)
(add! special-form-optimizers do-subsets optimize-dosubsets)
(when (bound? parallel)
  (add! special-form-optimizers
	(choice parallel spawn)
	optimize-block))

(add! special-form-optimizers
      (choice printout lineout stringout message notify %wc)
      optimize-block)

(add! special-form-optimizers unwind-protect optimize-unwind-protect)

(add! special-form-optimizers
      {"ONERROR" "UNWIND-PROTECT" "DYNAMIC-WIND"}
      optimize-block)
(add! special-form-optimizers {"FILEOUT" "SYSTEM"} optimize-block)
(add! special-form-optimizers {dbg dbgeval} optimize-block)

(add! special-form-optimizers 
    ({procedure-name (lambda (x) x) 
	(lambda (x) (string->symbol (procedure-name x)))}
       quasiquote)
      optimize-quasiquote)

(add! special-form-optimizers logmsg optimize-logmsg)
(add! special-form-optimizers logif optimize-logif)
(add! special-form-optimizers logif+ optimize-logif+)

(add! special-form-optimizers
      with-log-context
      optimize-block)

(add! special-form-optimizers
    with-lock
  optimize-block)

;;(add! special-form-optimizers doseq optimize-doseq)

(add! special-form-optimizers getopt optimize-block)
(add! special-form-optimizers testopt optimize-block)

(add! special-form-optimizers (list {"with/request" "with/request/out"} 'webtools) optimize-block)
(add! special-form-optimizers '("markupblock" webtools) optimize-markup)
(add! special-form-optimizers (list {"markup*block" "markup*"} 'webtools) optimize-markup*)
(add! special-form-optimizers '("emptymarkup" webtools) optimize-emptymarkup)
(add! special-form-optimizers '{("anchor" xhtml) ("anchor*" xhtml)} optimize-anchor*)
(add! special-form-optimizers '("xmlblock" webtools) optimize-xmlblock)
(add! special-form-optimizers (list {"xmlout" "xhtml" "xmleval"} 'webtools) optimize-block)

(when (bound? fileout)
  (add! special-form-optimizers
	(choice fileout system)
	optimize-block))

;;;; Showing warnings

(define (optimize/list-warnings (module #f))
  (if module-warnings
      (let ((all-warnings (if module
			      (get module-warnings
				   {module (get (pick module table?) '%moduleid)})
			      (getvalues module-warnings))))
	(do-choices (warnings all-warnings)
	  (let ((modname (pick (get warnings '%moduleid) symbol?))
		(modsource (pick (get warnings '%moduleid) string?)))
	    (logerr |Optimize|
	      "in " (or modname modsource) (when modname (printout " (" modsource ")"))
	      (do-choices (fn (reject (getkeys warnings) symbol?))
		(printout
		  "\n  For " (or (procedure-name fn) fn)
		  (if (ambiguous? (get warnings fn))
		      (do-choices (warning (get warnings fn))
			(printout "\n\t" (doseq (elt warning i) (printout (if (> i 0) " ") elt))))
		      (doseq (elt (get warnings fn) i) (printout " " elt)))))))))
      (logwarn |NoLog| "Module warnings are not being logged")))

(define (optimize/count-warnings (module #f) (total 0))
  (if module-warnings
      (let ((all-warnings (if module
			      (get module-warnings
				   {module (get (pick module table?) '%moduleid)})
			      (getvalues module-warnings))))
	(do-choices (warnings all-warnings)
	  (do-choices (fn (reject (getkeys warnings) symbol?))
	    (set! total (+ total (|| (get warnings fn))))))
	total)
      (error |NoOptimizeLog|)))
