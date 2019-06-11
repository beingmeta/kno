# Macros

Kno's Dirty Macros
  
Kno provides a very simple macro facility for implementing syntactic
extensions of the core Kno language. When the value of a symbol is a list
of the form:

    
    
    (macro (expr) body...)
    

the evaluator uses body to preprocess all expressions starting with the
symbol. The expressions in body are evaluated in a "safe environment" where
only the basic Scheme/Kno functions are available and the variable expr
is bound to the top level expression being processed. For example:

    
    
    (define push
     '(macro (expr)
       `(set! ,(caddr expr) (cons ,(cadr expr) ,(caddr expr)))))
    

defines a version of Common LISP's `push` macro, used thus:

    
    
    #|kno>|# (define atoms '())
    #|kno>|# (push 'x atoms)
    #|kno>|# (push 'y atoms)
    #|kno>|# atoms
    (Y X)
    #|kno>|# (let ((nums '()))
                 (dotimes (i 5) (push i nums))
                 nums)
    (5 4 3 2 1)
    

