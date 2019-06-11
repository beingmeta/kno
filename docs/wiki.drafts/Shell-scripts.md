# Implementing new commands with scripts

New command line directives can be implemented by KNO program files.
Under Unix, these should be marked as executable and start with a line
something like:
````
#!/usr/bin/kno
````
depending on where your local copy of KNO lives. The remaining lines are
KNO expressions evaluated to implement the specified command. If the
subsequent expressions define a procedure `main`, this procedure is applied to
the command line arguments to the script. E.G., suppose the file `square.scm`
contained the following text:
````scheme
#!/usr/bin/kno
;; This is the file square.scm
(define (square x) (* x x))
;; PARSE-ARG will convert a string to a number
(define (main x) (square (parse-arg x)))
````
we could use the file as a command from the shell:
````shell
sh% square.scm 10
100
````
providing that `square.scm` were set as executable.

The script can also access the arguments to the command through several
variables:
  * nargs is the number of arguments
  * args is a list of all the arguments
  * arg1, arg2, arg3, and arg4 are the first four arguments (if given)
these arguments are generally strings, which the function parse-arg will
convert to Lisp objects.

The default Kno installation installs a command `fdinstall-script` (which
is an KNO script) which puts the approriate `#!` line at the front of a
file and makes it executable. When called with two filename arguments, the
executable script is stored in the second filename and the source filename
(the first argument) is left untouched. Thus, we could create a simple
`square` command using our `square.scm` file:
````shell
sh% fdinstall-script square.scm square
sh% square 2000
4000000
````

Slighly more complex commands can provide command-line access Kno
databases. For instance, the following script finds WordNet senses based on a
word and a more general word
````scheme
#!/usr/bin/kno
;; This is the file find-sense.scm
(use-pool "brico@framerd.org") ; replace with local server
(define (main word category)
  (let ((candidates (find-frames "brico@framerd.org" 'words word))
	(super-senses (find-frames "brico@framerd.org" 'words category)))
    (do-choices (candidate (find-frames "brico@framerd.org" 'words
					word1))
      (if (value-path? candidate 'hypernym super-senses)
	  (lineout candidate)))))
````
which would work as follows:
````console
sh% knoinstall-script find-sense.scm find-sense
sh% find-sense dog animal
@/brico/f902("dog" "domestic_dog" "Canis_familiaris")
sh% find-sense dog person
@/brico/185c6("cad" "bounder" "blackguard" "dog" "hound" "heel")
@/brico/18651("dog")
@/brico/18b22("frump" "dog")
````    

