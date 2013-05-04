;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

;;; This does simple keytuple extraction from (currently) English
;;;  text, using a handful of simple rules.

(in-module 'gnosys/keytuples)

(define version "$Id$")

(use-module '{gnosys texttools tagger})

;;; Keytuple extraction

(define (get-keytuples parse)
  (if (string? parse)
      (get-keytuples (tagtext parse))
      (choice (getxkeys parse
			'{noun plural-noun solitary-noun proper-name}
			'{preposition adjective count-adjective proper-modifier
				      possessive proper-possessive}
			'{preposition})
	      (getxkeys parse
			'{noun noun-modifier}
			'{adjective possessive proper-possessive proper-modifier}
			'{})
	      (getxkeys parse
			'{adjective count-adjective}
			'{adverb}
			'{})
	      (getxkeys parse
			'{verb complement-adjective be-verb
			       ing-verb inflected-verb infinitival-verb}
			'{adverb negator modal-aux be-aux
				 noun plural-noun solitary-noun
				 proper-name}
			'{noun plural-noun solitary-noun preposition
			       trailing-adverb negator
			       proper-name}))))
(define (get-keylinks parse)
  (if (string? parse)
      (get-keylinks (tagtext parse))
      (choice (getxlinks parse
			 '{noun plural-noun solitary-noun proper-name}
			 '{preposition adjective count-adjective proper-modifier
				       possessive proper-possessive}
			 '{})
	      (getxlinks parse
			 '{noun noun-modifier}
			 '{adjective possessive proper-possessive proper-modifier}
			 '{})
	      (getxlinks parse
			 '{adjective count-adjective}
			 '{adverb}
			 '{})
	      (getxlinks parse
			 '{verb complement-adjective be-verb
				ing-verb inflected-verb}
			 '{adverb negator modal-aux be-aux
				  noun plural-noun solitary-noun
				  proper-name}
			 '{noun plural-noun solitary-noun preposition
				trailing-adverb negator
				proper-name}))))

(module-export! '{get-keytuples get-keylinks})
