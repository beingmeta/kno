(use-module 'brico)

(applytest @1/2c1c7 ?? '%id 'en)
(applytest #t overlaps? @1/175a9 (?? @?en "dog"))
(applytest @1/175a9 ?? @?en "dog" @?en "domestic dog")
(applytest @1/175a9 ?? @?en "dog" @?genls* (?? @?en "animal"))
(applytest @2/100266c ??
	   @?en "Rockville" @?partof (?? @?en "Maryland")
	   'sensecat 'noun.location)
(applytest @2/100266c ??
	   @?en "Rockville" @?partof* (?? @?en "Maryland")
	   'sensecat 'noun.location)
(applytest @2/20057be ?? @?en "Madonna" @?en "Louise Veronica Ciccone")
(applytest @2/20057be ?? @?en "Madonna" @?implies (?? @?en "singer"))
(applytest #t ambiguous? (?? @?en "Madonna" @?implies (?? @?en "album")))
(applytest @2/201e13b ?? @?en "Madonna" @?implies (?? @?en "album")
	   @?defterms
	   @/namedb/7e8dc(NOUN.GROUP "Warner Bros. Records" ISA "company"))
(applytest #t overlaps? (?? @?en "animal") (?? @?specls* @1/175a9))


