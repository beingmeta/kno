(in-module 'chopper/en)

(load-component "english.scm")

(module-export!
 '{
   closed-class-word?
   dictionary fragments
   noun-root verb-root
   tagit traceit tag-english})
