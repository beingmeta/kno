# Working with files

## Exploring the Filesystem

Kno uses strings to represent files and directories in the file system.
The file system can be explored by the functions `GETFILES` and `GETDIRS`.
`GETFILES` takes a directory name and returns all of the files it contains;
`GETDIRS` also takes a directory name but returns all of the subdirectories it
contains. The following procedure gets all of the files recursively underneath
a particular directory, taking advantage of `getfiles`, `getdirs`, and
Kno's automatic non-determinism:

    
    
          (define (allfiles dir)
          (choice (getfiles dir)
          (allfiles (getdirs dir))))
        
    

These predicates can be applied to give information about a file given its
name:

  * `(file-exists? filename)` returns true if filename exists
  * `(file-writable? filename)` returns true if filename can be modified
  * `(directory? filename)` returns true if filename is a directory
  * `(symbolic-link? filename)` returns true if filename is a symbolic link
  * `(regular-file? filename)` returns true if filename is a regular file (not a directory or a symbolic link)

The following functions can be applied to pathnames to generate other
pathnames or components of pathnames:

  * `(fullname path)` returns a complete pathname (based at the file system root) given a relative pathname.
  * `(basename path)` returns a the final part of a pathname, with the directory component removed.
  * `(dirname path)` returns the initial part of a pathname, just the directory.
  * `(readlink path)` returns the target of a link or the file itself otherwise.

Other information about particular files can be determined with these
functions:

  * `(file-size filename)` returns the size (in bytes) of a regular file
  * `(file-access-time filename)` returns the last time a file was accessed, as a timestamp object
  * `(file-creation-time filename)` returns the time at which a file was created, as a timestamp object
  * `(file-modification-time filename)` returns the last time at which a file was modified, as a timestamp object
  * `(file-size filename)` returns the number of bytes comprising a file
  * `(file-owner filename)` returns a string describing the owner of filename

The predicate `(FILE-OLDER? file1 file2)` returns true if file1 is older than
file2.

## User-specific Information

  * `(get-user-data)`  
`(get-user-data username)`  
`(get-user-data numeric-userid)` returns information about a specified user,
defaulting to the current user.

    
                  #|kno>|# (get-user-data)
              #[UID 31406
              GID 501
              UNAME "haase"
              TEXT-DATA "Kenneth Haase"
              HOMEDIR "/local/haase"
              SHELL "/bin/bash"]
              #|kno>|# (get-user-data "root")
              #[UID 0 GID 0 UNAME "root" TEXT-DATA "root" HOMEDIR "/root" SHELL "/bin/bash"]
              #|kno>|# (get-user-data 0)
              #[UID 0 GID 0 UNAME "root" TEXT-DATA "root" HOMEDIR "/root" SHELL "/bin/bash"]
            
    

  * `(get-homedir)` returns the absolute pathname of the current user's home directory. 
    
                  #|kno>|# (get-homedir)
              "/local/haase"
            
    

