This is a draft blueprint for a FramerD package system.

FramerD packages consist of modules or sets of modules and submodules which are bundled together to be installed or updated at once.  While there will be a programmatic way to do everything with modules, this specification will be in terms of the command line program 'fdpkg'.

Packages all end up being installed along FramerD's module search path, though there might be some options for modifying the global search path.  The search path currently has three locations, considered in order:

1. Local modules (/usr/share/framerd/modules/local/)
2. Shared modules (/usr/share/framerd/modules/shared/)
3. Builtin modules (/usr/share/framerd/modules/builtin/)

The **builtin modules** are installed by the operating system's local packaging software and were typically bundled with the binary release of FramerD.

The **shared modules** are modules whose "authority" isn't local.  They might be links to file system shares or direct checkouts from git, subversion, s3, or other repositories.

The **local modules** are always local to the machine and are typically symbolic links or copies.

For **fdpkg**, we might add a new element in the search path **installed modules** (/usr/share/framerd/modules/installed/) for modules which are local snapshots installed by fdpkg.  However, for the time being, we'll just have fdpkg create **local modules**.

In any of these locations, a module named *modname* (where *modname* may contain slashes) might be loaded from any of the locations (on order):
1. location/*modname*/module.scm
2. location/*modname*/*modname*.scm (yes, *modname* appears twice)
3. location/*modname*.scm

Note that the usual practice is for a composite module (i.e. one which has its own directory) is to have the main module file be *modname*/*modname*.scm and for *modname*/module.scm to be linked to that file.

The general form of fdpkg commands are:

fdpkg install <pkgspec>
fdpkg upgrade <pkgspec>
fdpkg install <modname>
fdpkg upgrade <modname>
fdpkg remove  <modname>
fdpkg info    <modname>

*modname* is a FramerD module name e.g. **aws/roles** (separated by slashes without a leading slash).

*pkgspec* can be a remote repository reference (initially, git or subversion), an S3 path, a URL, or a file system path.

INSTALLATION
============

Installing with a direct module argument somehow looks up the module name in a directory to get a *pkgspec* and installs that.
Installing with a *pkgspec* makes a local directory (usually beneath /usr/share/framerd/packages/) containing the contents of the referenced package (this is the installation directory) and then makes symlinks from the installation directory into /usr/share/framerd/modules/local/.

For git and subversion *pkgspec*s, the remote reference is just cloned or checked out into installation directory.  For S3 paths, the S3 subtree is copied down.  For URLs or file system references to ZIP/TAR files, the file is extracted into the installation directory.

For other file system references, installation copies the file or directory.

The name of the installation directory has the form *installname*-*isodate* or *installname*-*isodate*-*convertedpkgspec*, where:
* *installname* is derived from the *pkgspec* (for example, the git repository name or name+branch) in some consistent way;
* *convertedpkgspec* is a copy of the package spec where all punctuation is turned into underscores.

INSTALLATION DIRECTORY STRUCTURE
================================

An installation directory has a subdirectory _fdpkg which contains different kinds of metadata about the package.  Installation itself writes the original *pkgspec* (making relative file system paths absolute) into
**_fdpkg/source**.  If it's a bundled file (.zip, .tar, etc), the MD5 of the file data is stored in **_fdpkg/sourcehash**.  Other files written to this directory are **_fdpkg/installed** and **_fdpkg/upgraded** containing iso timestrings.

Other information may be supplied from the package source with semantics to be determined later, especially versioning and dependencies.

UPGRADING
=========

Upgrading finds the installation directory for the package and updates it.  In the case of repositories (git/subversion/etc), it does a pull or update; in the case of S3 paths, it does a sync;  in the case of zip files, it compares hashes and unzips the file again.

After this is done, the links between the module location and the installation directory are updated, including the deletion of links to files which no longer exist and the creation of links to new files.

GETINFO
=======

This describe the package using the _fdpkg subdirectory of the installation directory and whatever other information it can find.







