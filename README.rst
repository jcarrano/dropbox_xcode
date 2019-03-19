=====================================
(Unofficial) Dropbox Music Transcoder
=====================================

----------------------------------------------
Download and transcode FLAC files from Dropbox
----------------------------------------------

What is this
============

This is a script that recursively downloads whole directory trees from Dropbox
and converts FLAC files on-the-fly to OPUS.

This way, you can keep your precious FLAC files in the "cloud" and still have
your music fit in your HDD.

You will need to get an API key to use this.

Features
========

- Simultaneous downloading and transcoding FLAC to OPUS.
- Parallel downloads and parallel transcodes.

 - This means OPUS encoding with multiple CPUs.
 - Of course, the transcode step will be starved by the slower download speed.
 - Tries to make good use of bandwidth and CPU (this statement is probably false).

- Avoids downloading the same file twice:

 - Checks modification date, size, hash of regular files.
 - For audio files, the duration of the track is checked
 - Dropbox does not know how to parse FLAC files for duration: this script
   updates the metadata of upstream files to add this field!

- Dry run.
- Fancy progress bars (via tqdm)

Also, check out the implementation of the "Dropbox Hash" and steal it.

Notes
=====

You will need to have ``opusenc`` (from ``opus-tools``) installed.

``opusenc`` does not like ID3 tags so this script removes them prior to
conversion. The information in the ID3 tags is **discarded**. In any case, you
should not be using id3 in FLAC files.

Why?
====

Turns out that I had:

- Too many FLAC files in dropbox
- A too tiny SSD.
- Too much time to waste writing a script instead of doing the reasonable thing
  and downloading to an external drive.

Final notes
===========

The Dropbox SDK API sucks. Very much sucks.
