# The Multibag BagIt Profile

Version 0.5

__Contents__
* [Overview](#Overview)
* [The Multibag Data Structure](#The_Multibag_Data_Structure)
  * [Bag and File Name Restrictions](#Bag_and_File_Name_Restrictions)
* [Multibag Metadata Elements](#Multibag_Metadata_Elements)
* [The Multibag Tag Directory](#The_Multibag_Tag_Directory)
  * [The `member-bags.tsv` File](#The_member-bags.tsv_File)
  * [The `file-lookup.tsv` File](#The_file-lookup.tsv_File)
  * [The `deleted.txt` File](#The_deleted.txt_File)
  * [The `aggregation-info.txt` File](#The_aggregation-info.txt_File)
* [Multibag Aggregation Updates](#Multibag_Aggregation_Updates)
* [Combining Multibags Into a Single Bag](#Combining_Multibags_Into_a_Single_Bag)
* [Specification Changes](#Changes)

<a name="Overview"></a>
## Overview
_This section is non-normative._

This specification describes a general-purpose profile on the BagIt standard for splitting a data aggregation across multiple bags which we refer to as the _Multibag_ profile.  One key motivation for splitting an aggregation over several bags is to make it easier to handle very large aggregations in storage or transmission environments that would otherwise place a limit on the size of a bag.  The BagIt specification already supports this basic functionality; this profile expands on this functionality to accomplish the following goals:
   * provide a standard recipe for combining all bags representing a single logical aggregation into a single compliant bag.
   * support non-destructive updates to a bag aggregation by allowing one to create a new aggregation that combines an existing set of bag aggregations with an "errata" or "update" bag that contains only the files that have changed.

A bag that is compliant with the Multibag profile supports the following:
   * the standard BagIt structures
   * an additional set of Multibag fields in the `bag-info.txt` file
   * a multibag-specific tag directory containing tag files specified by this document
   * restrictions on bag and data payload filenames

When the Multibag profile is used to handle large data aggregations, the individual files are distributed across two or more bags (referred to in this document as a _Multibag aggregation_).  This profile defines how the multiple bags can be recombined to create a single coherent bag containing the aggregation.  On the other hand, often an application will want to extract only one or a few files from the aggregation and, thus, would prefer to avoid retrieving, transmitting, and/or unpacking all of the bags in the aggregation.  To enable this, one of the bags is known as the _Head Bag_; it contains both a listing of all of the other bags in the Multibag aggregation as well as a file lookup list for locating individual files.  Thus, to retrieve a subset of the files within the aggregation, an application would first retrieve and unpack the Head Bag (which can be made to be quite small) to consult its lookup list, and then retrieve and open only the member bags that contain the desired files.  

> Note that this version of the Multibag Profile does not address the problem of how to split up and distribute a single large file among multiple bags.  This is planned to be addressed in a future version.

It is intended that the Multibag profile be used for dataset preservation: all the files that are part of the dataset are stored into a repository as a Multibag aggregation.  Often it is necessary to later make updates to the dataset, and for many reasons it would not be desirable to recreate the entire aggregation, particularly if it is important to retain the earlier versions and when the dataset is large:
   * A small change to the dataset would mean replicating a large amount of unchanged data.
   * It may be expensive (in computing resources) to retrieve and re-save the aggregation from and to the preservation storage.

To address this problem, this profile supports a mechanism for _non-destructive updates_.  It allows a repository to create "errata" or "update" bags which contain only those parts of the dataset that have changed.  These are combined with the previous bags to create a new Multibag aggregation and a new version of the dataset.  All previous versions of the dataset can be reconstituted by retrieving the proper Head Bag for that version.  

<a name="The_Multibag_Data_Structure"></a>
## The Multibag Data Structure

_This section is normative._

A Multibag aggregation is a set of one or more bags that are each compliant with this specification.  The full set of files under the `data` directories in each of the bags represent a logical aggregation of data that can (in principle) be combined into a single bag, given the rules in the section, [Combining Multibags Into a Single Bag](#Combining_Multibags_Into_a_Single_Bag).  One of the bags in the set is designated the _Head Bag_.  This contains metadata that identifies the other bags that make up the aggregation as well as which bags contain which files.  

When a Multibag aggregation is being created to capture a large file aggregation, each bag would normally contain a different subset of the files; in this case, typically, no two bags would contain file under the `data` directory with the same filepath.  How the `data` files are distributed amongst the bags in this use case is the choice of the creating application.  When a Multibag aggregation is supporting non-destructive updates (discussed in section, [Multibag Aggregation Updates](#Multibag_Aggregation_Updates)), files with the same path MAY appear in different bags.  These files are considered different versions of the same file.  When the bags are combined to create a single bag (according to the rules given in the section, [Combining Multibags Into a Single Bag](#Combining_Multibags_Into_a_Single_Bag)), only the latest version of the files is retained.  Regardless of the reason for there being multiple files with the same path in different member bags, all the files in the aggregation MUST be distributed such that applying the rules in section, [Combining Multibags Into a Single Bag](#Combining_Multibags_Into_a_Single_Bag), produces a compliant BagIt bag that properly represents the full file aggregation.  

The multibag profile does not specify how one determines a bag's
membership in an aggregation without opening up and examining the Head
Bag's metadata.  Further, this profile does not define a means for
identifying the Head Bag of an aggregation without opening and
examining bags.  Applications may apply bag naming conventions to
accomplish this; however, the convention should take into account
_(1)_ the restrictions on bag names (see next subsection, [Bag and
File Name Restrictions](#Bag_and_File_Name_Restrictions)), and _(2)_
the mechansim for non-destructive updates (see section, [Multibag
Aggregation Updates](#Multibag_Aggregation_Updates)).   

Note that a bag can be part of multiple bag aggregations
simultaneously.  Specifically, Multibag's mechanism for
non-destructive updates leverages this feature.  A bag can only be the
Head Bag for one Multibag aggregation.

<a name="Bag_and_File_Name_Restrictions"></a>
### Bag and File Name Restrictions

_This section is normative._

To facilitate a simple format for the Multibag tag files (see section,
[The Multibag Tag Directory](#The_Multibag_Tag_Directory)) that is
straight-forward to parse but yet supports file names with embedded
spaces, this specification places the follow restrictions on both the
names of the component bags and the files and directories that appear
under `data` directory:

* A name must not contain embedded TAB characters.
* A name must not begin or end with any whitespace character

<a name="Multibag_Metadata_Elements"></a>
## Multibag Metadata Elements

_This section is normative._

A bag compliant with the Multibag profile MUST contain a `bag-info.txt` file as defined by the BagIt standard.  The Multibag profile defines several metadata elements that can appear in the `bag-info.txt` file using the standard BagIt tag syntax.  The names of the elements and the meanings of their values are as follows:

<dl>
   <dt> <code>Multibag-Version</code> </dt>
   <dd> The version of the Multibag profile specification that the bag conforms to. The version described by this document is 0.4. </dd>

   <dt> <code>Multibag-Reference</code> </dt>
   <dd> A URL pointing to Multibag specification referred to in the <code>Mulibag-Version</code> element. </dd>

   <dt> <code>Multibag-Tag-Directory</code> </dt>
   <dd> the path relative to the bag's base directory to the multibag-specific tag directory. </dd>

   <dt> <code>Multibag-Head-Version</code> </dt>
   <dd> the version of the bag aggregation that the current bag is the head bag for (see notes below). </dd>

   <dt> <code>Multibag-Head-Deprecates</code> </dt>
   <dd> one or two tokens, separated by a comma, where the first field is the version of another Multibag aggregation that the current aggregation deprecates, and the second field is the name of the head bag for the deprecated aggregation. </dd>
<dl>

The use of these metadata elements are subject to the following constraints:

   * A bag that conforms to the Multibag profile MUST include the `Multibag-Version` metadata element.

   * The bag that represents the Head Bag MUST include the `Multibag-Head-Version` metadata element; its value is a version identifier for the Multibag aggregation it is the Head Bag for. As described in the section, [Multibag Aggregation Updates](#Multibag_Aggregation_Updates), an update to the aggregation will be represented by a new Head Bag; the version provided in the `Multibag-Head-Version` metadata element in the new Head Bag MUST be different from that in Head Bags for all previous versions.

   * Only Head Bags SHOULD contain the `Multibag-Head-Deprecates` metadata element. It MAY be provided multiple times to identify multiple previous versions of the aggregation. In general, the element is optional, regardless of the existence of previous versions.

   * The `Multibag-Tag-Directory` is optional, and it should only appear if the bag represents the Head Bag. If the bag is a Head Bag but this element is not provided, the path to the multibag-specific tag directory will be assumed to be simply, 'multibag'.

   * Including the `Multibag-Reference` metadata element is optional.

In addition to including the Multibag-specific metadata, the `bag-info.txt` file SHOULD also include the `Bag-Group-Identifer` element, set to an identifier referring to the Multibag aggregation as a whole.  The elements, `Internal-Sender-Identifier` and `Internal-Sender-Description`, are also recommended for inclusion.  It is recommended that the `Internal-Sender-Description` represent a description of the aggregation as a whole.  

It is recommended that the `bag-info.txt` _not_ include the `Bag-Count` element.  (This element gives the total number of bags in its group and the position the bag in the group.)  Because a bag can be a member of multiple Multibag groups (and different versions of the group), there could be contexts where its value is not correct.  In particular, updating a Multibag aggregation via the mechanism given in section, [Multibag Aggregation Updates](#Multibag_Aggregation_Updates), will invalidate the `Bag-Count` value.  

### Example `bag-info.txt`

_This section is non-normative._

_in progress: fill in example, explanation_
<verbatim>

</verbatim>

<a name="The_Multibag_Tag_Directory"></a>
## The Multibag Tag Directory

_This section is normative._

A Multibag Head Bag MUST contain a special directory located outside of the `data` directory which is referred to as a _Multibag Tag Directory_.  The location of this directory is given as the value of the `Multibag-Tag-Directory` metadata element in the `bag-info.txt` file (see section, [Multibag Metadata Elements](#Multibag_Metadata_Elements)) as a file path relative to the bag's base directory; if the element is not given, the location of MUST be a directory directly within the bag's base directory called `multibag`.  

The directory MUST contain two files called `member-bags.tsv` and
`file-lookup.tsv`, respectively.  The directory MAY also contain a file
called `deleted.txt` and/or a file called `aggregation-info.txt`.  The
directory may contain other files, but applications that support this
profile can ignore them to properly interact with bags in the bag
aggregation.  In the case in which a multibag aggregation is created
by splitting up a single bag, the source bag's `bag-info.txt` file
may be saved as the `aggregation-info.txt` file in the resulting Head
Bag.  

<a name="The_member-bags.tsv_File"></a>
### The `member-bags.tsv` File

_This section is normative._

The purpose of the `member-bags.tsv` file is to allow applications to know which other bags belong to a Multibag aggregation by examining the aggregation's Head Bag.  It also allows the application to retrieve the other member bags from remote locations if they are so available.  

The `member-bags.tsv` is a text file that lists the names of the bags
that make up the Multibag aggregation.  Each line of the file contains
one or more TAB-delimited fields and has the format:

```
BAGNAME[\tURL][\t...][\t# COMMENT]
```

where "\t" is a single TAB character.  The first field, BAGNAME,
is the name of a bag that is part of the aggregation.  This name
should match the name of the bag's base directory; it should not
matched a serialized form of the bag.  The name may be followed by one
or space characters (i.e. before a delimiting TAB character); these
must not be considered part of the bagname.  

The second field, if provided, is an absolute URL from which a serialized copy
of the bag can be retrieved.  This document does not specify how one
can deserialize the file stream into the named bag; however, providers
should employ common conventions such as filename extensions
(e.g. ".zip") or HTTP content-types (e.g. "application/zip") to
indicate the necessary mechanism for recreating the member bag.  The
URL field is optional; when it is not provided, applications must rely
on other information not specified by this document for determining
where and how to retrieve the member bag referred to by the name.

Additional fields may appear after the URL field.  If additional
fields are provided but a URL is not applicable or known, the second
field must be an emtpy string (that is, two consecutive TAB characters
will appear after the BAGNAME).  The form and meaning of the
additional fields beyond the URL are not specified by this document,
with except for the following:  a column that begins with a pound sign
('#') and a space indicates the start of a human-readable comment, and
all subsequent characters in that line can be considered part of
that comment.  

The order that bags are listed in the file is significant to the mechanism for combining Multibag bags into a single bag, as described in section, [Combining Multibags Into a Single Bag](#Combining_Multibags_Into_a_Single_Bag).  The last bag named in the list MUST be the Head Bag for the aggregation.  

#### Example

_This section is non-normative._

<a name="The_file-lookup.tsv_File"></a>
### The `file-lookup.tsv` File

_This section is normative._

The purpose of the `file-lookup.tsv` file is to allow applications to locate individual files across all of the aggregation's bags without having to open and examine all of the bags; rather, an application need only open the HeadBag to discover a file's location.

The `file-lookup.tsv` file is a text file that lists the names of
files available in the union of all the bags that make up the Multibag
aggregation with an indication of in which bag the file is stored.
Each line of the file contains two fields separated by a single TAB
character; that is, each line has the format:

```
FILEPATH\tBAGNAME
```

where "\t" is a TAB character, FILEPATH is the path to the file
relative to the bag's base directory, and BAGNAME is the name of the
bag in the aggregation that the file is located in.  One or more space
characters may appear before or after the TAB; these must not be
considered part of the FILEPATH or the BAGNAME fields.  

Creators of Multibag-compliant bags should include in the `file-lookup.tsv` lising all files that users might want easy access to--i.e. the ability to extract an individual file from its enclosing bag without having to potentially unserialize and search all of the bags in the aggregation.  All files under the `data` directories in all of the bags SHOULD be listed in the file.  Other metadata or tag files outside of the `data` directories MAY be listed as well.  

<a name="The_deleted.txt_File"></a>
### The `deleted.txt` File

_This section is normative._

The purpose of the `deleted.txt` file is to list the files that may be
included in any of the aggregation's member bags but which should not
be considered part of the aggregation.  This is useful when the Head
Bag represents an update to previous version of the aggregation. (In
this case, the new Head Bag would typically be created much later than
other member files; see section, [Multibag Aggregation
Updates](#Multibag_Aggregation_Updates).)  The `deleted.txt` file can
indicate that files have been removed, rather than updated, in the
new version.

Each non-blank line in the file has the format,

```
FILEPATH
```

where FILEPATH is the path, relative to the root directory of the bag,
of a deleted file.  See section, [Combining Multibags Into a Single
Bag](#Combining_Multibags_Into_a_Single_Bag), for details on how this
file should be used when creating the aggregated bag.

It is recommended that the file paths listed in the `deleted.txt` file
only refer to files that appear in (at least) one of the aggregation's
member bags (as listed in the `member-bags.tsv` file); however, this
is not required.

<a name="The_aggregation-info.txt_File"></a>
### The `aggregation-info.txt` File

_This section is normative._

Each bag in a multibag aggregation must be a legal bag itself and thus
must have a `bag-info.txt` file that describes itself as a standalone
bag.  Consequently, it can be ambiguous as to what the info metadata
should be for the aggregation as a whole, particularly in the event that
multibags are combined into a single coherent bag.  One of the means
for determining aggregation's info metadata described in the section,
["Combining Multibags Into a Single
Bag"](#Combining_Multibags_Into_a_Single_Bag), is via a an
`aggregation-info.txt` file saved in the `multibag` directory of the
aggregation's Head Bag.

The format of the optional `aggregation-info.txt` file is exactly the
same as the `bag-info.txt` file and is subject to the same
requirements; its contents respresent the bag-info metadata for the
aggregation as a whole (see also section, [Combining Multibags Into a
Single Bag](#Combining_Multibags_Into_a_Single_Bag)).


<a name="Multibag_Aggregation_Updates"></a>
## Multibag Aggregation Updates

_This section is normative._

This section describes how to update Multibag aggregation while retaining full access to the un-updated version without recreating the entire aggregation.  The updating application creates one or more new Multibag-compliant bags that contain new files to be added to the aggregation or new versions of files contained in the previous aggregation; these files can be located anywhere in the bag--i.e. either inside the `data` directory or out.  A new file is one whose path does not match any of the files within the previous aggregation; a new version of file is one that does match a file within the previous aggregation.  

One of the new bags is designated as the Head Bag for the updated version of the aggregation; it MUST meet all of the requirements of a Head Bag.  It SHOULD include in its `bag-info.txt` file the `Multibag-Head-Deprecates` metadata element, identifying the Head Bag of the previous aggregation that it replaces.  The new Head Bag MAY also replicate the `Multibag-Head-Deprecates` metadata occurrences in the deprecated Head Bag so as to reference even earlier versions of the aggregation.   

The `member-bags.tsv` file in the new Head Bag MUST list all of member bags of the previous aggregation that contain data that is to be part of the new aggregation.  The file is allowed to not include one, some, or all of the bags from the previous aggregation if none of their files should be included or that they would otherwise be replaced in the new aggregation.  The order of listing of the bags from the previous aggregation's `member-bags.tsv` file MUST be the preserved in the new one.  The new bags MUST be listed after the bags from the previous aggregation, and the new Head Bag MUST be listed last.  The order of the new bags (that are not Head Bags) must take into consideration the rules for [Combining Multibags Into a Single Bag](#Combining_Multibags_Into_a_Single_Bag).  

The `file-lookup.tsv` file in the new Head Bag SHOULD list all of the files from the previous Head Bag's `file-lookup.tsv` file.  Files that are not considered part of the new aggregation MAY be absent from this file; however, a file's absence should not be taken as an indication that the file has been deleted as part of the update.  

If any of the member bags that make up the new aggregation contain
files that should not be considered part of the new aggregation, then
the the new Head Bag must contain a `deleted.txt` file, and the paths
to those files must be listed within it according to the format
describe in the section, [The `deleted.txt` File](#The_deleted.txt_File).
This is a definitive means for indicating that a file is being removed
from an aggregation as part of an update.  If the previous
aggregation's Head Bag contained a `deleted.txt` file, all paths from
that previous file must appear in the `deleted.txt` file for the new
Head Bag, excepting those that are being returned to the aggregation
as part of the update.

Note that the other way to remove files from an aggregation as part of
an update is to not include the member bags that contain the files in
the new `member-bags.tsv` file.  This technique effectively removes _all
of the files_ contained in the bags not included; in this case, a
`deleted.txt` file is not required.  Use of the `deleted.txt` file,
then, is typically a more efficient way to remove individual files
from the aggregation.


<a name="Combining_Multibags_Into_a_Single_Bag"></a>
## Combining Multibags Into a Single Bag

_This section is normative._

It must always be possible, in principle, to combine all of the bags in a Multibag aggregation into a single BagIt-compliant bag (barring storage and compute resource limitations) by following the process detailed in this section.  Other methods that produce the same end result may be used; however, this method defines the end-result.  The `member-bags.tsv` file lists the member bags in an aggregation in the order which they must be combined.  

An application MUST be able to combine a Multibag aggregation into a single bag by following the these steps:

   1. The application retrieves the aggregation's Head Bag and extracts the `member-bags.tsv` file.
   1. The application retrieves the first bag listed in the file, unserializes it (if necessary), and copies it to a location in storage where the final single bag is to be assembled. The directory structure of the bag is retained in the copy.
   1. The application retrieves and unserializes (if necessary) each
   subsequent bag in the list, in order, and unpacks or copies its
   contents into the same storage location, retaining the bag's
   directory structure. In this process, updated versions of files MAY
   overwrite deprecated versions (with the exception of the
   BagIt-specific files, `bagit.txt`, `bag-info.txt`, `fetch.txt` and
   the manifests, which must be handled separately). 
   1. The special BagIt-specific files for the combined bag should be reconstituted according to the following rules:
      <dl>
          <dt> 4.1. <code>bagit.txt</code> </dt>
          <dd> The aggregated bag should have a <code>bagit.txt</code>
               that matches that of the aggregation's Head Bag;
               however, this is not required.  The application is
               responsible for ensuring that all tag files employ the
               coding indicated by the <code>bagit.txt</code> file's
               <code>Tag-File-Character-Encoding</code> field.  </dd>
          <dt> 4.2. <code>bag-info.txt</code> </dt>
          <dd> <p>If the Head Bag contains an
               <code>aggregation-info.txt</code> file in its Multibag
               tag directory (see section,
               <a href="#The_Multibag_Tag_Directory">The Multibag Tag
               Directory</a>), that file should be installed as the
               <code>bag-info.txt</code> file for the aggregated bag.  The
               application may make additional changes to file
               later.  In particular, the application may update
               the <code>Payload-Oxum</code> and <code>Bag-Size</code>
               fields to ensure their values are correct for the 
               aggregated bag.</p>
               <p>If the Head Bag <i>does not</i> contain an
               <code>aggregation-info.txt</code> file, an aggregated
               <code>bag-info.txt</code> file should be created via
               the follow steps:
               <ol>
                 <li> The contents of the <code>bag-info.txt</code> file 
                      from the first bag listed in the
                      <code>member-bags.tsv</code> file is considered the 
                      initial contents of the aggregated info tag data. </li>
                 <li> The tag data from the <code>bag-info.txt</code> file from
                      each subsequent bag listed in the
                      <code>member-bags.tsv</code> file are read in order.  
                      All remaining values associated with a given
                      field name then over-ride all previous values with the 
                      same name.  Tag data with names not previously 
                      encountered in this process are added to the aggregated 
                      tag data.  </li>
                 <li> After the data from the last bag (the Head Bag)
                      has been merged, all tag data from the excepted list 
                      should be removed; the excepted list includes
                      <code>Bag-Count</code>, <code>Payload-Oxum</code>,
                      <code>Bag-Size</code>, and any tag with a 
                      name starting with <code>Multibag-</code>.  
                 <li> The application may make additional changes to
                      the tag data.  In particular, if it is desired
                      that the aggregated bag be a BagIt-compliant
                      bag, the <code>Payload-Oxum</code> and
                      <code>Bag-Size</code> tags should be added to reflect 
                      the aggregated bag.
                 <li> The application should add a tag called
                      <code>Multibag-Rebagging-Date</code> set to the current 
                      date.  </li>
                 <li> The tag data that results from the above steps
                      should be written out as the aggregated bag's
                      <code>bag-info.txt</code> file. </li>
               </ol></p>
               </dd>
          <dt> 4.3. <code>fetch.txt</code>
          <dd> The aggregated bag's <code>fetch.txt</code> file must
               be a merging all the <code>fetch.txt</code> files in
               the bags listed in the <code>member-bags.tsv</code>
               file according to the following process: 
               <ol>
                 <li> The contents from the first bag listed in the
                      <code>member-bags.tsv</code> file is taken as the
                      initial fetch list for the aggregated bag. </li>
                 <li> The contents of <code>fetch.txt</code> file from
                      each subsequent bag listed in the
                      <code>member-bags.tsv</code> file are read in 
                      order.  Any line with a file path that matches 
                      that of a line from a previously read file 
                      over-rides that previous line.  </li>
                 <li> After the <code>fetch.txt</code> lines from the
                      last bag (the Head Bag) has been merged, any
                      paths listed in the Head Bag's <code>deleted.txt</code>
                      file must be removed from the resulting fetch list. </li>
                 <li> The remaining fetch list must be written out as
                      the aggregated bag's <code>fetch.txt</code> file.  </li>
               </ol></dd>
          <dt> 4.4. <code>manifest-</code><i>alg</i><code>.txt</code>, <code>tagmanifest-</code><i>alg</i><code>.txt</code> </dt>
          <dd> A manifest file for the aggregated bag, for each
               algorithm represented, must be the union of the same
               manifest files (type and algorithm) of all of the
               member bags but excluding the files listed in the
               <code>deleted.txt</code> file.   </dd>
      </dl>
   1. The `multibag` tag directory should be removed.
   1. If the aggregated bag is to be Bagit-compliant, the `bag-info.txt` file should be updated to reset
      the `Payload-Oxum` and `Bag-Size` tags to values applicable to the aggregated bag.  
      
Other BagIt profiles may specify rules for reconstituting other tag files from versions in the member bags.  In the absence of such rules, applications should assume assume that versions in the member bags listed later should replace those listed earlier. 

Previous versions of a Multibag aggregation may be assembled into a single bag by consulting a Head Bag's `Multibag-Head-Deprecates` metadata (in its `bag-info.txt` file) and retrieving the Head Bag of the previous version that the element refers to; the application can then follow the above steps with the deprecated Head Bag.  

<a name="Changes"></a>
# Specification Changes

The initial draft was part of a comprensive document that also
described the NIST Preservation BagIt Profile (version 0.2).  The
Multibag component was spun off to create its verison 0.2.  

## Since 0.2

   * `group-members.txt` and `group-directory.txt` were changed to
     `member-bags.tsv` and `file-lookup.tsv`, respectively.
     * Switching to TSV format was necessary because parsing these files
       became ambiguous when the file paths or bag names contains
       spaces (which is not disallowed by the core BagIt spec).  TSV
       provides a means for unambiguous retrieval of names without 
       custom parsing; however, tabs are disallowed as part of
       filenames and bagnames.  
     * The base names were changed as some reviewers found the names
       not obvious as to their purpose.
   * The format change for the above mentioned tag files necessitated
     adding restrictions on the names of the component bags and the
     files that appear under the data directory.  
   * The steps for [Combining Multibags Into a Single
     Bag](#Combining_Multibags_Into_a_Single_Bag) were expanded to
     explain how to create the final versions of the BagIt tag files.

## Since 0.3

   * The `deleted.txt` and `aggregation-info.txt` files are defined.
   * The rules for setting the contents of the special BagIt files
     (`bagit.txt`, `bag-info.txt`, `fetch.txt`, and the manifests) are
     spelled out.

## Since 0.4

   * The algorithm for "Combining Multibags Into a Single Bag" was
     adjusted to account for the presence of a `deleted.txt` file.  

## To do

   * change `member-bags.tsv` specification to optionally included PID
   * address how to split (large) individual files among multiple
     member files.
