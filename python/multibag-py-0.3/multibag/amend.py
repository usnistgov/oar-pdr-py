"""
This module supports use of the multibag profile for amending an existing bag
or multibag aggregation.   It also can update an existing bag to convert it to
a head bag for a single-bag aggregation.
"""
import os, sys, re
from collections import OrderedDict

from .constants import CURRENT_VERSION, CURRENT_REFERENCE
from .access.bagit import Bag

if sys.version_info[0] > 2:
    _unicode = str
else:
    _unicode = unicode

from .access.multibag import as_headbag, MemberInfo, HeadBag
from .access.extended import as_extended
from .access.bagit    import open_bag

def make_single_multibag(bagdir, version="1", pid=None):
    """
    convert a traditional bag into a head bag for a single-bag aggregation
    (i.e. a multibag aggregation with only one bag in it).  

    :param str bagdir:   the path to the root directory of the bag to be 
                         converted.
    :param str version:  the version string to assign to the bag as the version
                         of the aggregation (default: "1").
    :param str pid:      a URI representing a PID to assign to the new bag.  
    """
    mkr = SingleMultibagMaker(bagdir)
    mkr.convert(version, pid)

class SingleMultibagMaker(object):
    """
    This class collects operations for turning a standard bag into a head 
    bag for a single-bag aggregation.

    The conversion can be done most easily by instantiating this class and 
    calling the convert() method.  This method calls write_member_bags(), 
    write_file_lookup(), and update_info() in sequence.  For greater control
    over this conversion--namely, to control which files are included in the 
    file-lookup.tsv file (by default, only the data payload files are 
    included)--one can call these latter method oneself.  See the documentation
    for the individual methods for more information.  
    """
    def __init__(self, bagdir, multibag_tagdir="multibag"):
        """
        Initialize the object that will convert a bag with a given root 
        directory.
        """
        if not os.path.exists(bagdir):
            raise OSError(2, "Directory not found: "+bagdir, bagdir)

        self.bagdir = bagdir
        self.bag = HeadBag(bagdir)
        if multibag_tagdir != 'multibag':
            self.bag.set_multibag_tag_dir(multibag_tagdir)

    def write_member_bags(self, pid=None, comment=None, *info):
        """
        write the member-bags.tsv file for a single-bag aggregation.  The 
        PID, if provided, should be set to resolve to this bag (usually a
        serialized form for it).
        :param str pid:  Persistent ID to associate with this bag.
        :raises OSError: if there is a failure to open the output file or
                         otherwise write the data.  
        """
        # set the info necessary for updating the member bag listing
        if not self.bag.info.get('Multibag-Version'):
            self.update_info();
        
        bagname = os.path.basename(self.bagdir)
        mi = MemberInfo(bagname, pid, comment, *info)
        self.bag.set_member_bags([mi])
        self.bag.save_member_bags()

    def write_file_lookup(self, include=None, exclude=None, trunc=False):
        """
        write the file-lookup.tsv file.  This function can be called multiple
        times as the file paths will be appended to the existing file-lookup.tsv
        unless trunc=True.  

        :param list include:  a list of directories or files that should be 
                              included.  If not provided, a default value of 
                              ['data'] will be assumed.  Each element must be 
                              a path relative to the bag's root directory.  If 
                              an entry is a directory, all files under that 
                              directory (recursively) will be included except
                              any whose path matches an entry in the exclude
                              list parameter.  
        :param list exclude:  a list of files or directories to exclude.  The
                              include list will over-ride those listed in this
                              list, unless the corresponding include entry is a
                              directory.
        :param boolean trunc: if False, append specified file paths to the end
                              of an existing file-lookup.tsv (creating the 
                              file if necessary); if True, discard any existing
                              content of the file-lookup.tsv before adding paths.
        """
        # set the info necessary for updating the file-lookup listing
        if not self.bag.info.get('Multibag-Version'):
            self.update_info();
        
        if trunc:
            self.bag.clear_file_lookup()

        self.bag.update_for_member(self.bag, include, exclude, False)
        self.bag.save_file_lookup()

    def update_info(self, version="1"):
        """
        update the bag info tag data to include head bag information.

        :param str version:  a version string to set as the version of the 
                             aggregation.  The default is "1".
        """
        self.bag.update_info(version)


    def convert(self, version="1", pid=None):
        """
        convert the attached bag to single multibag aggregation.  This calls, 
        in succession, write_member_bags(), write_file_lookup(), 
        and update_info() to complete the conversion.  

        :param str version:  a version string to set as the version of the 
                             aggregation.  
        :param bool payload_only:  if True, the file-lookup.tsv file will only 
                                   contain the payload files.
        """
        self.write_member_bags(pid)
        self.write_file_lookup(trunc=True)

        # This (re-)calculates Bag-Size due to the new multibag tag files
        self.update_info(version)


def amend_bag_with(amendee, amendment_head, version, *amendment):
    """
    update an existing multibag aggregation to add a new or update files.  This 
    defines a new head bag for the aggregation.  

    The bag aggregation to be updated (amendee) is represented either as a 
    traditional (legal) bag or as a head bag of multibag aggregation.  In the 
    former case, the bag simply represents a degenerate, single-bag aggregation
    (and the multibag extenstions need not exist).  In the latter case, the bag 
    can be a head bag that represents a data collection spread over any number 
    of multibag member bags (which can include head bags of earlier versions).  

    The update to the amendee, the amendments, is represented by a 
    single bag that will be the new head bag and zero or more additional member 
    bags; all of these can contain new or updated files.  The order in which the
    amendments are provided is significant; it sets the order in which they 
    are combined when creating an aggregated bag.

    :param amendee:   the path to a bag being added onto.  This can either 
                           be a normal (legal) bag or the latest head bag for 
                           a multibag aggregation.  The path can either point
                           the bag's root directory or to a supported serialized 
                           bag file.  This bag is not updated (and, thus, can be 
                           read-only).  
    :type amendee:    Bag instance or str path to bag
    :param str amendment_head:  the path to a bag that contains updated and/or 
                           new additional files to be added to the collection 
                           described by the amendee.  
    :param str version:    the version to give to the new aggregation as a 
                           result of this amendment.
    :param *list amendment:  additional bags that contain data that are 
                           intended to be part of the update.  
    """
    amender = Amender(amendee, amendment_head)
    amender.init_from_amendee()
    for bag in amendment:
        amender.add_amending_bag(bag)
    amender.finalize(version)

class Amender(object):
    """
    a class that carries out the job of establishing an update to an existing
    bag or a multibag aggregation.  The result is the establishment of a new 
    head bag for the aggregation.  

    The bag aggregation to be updated (amendee) is represented either as a 
    traditional (legal) bag or as a head bag of multibag aggregation.  In the 
    former case, the bag simply represents a degenerate, single-bag aggregation
    (and the multibag extenstions need not exist).  In the latter case, the bag 
    can be a head bag that represents a data collection spread over any number 
    of multibag member bags (which can include head bags of earlier versions).  

    The update to the amendee, the amendments, is represented by a 
    single bag that will be the new head bag and zero or more additional member 
    bags; all of these can contain new or updated files.  The order in which the
    amendments are provided is significant; it sets the order in which they 
    are combined when creating an aggregated bag.
    """

    def __init__(self, amendee, amendment, pid=None, comment=None, info=[]):
        """
        Setup an amendment to an existing bag
        :param amendee:   the old head bag of an existing aggregation (or 
                          a single traditional bag)
        :type amendee:    Bag instance or str path to the bag
        :param str amendment:  a bag that is to become the new head of the amended
                          aggregation.  This must be given as the str path 
                          to the bag's root directory.
        """
        if not isinstance(amendee, Bag):
            amendee = open_bag(amendee)
        self._oldhead = as_extended(amendee)
        if self._oldhead.is_head_multibag():
            as_headbag(self._oldhead)

        self._newheaddir = amendment
        self._newhead = HeadBag(self._newheaddir)

        self._pid = pid
        self._comm = comment
        self._info = list(info)

    def init_from_amendee(self, keepprofver=False):
        """
        Pull information from the amendee bag to initialize the multibag
        information in the new target head bag.

        :param bool keepprofver:  if True, the profile version for the new 
                                  target head bag will be set to that of the 
                                  amendee bag; otherwise (default), it will 
                                  set to that of the latest version supported 
                                  by this module.
        """

        self._init_multibag_info(keepprofver)
        self._init_member_bags()
        self._init_file_lookup()

    def _init_member_bags(self):
        target = os.path.join(self._newheaddir, self._newhead.multibag_tag_dir,
                              'member-bags.tsv')
        if os.path.isfile(target):
            # clear out any existing version of the file
            os.remove(target)
            self._newhead.member_bags(True)
        else:
            self._newhead.ensure_tagdir()

        if self._oldhead.is_head_multibag():
            # copy over the old member-bags.tsv line by line (in case they are
            # actually coming from a group-members.txt file)
            for mi in self._oldhead.iter_member_bags():
                self._newhead.add_member_bag(mi.name, mi.uri, mi.comment,
                                             mi.info)
        else:
            self._newhead.add_member_bag(self._oldhead.name)

    def _init_file_lookup(self):
        # clear out any previous existing copy of the file
        self._newhead.clear_file_lookup()

        if self._oldhead.is_head_multibag():
            # copy over the old file-lookup.tsv line by line (in case they are
            # actually coming from a group-directory.txt file)
            for item in self._oldhead.iter_file_lookup():
                self._newhead.add_file_lookup(item[0], item[1])

        else:
            # just add the files under 'data'
            self._newhead.update_for_member(self._oldhead, make_member=False)

    def _init_multibag_info(self, keepprofver=False):
        profver = CURRENT_VERSION
        if keepprofver and 'Multibag-Version' in self._oldhead.info:
            profver = self._oldhead.info['Multibag-Version']
        self._newhead.info['Multibag-Version'] = profver
            
        if 'Multibag-Head-Deprecates' in self._oldhead.info:
            if isinstance(self._oldhead.info['Multibag-Head-Deprecates'], list):
                self._newhead.info['Multibag-Head-Deprecates'] = \
                               self._oldhead.info['Multibag-Head-Deprecates']
            else:
                self._newhead.info['Multibag-Head-Deprecates'] = \
                              [ self._oldhead.info['Multibag-Head-Deprecates'] ]
        else:
            self._newhead.info['Multibag-Head-Deprecates'] = []

        self._newhead.info['Multibag-Head-Deprecates'].append(
                self._oldhead.info.get('Multibag-Head-Version', '1'))

    def add_amending_bag(self, membag, include_lu=None, exclude_lu=None,
                         pid=None, comment=None, info=[]):
        """
        add a non-head bag that is to be part of the amendment.  The order 
        that bags are added via this method is the order that they should be
        combined to recreate the original aggregated bag.  

        :param str membag:    the path to the bag's root directory or to a 
                              file representing its serialized form.  This bag
                              is examine in read-only mode.
        :param include_lu:    a list of root-relative paths to files or 
                              directories that should be included in the 
                              lookup file.  If the path is a directory, all of 
                              the files found below it will be included in the 
                              lookup.  
        :type include_lu:  list of str
        :param exclude_lu:    a list of files or directories to exclude from 
                              the lookup file.  The include list will over-ride 
                              those listed in this list, unless the 
                              corresponding include entry is a directory.
        :type exclude_lu:  list of str
        :param str pid:       a URI representing a resolvable PID for bag.
        :param str comment:   a comment about this bag to include in the 
                              member-bags.tsv file.  
        :param info:          additional string information to include with 
                              the bag's entry in the member-bags.tsv file.
        :type  info:       list of str
        """
        if not isinstance(membag, Bag):
            membag = open_bag(membag)
        as_extended(membag)

        self._newhead.add_member_bag(membag.name, pid, comment, info)
        self._newhead.update_for_member(membag, include_lu, exclude_lu,
                                        make_member=False)

    def finalize(self, version, include_lu=None, exclude_lu=None):
        """
        complete the update to the new head bag, including updating the 
        multibag tag files to include the new head bag itself.  This saves 
        all updates thus far to the new head bag.   

        :param str version:   the version string to assign to the updated 
                              aggregation
        :param include_lu:    a list of root-relative paths to files or 
                              directories within the new head bag that should 
                              be included in the lookup file.  If the path is 
                              a directory, all of the files found below it 
                              will be included in the lookup.  If not provided, 
                              ['data'] will be assumed.
        :type include_lu:  list of str
        :param exclude_lu:    a list of files or directories within the new 
                              head bag to exclude from the lookup file.  The
                              include list will over-ride those listed in this
                              list, unless the corresponding include entry is a
                              directory.
        :type exclude_lu:  list of str
        """
        self._newhead.add_member_bag(self._newhead.name, self._pid, self._comm,
                                     self._info)
        self._newhead.update_for_member(self._newhead, include_lu, exclude_lu,
                                        make_member=False)

        self._newhead.save_member_bags()
        self._newhead.save_file_lookup()
        self._newhead.update_info(version, self._newhead.profile_version)

        
    

