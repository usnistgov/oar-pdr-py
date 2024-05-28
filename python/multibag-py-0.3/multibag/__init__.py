"""
a reference implementation of the Multibag BagIt Profile.

The main multibag module provides the ProgenitorBag class in which an instance
wraps around a standard bag that would be split in to Multibag-compliant 
member bags.  It also provides the Multibag class in which an instance wraps 
a Multibag member bag; it can test the bag's validity with Multibag 
specification. The HeadBag class is Multibag specialization specifically for 
head bags which can be used to reconstitute the progenitor bag.  
"""
from .split import Splitter, WellPackedSplitter, NeighborlySplitter, SplitPlan
from .access.multibag import HeadBag, as_headbag, is_headbag, open_headbag
from .access.bagit import open_bag, BagError, BagValidationError
from .validate import (MultibagValidationError, HeadBagValidator,
                       MemberBagValidator, validate_headbag, validate_memberbag)
from .amend import make_single_multibag, amend_bag_with, SingleMultibagMaker
from .restore import restore_bag


