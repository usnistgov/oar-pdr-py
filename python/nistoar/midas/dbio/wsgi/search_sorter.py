"""
a module for sorting dbio search results.  The central abstract class from this module is RecordSorter.  
Different implementations can sort based on different criteria.
"""
from abc import ABC, abstractmethod
from typing import Iterator, List

from ..base import ACLs, ProtectedRecord

class RecordSorter(ABC):
    """
    A base class for sorter implementations.  A RecordSorter is a container for the records to be sorted.
    Records are fed into the container via the :py:meth:`add_record` method; the order that the records are 
    added can play into the final sorted order.  After all records have been added, the :py:meth:`sorted` 
    method will generate (i.e. as an iterator) the sorted results, spitting out the records in the configured 
    order.
    """

    @abstractmethod
    def add_record(self, rec: ProtectedRecord) -> ProtectedRecord:
        """
        add the given record to this container.  The same record is returned so that it can be used
        as part of a pipeline.  
        """
        raise NotImplemented()

    @abstractmethod
    def __len__(self):
        raise NotImplemented()

    @abstractmethod
    def sorted(self, reverse: bool = False, pop: bool = True) -> Iterator[ProtectedRecord]:
        """
        iteratate out the contents of this container in sorted order.
        :param bool reverse:  if True, return the records in an order reverse to the sorter's natural 
                              order.
        :param bool pop:      if True (default), each returned record will be simultaneously removed 
                              from this container.  If False, it will be kept in the container so that 
                              it can be included in the sorted output in a future call to this method.
        """
        raise NotImplemented()


class OriginalOrder(RecordSorter):
    """
    This is a null sorter that simply returns records in the order that they were added, unless reverse is 
    specified.
    """
    def __init__(self):
        self._recs = []

    def add_record(self, rec: ProtectedRecord) -> ProtectedRecord:
        self._recs.append(rec)

    def __len__(self):
        return len(self._recs)

    def sorted(self, reverse: bool = False, pop: bool = True) -> Iterator[ProtectedRecord]:
        if pop:
            return self._sorted_pop(reverse)
        else:
            return self._sorted_nopop(reverse)

    def _sorted_pop(self, reverse: bool = False) -> Iterator[ProtectedRecord]:
        popat = -1 if reverse else 0
        while self._recs:
            yield self._recs.pop(popat)

    def _sorted_nopop(self, reverse: bool = False) -> Iterator[ProtectedRecord]:
        if reverse:
            return reversed(self._recs)
        else:
            return iter(self._recs)


class SortByPerm(RecordSorter):
    """
    Sort records by the access permission afforded to the current or specified user.  By default, the 
    records are ordered by most permissive (i.e. owner) to least permissive (i.e. read-only); the reverse 
    can be chosen instead.  Within a permission category, the order of the records is in the order that 
    the records were added.  (In practice, this will be in the order that the records are returned from 
    the source database, which may have applied its own sorting.)
    """

    def __init__(self, user: str=None):
        self._user = user
        self._selected = {}

    def add_record(self, rec: ProtectedRecord) -> ProtectedRecord:
        maxperm = ''
        if rec.owner == rec._cli.user_id:
            maxperm = "owner"
        elif rec.authorized(ACLs.ADMIN):
            maxperm = ACLs.ADMIN
        elif rec.authorized(ACLs.WRITE):
            maxperm = ACLs.WRITE
        elif rec.authorized(ACLs.READ):
            maxperm = ACLs.READ
        else:
            maxperm = ""

        if maxperm not in self._selected:
            self._selected[maxperm] = []
        self._selected[maxperm].append(rec)

    def __len__(self):
        out = 0
        for perm in self._selected:
            out += len(self._selected[perm])
        return out

    def sorted(self, reverse: bool = False, pop: bool = True) -> Iterator[ProtectedRecord]:
        perms = ["owner", ACLs.ADMIN, ACLs.WRITE, ACLs.READ, ""]
        if reverse:
            perms.reverse()
        if pop:
            return self._sorted_pop(perms, reverse)
        else:
            return self._sorted_nopop(perms, reverse)

    def _sorted_pop(self, perms: List[str], reverse: bool = False):
        popat = -1 if reverse else 0
        for perm in perms:
            recs = self._selected.get(perm, [])
            while recs:
                yield recs.pop(popat)

    def _sorted_nopop(self, perms: List[str], reverse: bool = False):
        iterator = reversed if reverse else iter
        for perm in perms:
            recs = self._selected.get(perm, [])
            for rec in iterator(recs):
                yield rec
