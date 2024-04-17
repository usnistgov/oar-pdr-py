"""
a module supporting special-purpose indexes for the DBIO.
"""
import json, csv, re
from abc import ABC, abstractmethod
from collections import namedtuple
from collections.abc import Mapping
from typing import Iterator, Iterable, Callable, List, Tuple, Any, Union, NewType, TypeVar
from io import StringIO
from copy import deepcopy

import nistoar.nsd.client as nsd
from nistoar.base.config import ConfigurationException

Index = NewType("Index", object)

T = TypeVar('T')
Indexable = Union[Iterable[T], Iterator[T]]

class Index:
    """
    a lookup object for finding dictionary records containing properties with particular values.
    Equivalent in concept to a database index, this index is intended to quickly identify 
    records where a property value _starts_ with a given substring.  

    An Index is created (via an :py:class:`Indexer`) by scanning a set of dictionary records stored 
    elsewhere, and contains targets, keys, and labels.  In particular, the Index contains a unique 
    list _target_ values matching the values of a property (or properties) appearing in all of the 
    scanned records.  Each target maps to a unique set of _keys_--that is, identifiers that can be 
    used to quickly retrieve a record from its external collection.  Accompanying each key is a 
    _label_ that can represent the record in a display.  

    It's worth noting that it's expected that an index that is part of a full-blown database will 
    be more performant at selecting records than this index.  The motivation for this class is to 
    provide an index that can be delivered to remote clients through a web service.  With this in 
    mind, the two most important functions of this class are :py:meth:`select_startswith` and 
    :py:meth:`export_as_json`:  the first creates a new index that contains only those entries 
    where the target value starts with a particular substring, and the second provides a format 
    for the index that can be delivered to a web client.  The client can then do its own fast 
    lookups, and use the keys to retrieve specific records of interest based on the target value. 
    """
    def __init__(self, caseins=True):
        """
        initialize the index.  
        :param bool caseins:  if True (default), treat target values as case-insensitive
        """
        self._data = {}
        self._mkt = str
        if caseins:
            self._mkt = lambda s: s.lower()

    def register(self, target: str, key, dispstr: str=None):
        """
        add a key into this index
        :param str  target:  the value of the target property being indexed
        :param         key:  a unique identifier for the record that contains the target property
        :param str dispstr:  the string to save as the displayable representation of the record.
                             If not provided, the target value will be used.
        """
        if dispstr is None:
            dispstr = target
        target = self._mkt(target)
        if target not in self._data:
            self._data[target] = {}
        self._data[target][key] = dispstr

    def select_startswith(self, substr: str) -> Index:
        """
        return an Index that contains only those entries where the target starts with the 
        given string.
        """
        out = Index(self._mkt is not str)
        substr = self._mkt(substr)
        out._data = dict((p[0], deepcopy(p[1])) for p in self._data.items() if p[0].startswith(substr))
        return out

    def key_labels_for(self, target) -> Mapping:
        """
        return a map of keys to display labels for a given target value
        """
        return dict(self._data.get(self._mkt(target), {}))

    def iter_key_labels(self) -> Iterator[Tuple]:
        """
        return a unique List of key-label pairs from all entries in this Index.  This 
        would most usefully called on the output of :py:meth:`select_startswith`.  The 
        keys in the list are guaranteed to be unique, but the labels are not.
        """
        out = {}
        for map in self._data.values():
            for key, label in map.items():
                out[key] = label
        return out.items()

    def __ior__(self, other: Index):
        self.update(other)
        return self

    def update(self, other: Index) -> Index:
        """
        merge the contents of another index into this one (possibly overwriting display strings)
        :returns: self (this Index instance)
                  :rtype: Index
        """
        for t in other._data:
            t = self._mkt(t)
            if t not in self._data:
                self._data[t] = {}
            self._data[t].update(other._data[t])
        return self

    def __or__(self, other: Index):
        out = self.clone()
        out.update(other)
        return out

    def clone(self) -> Index:
        """
        create a deep copy of this index
        """
        out = Index(self._mkt is not str)
        out._data = dict((p[0], deepcopy(p[1])) for p in self._data.items())
        return out

    def export_as_json(self, pretty=False) -> str:
        """
        export this index into JSON
        """
        if pretty:
            return json.dumps(self._data, indent=2)
        else:
            return json.dumps(self._data)

    def export_as_csv(self, keydelim=':') -> str:
        out = StringIO(newline='')
        wrtr = csv.writer(out, csv.unix_dialect, quoting=csv.QUOTE_MINIMAL)
        for target in self._data:
            wrtr.writerow([target]+[keydelim.join(str(i) for i in p)
                           for p in self._data[target].items()])
        return out.getvalue()


class Indexer(ABC):
    """
    a class that creates a string index on a set of records.  
    """

    @abstractmethod
    def make_index(self, data: Indexable[Mapping], caseins: bool=True) -> Index:
        """
        iterate through the given stream of objects and create an index
        :param Iterator[Mapping]|Iterable[Mapping] data:   the data to be index.  This should 
             be either an array of or an iterator of dictionary objects.  
        """
        raise NotImplemented()

class IndexerOnProperty(Indexer):
    """
    create an Indexer based on the unique values in a string-valued property within 
    each item being indexed.
    """

    def __init__(self, targetprop: str, keyprop, disp_props: Iterator[str]=[],
                 disp_format: Union[str, Callable[[Any], str], None]=None):
        """
        initialize the indexer 
        :param str targetprop:  the string-valued property from the records being index that is 
                                the target of the index.  That is, the index will provide pointers
                                to records that match values of this property.
        :param str    keyprop:  the property in the records being indexed that represents a record's 
                                unique identifier.
        :param List[str] disp_props:  a list of property values that should be used to create a 
                                display string for a matching record.  If None or empty, the 
                                target property will be used.
        :param str|func disp_format:  a string or function to be used to convert the values of the 
                                properties given by ``disp_props`` into a display string.  If the 
                                value is a string, it will be taken as a format string to be 
                                processed by ``str.format()``.  Otherwise, it should be a function
                                that can be called with at least as many positional arguments as the 
                                length of ``disp_props``.  
        """
        self.keyp = keyprop
        self.tp = targetprop

        if not disp_props:
            disp_props = [self.tp]
        elif isinstance(disp_props, str):
            disp_props = [disp_props]
        self.dispp = list(disp_props)

        if disp_format is None:
            disp_format = self._def_disp_fmt
        elif isinstance(disp_format, str):
            disp_format = disp_format.format
        try:
            disp_format(*self.dispp)
        except TypeError as ex:
            raise ValueError("IndexerOnProperty(): disp_format is not a function or format string")
        except IndexError as ex:
            raise ValueError("IndexerOnProperty(): disp_props does not enough values for disp_format")
        except Exception as ex:
            raise ValueError("IndexerOnProperty(): disp_props is not compatible with disp_format")
        self._fmt = disp_format

    def _def_disp_fmt(self, *args):
        return ", ".join(args)

    def make_index(self, data: Indexable[Mapping], caseins: bool=True) -> Index:
        """
        iterate through the given stream of objects and create an index
        :param Iterator[Mapping]|Iterable[Mapping] data:   the data to be index.  This should 
             be either an array of or an iterator of dictionary objects.  
        :param bool caseins:  if True (default), treat target values as case-insensitive
        """
        out = Index(caseins)

        for item in data:
            if isinstance(item.get(self.tp), str):
                out.register(item[self.tp], item[self.keyp], self.format_key(item))

        return out

    def format_key(self, item):
        """
        create a display string to represent this record from the record itself.  This implementation
        will create a string based the parameters provided at construction time.  Subclasses may 
        override this method to provide more complex implementations.
        """
        vals = [str(item[p]) for p in self.dispp]
        return self._fmt(*vals)

class DAPAuthorIndexer(Indexer):
    """
    an indexer of DAP records based on the associated authors.  By default, the index produced look
    ups against by either the first name or last name.  
    """
    _orcid_key_re = re.compile(r'^[^/]+/authors#orcid:')
    
    def __init__(self, authprops=None):
        """
        initialize the indexer.  By default, the index will be based on both the first and last name
        of the author; however, this can be overridden via ``authoprops``
        :param str|list(str) authprops:  the string-valued author properties that should be indexed,
                                         given either as a string (for a single property) or a list 
                                         strings. If not provided, the ``["familyName", "givenName"]``
                                         will be assumed.  
        """
        if not authprops:
            authprops = ["familyName", "givenName"]
        elif isiinstance(authprops, str):
            authprops = [authprops]
        self._targets = list(authprops)

    def make_index(self, data: Indexable[Mapping], caseins: bool=True) -> Index:
        out = Index(caseins)

        for item in data:
            # item is a DBIO record
            if not item.get('data',{}).get("authors"):
                continue

            for auth in item['data']['authors']:
                # figure out something to use as an id
                if auth.get('orcid'):
                    id = f"{item['id']}/authors#orcid:{auth.get('orcid')}"
                elif not item.get('id'):
                    continue
                elif auth.get('@id'):
                    id = f"{item['id']}/authors#{auth.get('@id')}"
                elif auth.get('familyName'):
                    id = f"{item['id']}/authors#familyName:{auth['familyName']}"
                else:
                    continue

                fn = auth.get('fn')
                if not fn:
                    fn = f"{auth.get('givenName')} {auth.get('familyName')}"

                # look up on both names
                for prop in self._targets:
                    if auth.get(prop):
                        if (prop == "familyName" or prop == "givenName") and \
                           len(auth[prop].rstrip('.')) < 2:
                            continue
                        out.register(auth[prop], id, fn)

        return out
    
class NSDPeopleResponseIndexer(Indexer):
    """
    an Indexer implementation that operates on a response to a person query to an NSD query 
    via the :py:class:`~nistoar.nsd.client.NSDClient`.  
    """
    _dispfmt = "{0}, {1}"

    def __init__(self, targets: List[str]=None):
        if not targets:
            targets = "lastName firstName".split()
        self.delegates = []
        for target in targets:
            self.delegates.append(IndexerOnProperty(target, "peopleID", ["lastName", "firstName"],
                                                    self._dispfmt))

    def make_index(self, data: Indexable[Mapping], caseins: bool=True) -> Index:
        out = Index()
        for delg in self.delegates:
            out |= delg.make_index(data, caseins)
        return out

class NSDPeopleIndexClient:
    """
    a class that can create an index based on a query to an NSD service.  It wraps around an 
    :py:class:`~nistoar.nsd.client.NSDClient` instance which it uses to submit a query to 
    People search endpoint and returns an Index of the results.
    """

    def __init__(self, client: nsd.NSDClient, indexprops=None, indexer: Indexer=None, 
                 enforce_start=False):
        """
        create the index-making client.  
        :param NSDClient client:   the NSDClient instance to use to send queries
        :param [str] indexprops:   a list of the people properties to index.  These 
                                   properties will be used provide constraints on queries
                                   to the NSD service.  If not provided, these default to 
                                   ``["lastName", "firstName"]``.  
        :param Indexer indexer:    the Indexer instance to use to create indicies.  This 
                                   Indexer must be configured to operate on responses 
                                   from an :py:class:`~nistoar.nsd.client.NSDClient`.
                                   If not provided, a default instance will be created 
                                   based on the ``indexprops`` values.  
        :param bool enforce_start: if True, require for the records returned from the NSD 
                                   query that the values of the indexed properties start with 
                                   index-making prompt value (see :py:meth:`get_index_for`).  
                                   Some implementations of the NSD service will match any value 
                                   substring by default.  Setting this to True will filter out 
                                   matched records that do not start with the prompt string.  
                                   The default is False, because the index generation will 
                                   effectively do the same.  
        """
        self.cli = client
        self.muststart = enforce_start

        if not indexprops:
            indexprops = "lastName firstName".split()
        if not isinstance(indexprops, (list, tuple)) or any(not isinstance(e, str) for e in indexprops):
            raise ValueError("NSDPeopleIndexClient(): indexprops is not a list of strings: "+str(indexprops))
        self.props = indexprops

        if not indexer:
            indexer = NSDPeopleResponseIndexer(self.props)
        self.idxr = indexer

    def get_index_for(self, prompt: str) -> Index:
        """
        send a query for people whose names start with the given prompt string and return an index of
        the results.  The records that will be indexed will be those whose index properties ("lastName" 
        and "firstName", by default) start with the prompt string.
        :raises NSDServerError:  if there is a problem communicating with the NSD server.  In particular,
                                 this will usually be raised if the NSD endpoint URL is incorrect.
        :raises ConfigurationException:  if there appears to be a problem with how this class instance is 
                                 configured such as incompatible index properties or indexer.
        """
        filter = {}
        for prop in self.props:
            filter[prop] = [ prompt ]

        try:
            res = self.cli.select_people(filter=filter)
        except NSDClientError as ex:
            raise ConfigurationException("client configuration results in bad service query: "+str(ex))

        if self.muststart:
            def startswithprompt(rec):
                for p in self.props:
                    if rec[p].lower().startswith(prompt):
                        return True
                return False

            res = [r for r in res if startswithprompt(r)]

        return self.idxr.make_index(res).select_startswith(prompt)


