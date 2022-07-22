"""
an implementation of the NERDResource storage interface that stores the data in memory (using OrderedDicts).

This is provide for purposes of testing the interface.
"""

# See .base.py for function documentation

import copy, re, functools
from collections import OrderedDict
from collections.abc import Mapping
from logging import Logger
from typing import Iterable, Iterator

from .base import *
from .base import _NERDOrderedObjectList

import nistoar.nerdm.utils as nerdmutils

_idre = re.compile(r"\w_(\d+)$")

class InMemoryObjectList(_NERDOrderedObjectList):
    """
    An in-memory implementation of the _NERDOrderedObjectList interface where the list is kept in memory
    """
    _pfx = "obj"

    def __init__(self, resource: NERDResource, data=[]):
        super(InMemoryObjectList, self).__init__(resource)
        self._data = {}
        self._order = []
        self._ididx = 0
        if data:
            self._load_data(data)

    def empty(self):
        if self._res.deleted:
            raise RecordDeleted(self._res.id, "empty")
        self._order = []
        self._data = {}

    def _load_data(self, items):
        order = []
        n = 0
        for itm in items:
            if itm.get('@id'):
                self._data[cmp.get('@id')] = copy.deepcopy(itm)
                m = _idre.find(itm['@id'])
                if m:
                    # the id was set by a previous call to this class's minter
                    # extract the number to ensure future ids are unique
                    n = int(m.group(1))
                    if n >= self._ididx:
                        self._ididx = n + 1
            order.append(itm)

        for itm in order:
            if not itm.get('@id'):
                itm = copy.deepcopy(itm)
                itm['@id'] = self._get_default_id_for(itm)
                self._data[itm['@id']] = itm
            self._order.append(itm['@id'])

    def _new_id(self):
        out = "%s_%d" % (self._pfx, self._ididx)
        self._ididx += 1
        return out

    @property
    def ids(self) -> [str]:
        return list(self._order)

    @property
    def count(self) -> int:
        return len(self._order)

    def _get_item_by_id(self, id: str):
        return self._data[id]

    def _get_item_by_pos(self, pos: int):
        return self._data[self._order[pos]]

    def set_order(self, ids: Iterable[str]):
        neworder = []
        for id in ids:
            if id not in neworder:
                neworder.append(id)
        for id in self._order:
            if id not in neworder:
                neworder.append(id)
        self._order = neworder

    def _set_item(self, id: str, md: Mapping, pos: int=None):
        if abs(pos) > self.count:
            raise IndexError("NERDm List index out of range: "+str(pos))
        md = OrderedDict(md)
        md['@id'] = id
        
        if pos is not None:
            neworder = list(self._order)
            try:
                oldpos = neworder.index(id)
            except ValueError as ex:
                pass
            else:
                if pos > 0 and oldpos < pos:
                    pos -= 1
                elif pos == -1 * len(neworder):
                    pos = 0
                neworder.remove(id)
            neworder.insert(pos, id)
        elif id not in neworder:
            neworder.append(id)
                    
        self._data[id] = md
        self._order = neworder

    def _remove_item(self, id: str):
        del self._data[id]
        try:
            self._order.remove(id)
        except ValueError:
            pass

class InMemoryAuthorList(InMemoryObjectList, NERDAuthorList):
    """
    an in-memory implementation of the NERDAuthorList interface
    """
    _pfx = "auth"
    def __init__(self, resource: NERDResource, auths=[]):
        InMemoryObjectList.__init__(self, resource, auths)

class InMemoryRefList(InMemoryObjectList, NERDRefList):
    """
    an in-memory implementation of the NERDRefList interface
    """
    _pfx = "ref"
    def __init__(self, resource: NERDResource, refs=[]):
        InMemoryObjectList.__init__(self, resource, refs)

class InMemoryNonFileComps(InMemoryObjectList, NERDNonFileComps):
    """
    an in-memory implementation of the NERDNonFileComps interface
    """
    _pfx = "cmp"
    def __init__(self, resource: NERDResource, comps=[]):
        InMemoryObjectList.__init__(self, resource, comps)

    def _load_data(self, comps):
        order = []
        n = 0
        for cmp in comps:
            if 'filepath' not in cmp:
                if cmp.get('@id'):
                    self._data[cmp.get('@id')] = copy.deepcopy(cmp)
                    m = _idre.find(cmd['@id'])
                    if m:
                        # the id was set by a previous call to this class's minter
                        # extract the number to ensure future ids are unique
                        n = int(m.group(1))
                        if n >= self._ididx:
                            self._ididx = n + 1
                order.append(cmp)

        for cmp in order:
            if not cmp.get('@id'):
                cmp = copy.deepcopy(cmp)
                cmp['@id'] = self._get_default_id_for(cmp)
                self._data[cmp['@id']] = cmp
            self._order.append(cmp['@id'])
            

class InMemoryFileComps(NERDFileComps):
    """
    an in-memory implementation of the NERDAuthorList interface
    """
    def __init__(self, resource: NERDResource, comps=[], iscollf=None):
        super(InMemoryFileComps, self).__init__(resource, iscollf)
        self._files = {}
        self._children = OrderedDict()
        self._ididx = 0

        if comps:
            self._load_from(comps)

    def empty(self):
        if self._res.deleted:
            raise RecordDeleted(self._res.id, "empty")
        self._children.clear()
        self._files.clear()

    def _load_from(self, cmps: [Mapping]):

        # Once through to load all files by their ID
        for cmp in cmps:
            if cmp.get('filepath'):
                if cmp.get('@id'):
                    m = _idre.find(cmd['@id'])
                    if m:
                        # the id was set by a previous call to this class's minter
                        # extract the number to ensure future ids are unique
                        n = int(m.group(1))
                        if n > self.ididx:
                            self.ididx = n + 1
                    # store a copy of the file component
                    self._files[cmp['@id']] = copy.deepcopy(cmp)

        # Go through again to (1) assign ids to file components that are missing one,
        # and (2) create a map from parent subcollections to their children
        children = {'': []}
        subcolls = []
        for cmp in cmps:
            if cmp.get('filepath'):
                if not cmp.get('@id'):
                    # assign an ID to file component missing one
                    cmp = copy.deepcopy(cmp)
                    cmp['@id'] = self._get_default_id_for(cmp)
                    self._files[cmp['@id']] = cmp

                # build parent-children map
                if '/' in cmp['filepath']:
                    parent = os.path.dirname(cmp['filepath'])
                    if parent not in children:
                        children[parent] = []
                    children[parent].append( (os.path.basename(cmp['filepath']), cmp['@id']) )
                else:
                    children[''].append(cmp['filepath'])

                # remember subcollections
                if self.is_collection(cmp):
                    subcolls.append(cmp)

        # Go through a last time to set the subcollection content info into each subcollection component
        for cmp in subcolls:
            if cmp.get('filepath') in children:
                if '_children' not in cmp:
                    cmp['_children'] = OrderedDict()

                # base subcollection contents first on 'has_member' list as this captures order info
                if cmp.get('has_member'):
                    if isinstance(cmd.get('has_member',[]), str):
                        cmp['has_member'] = [cmp['has_member']]
                    for child in cmp['has_member']:
                        if child.get('@id') in self._files and child.get('name'):
                            cmp['_children'][child['name']] = child.get('@id')

                # capture any that got missed by 'has_member'
                for child in children[cmp['filepath']]:
                    if child[0] not in cmp['_children']:
                        cmp['_children'][child[0]] = child[1]
                            

    def get_file_by_id(self, id: str) -> Mapping:
        return self._export_file(self._get_file_by_id(id))

    def _get_file_by_id(self, id: str) -> Mapping:
        try:
            return self._files[id]
        except KeyError:
            raise ObjectNotFound(id)

    def get_file_by_path(self, path: str) -> Mapping:
        return self._export_file(self._get_file_by_path(path))

    def _export_file(self, fmd):
        out = OrderedDict([m for m in fmd.items() if not m[0].startswith("_")])
        if self.is_collection(out):
            out['has_member'] = [OrderedDict([('@id', m[1], 'name', m[0])]) for m in fmd.get("_children",[])]
        return out

    def _get_file_by_path(self, path: str) -> Mapping:
        return self._get_file_by_relpath(self._children, path.split('/'), path)

    def _get_file_by_relpath(self, children: Mapping, steps: [str], origpath):
        top = steps.pop(0)
        if top not in children:
            raise ObjectNotFound(origpath)
        child = self._get_file_by_id(self._children[top])
        if not steps:
            return child
        
        if not self.is_collection(child):
            raise ObjectNotFound(origpath)
        return self._get_file_by_relpath(self, child.get('_children',{}), steps)

    @property
    def ids(self):
        return [f.get('@id', '') for id in self.iter_files()]

    def iter_files(self):
        return self._FileIterator(self)

    @property
    def count(self) -> int:
        return len(self._files)

    def data(self) -> [Mapping]:
        return [f for f in self.iter_files()]

    class _FileIterator:
        def __init__(self, fstore, children=None):
            self._fs = fstore
            if children is not None:
                children = list(fstore._children.values())
            self.descendents = children
        def __iter__(self):
            return self
        def __next__(self):
            while self.descendents:
                desc = self._fs.get_file_by_id(self.descendents.pop(0))
                if desc.get('_children'):
                    self.descendents.append(desc.get('_children', []))
                yield desc
            
    def get_ids_in_subcoll(self, collpath: str) -> [str]:
        children = self._children
        try:
            coll = self._get_file_by_path(collpath)
        except ObjectNotFound:
            return []
        else:
            children = coll.get('_children', [])
        return list(children.values())

    def get_subcoll_members(self, collpath: str) -> Iterator[Mapping]:
        for id in self.get_ids_in_subcoll(collpath):
            yield self.get_file_by_id(id)

    def _get_default_id_for(self, md):
        pfx = "file"
        if self.is_collection(md):
            pfx = "coll"
        out = "%s_%d" % (pfx, self._ididx)
        self._ididx += 1
        return out

    def set_order_in_subcoll(self, collpath: str, ids: Iterable[str]) -> Iterable[str]:
        children = self._children
        if collpath:
            coll = self._get_file_by_path(collpath)
            if not self.is_collection(coll):
                raise ObjectNotFound(collpath, message=collpath+": not a subcollection component")
            if '_children' not in coll:
                coll['_children'] = OrderedDict
            children = coll['_children']

        # create an inverted child map
        byid = dict( [(itm[1], itm[0]) for itm in children.items()] )

        # reorder the original map
        children.clear()
        missing = []
        for id in ids:
            if id in byid:
                children[byid[id]] = id
            else:
                missing.append(id)
        for id in missing:
            children[byid[id]] = id

    def set_file_at(md, filepath: str=None, id=None):
        # first, make sure we have both an id and a filepath for the input metadata
        oldfile = self.get(id) if id else None
        if not id:
            id = md.get('@id')

        if not filepath:
            filepath = md.get('filepath')
        if not filepath and oldfile:
            filepath = oldfile.get('filepath')
        if not filepath:
            raise ValueError("set_file_at(): filepath be provided directly")
        if not id:
            if not oldfile:
                try:
                    oldfile = self.get_file_by_path(filepath)
                except ObjectNotFound:
                    pass
            if oldfile:
                id = oldfile.get('@id')
        if not id:
            id = self._get_default_id_for(md)

        # is the file getting moved?
        if oldfile and filepath != oldfile.get('filepath'):
            # unregister it from its old parent
            name = os.path.basename(oldfile.get('filepath'))
            children = self._children
            if '/' in oldfile.get('filepath'):
                try:
                    parent = self.get_file_by_path(os.path.dirname(oldfile['filepath']))
                except ObjectNotFound:
                    pass
                else:
                    children = parent.get('_children')
            if children is not None:
                try:
                    del children[name]
                except KeyError:
                    pass
        
        name = os.path.basename(filepath)
        children = self._children
        if '/' in filepath:
            parentpath = os.path.dirname(filepath)
            coll = self.get_file_by_filepath(parentpath)  # may raise ObjectNotFound
            if not self.is_collection(coll):
                raise ObjectNotFound(parentpath, message=parentpath+": not a subcollection")
            children = coll.setdefault('_children', OrderedDict())

        md = copy.deepcopy(md)
        md['@id'] = id
        md['filepath'] = filepath
        self._files[id] = md
        if children.get(name) and children.get(name) != id:
            try:
                del self._files[children[name]]
            except KeyError:
                pass
        children[name] = id

        return id

class InMemoryResource(NERDResource):
    """
    an in-memory implementation of the NERDResource interface
    """
    _subprops = "authors references components @id".split()

    def __init__(self, id: str, rec: Mapping={}, parentlog: Logger=None):
        super(InMemoryResource, self).__init__(id, parentlog)
        self._data = OrderedDict()
        self._auths = None
        self._refs  = None
        self._files = None
        self._nonfiles = None

        for itm in rec.items():
            if itm[0] not in self._subprops:
                self._data[itm[0]] = copy.deepcopy(itm[1])
            elif isinstance(itm[1], (list, tuple)):
                if itm[0] == "authors":
                    self._auths = InMemoryAuthorList(self, itm[1])
                elif itm[0] == "references":
                    self._refs = InMemoryRefList(self, itm[1])
                elif itm[0] == "components":
                    self._nonfiles = InMemoryNonFileComps(self, itm[1])
                    self._files = InMemoryFileComps(self, itm[1])

        if self._auths is None:
            self._auths = InMemoryAuthorList(self)
        if self._refs is None:
            self._refs = InMemoryRefList(self)
        if self._nonfiles is None:
            self._nonfiles = InMemoryNonFileComps(self)
        if self._files is None:
            self._files = InMemoryFileComps(self)

    @property
    def authors(self):
        return self._auths
    @property
    def references(self):
        return self._refs
    @property
    def nonfiles(self):
        return self._nonfiles
    @property
    def files(self):
        return self._files

    def replace_res_data(self, md):
        if self.deleted:
            raise RecordDeleted(self.id, "replace")
        for itm in rec:
            if itm[0] not in self._subprops:
                self._data[itm[0]] = copy.deepcopy(itm[1])

    def deleted(self):
        return self._data is None

    def delete(self):
        if not self.deleted:
            self._data = None

    def res_data(self):
        out = copy.deepcopy(self._data)
        out['@id'] = self.id
        return out
        
    def data(self, inclfiles=True) -> Mapping:
        out = self.res_data()
        if self._auths.count > 0:
            out['authors'] = self._auths.data()
        if self._refs.count > 0:
            out['references'] = self._auths.data()
        if self._nonfiles.count > 0 or self._files.count > 0:
            out['components'] = []
            if self._nonfiles.count > 0:
                out.extend(self._nonfiles.data())
            if self._files.count > 0:
                out.extend(self._files.data())
        return out
        
class InMemoryResourceStorage(NERDResourceStorage):
    """
    a factory for opening records stored in memory
    """

    def __init__(self, newidprefix: str="nrd", existing: [Mapping]=[], logger: Logger=None):
        """
        initialize a factory with some existing in-memory NERDm records
        :param str  newidprefix:  a prefix to use when minting new identifiers
        :param Mapping existing:  a list of NERDm records that should be made available via 
                                  :py:method:`open`.
        """
        self.log = logger
        self._pfx = ""
        self._recs = {}
        self._ididx = 0
        if existing is None:
            existing = []
        for rec in existing:
            self.load_from(rec)

    def load_from(self, rec: Mapping, id: str=None):
        """
        load a NERDm record into this storage.  If the record exist
        :param Mapping rec:  the NERDm Resource record to load, given as a JSON-ready dictionary
        :param str id:       the ID to assign to the record; if not given, the value of the record's
                             `@id` property will be used or one will be created for it.
        """
        if not id:
            id = rec.get('@id')
            m = _idre.find(id)
            if m:
                n = int(m.group(1))
                if n >= self._ididx:
                    self._ididx = n + 1
        if not id:
            id = self._new_id()
        if id in self._res:
            self._recs[id].replace_all_data(rec)
        else:
            self._recs[id] = InMemoryResource(id, rec, self.log)

    def _new_id(self):
        out = "%s_%d" % (self._pfx, self._ididx)
        self._ididx += 1
        return out

    def exists(self, id: str) -> bool:
        return id in self._recs

    def delete(self, id: str) -> bool:
        if id in self._recs:
            self._recs[id].delete()
            del self._recs[id]
            return True
        return False
    
    def open(self, id: str=None) -> NERDResource:
        if not id:
            id = self._new_id()
        if id not in self._recs:
            self._recs[id] = InMemoryResource(id, None, self.log)
        return self._recs[id]
