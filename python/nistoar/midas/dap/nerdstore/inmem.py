"""
an implementation of the NERDResource storage interface that stores the data in memory (using OrderedDicts).

This is provided for purposes of testing the interface.  See 
:py:mod:`nerdstore.base<nistoar.pdr.draft.nerdstore.base` for full interface documentation.  
"""

# See .base.py for function documentation

import os, copy, re, math
from collections import OrderedDict
from collections.abc import Mapping
from logging import Logger
from typing import Iterable, Iterator, List

from .base import *
from .base import _NERDOrderedObjectList, DATAFILE_TYPE, SUBCOLL_TYPE, DOWNLOADABLEFILE_TYPE

_idre = re.compile(r"^\w+_(\d+)$")

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

    def _reserve_id(self, id):
        m = _idre.search(id)
        if m:
            # the id was set by a previous call to this class's minter
            # extract the number to ensure future ids are unique
            n = int(m.group(1))
            if n >= self._ididx:
                self._ididx = n + 1

    @property
    def ids(self) -> [str]:
        return list(self._order)

    @property
    def count(self) -> int:
        return len(self._order)

    def _get_item_by_id(self, id: str):
        return copy.deepcopy(self._data[id])

    def _get_item_by_pos(self, pos: int):
        return copy.deepcopy(self._data[self._order[pos]])

    def set_order(self, ids: Iterable[str]):
        neworder = []
        for id in ids:
            if id not in neworder and id in self._order:
                neworder.append(id)
        for id in self._order:
            if id not in neworder:
                neworder.append(id)
        self._order = neworder

    def move(self, idorpos: str, pos: int = None, rel: int = 0) -> int:
        if pos is None:
            pos = self.count
            rel = 0
        if not isinstance(pos, int):
            raise TypeError("move(): pos is not an int")

        if isinstance(idorpos, int):
            if idorpos < -1*(len(self._order)-1) or idorpos >= len(self._order):
                raise IndexError(idorpos)
            oldpos = idorpos
        else:
            self._data[idorpos]  # raises KeyError
            try: 
                oldpos = self._order.index(idorpos)
            except ValueError:
                # shouldn't happen; self-correcting programmer error (danger!)
                self._order.append(idorpos)
                oldpos = len(self._order) - 1

        if not isinstance(rel, (int, float)):
            rel = 1 if bool(rel) else 0
        if rel != 0:
            rel = math.floor(round(math.fabs(rel)/rel))  # +1 or -1
            pos = oldpos + rel * pos
        # if pos == oldpos:
        #     return pos

        id = self._order.pop(oldpos)
        if pos > len(self._order):
            self._order.append(id)
            return len(self._order) -1

        elif pos < 0:
            pos = 0
            
        self._order.insert(pos, id)
        return pos

    def _set_item(self, id: str, md: Mapping, pos: int=None):
        if pos is not None and abs(pos) > self.count:
            raise IndexError("NERDm List index out of range: "+str(pos))
        md = OrderedDict(md)
        md['@id'] = id
        
        neworder = list(self._order)
        if pos is not None:
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
        out = self._data.pop(id)
        try:
            self._order.remove(id)
        except ValueError:
            pass
        return out

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
                    m = _idre.search(cmp['@id'])
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
    an in-memory implementation of the NERDFileComps interface
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
                    m = _idre.search(cmp['@id'])
                    if m:
                        # the id was set by a previous call to this class's minter
                        # extract the number to ensure future ids are unique
                        n = int(m.group(1))
                        if n > self._ididx:
                            self._ididx = n + 1
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
                    parent = self._dirname(cmp['filepath'])
                    if parent not in children:
                        children[parent] = []
                    children[parent].append( (self._basename(cmp['filepath']), cmp['@id']) )
                else:
                    children[''].append( (cmp['filepath'], cmp['@id']) )
                    self._children[cmp['filepath']] = cmp['@id']

                # remember subcollections
                if self.is_collection(cmp):
                    subcolls.append(cmp)

        # Go through a last time to set the subcollection content info into each subcollection component
        for cmp in subcolls:
            if cmp.get('filepath') in children:
                if '_children' not in cmp:
                    cmp['__children'] = OrderedDict()

                # base subcollection contents first on 'has_member' list as this captures order info
                if cmp.get('has_member'):
                    if isinstance(cmp.get('has_member',[]), str):
                        cmp['has_member'] = [cmp['has_member']]
                    for child in cmp['has_member']:
                        if child.get('@id') in self._files and child.get('name'):
                            cmp['__children'][child['name']] = child.get('@id')

                # capture any that got missed by 'has_member'
                for child in children[cmp['filepath']]:
                    if child[0] not in cmp['__children']:
                        cmp['__children'][child[0]] = child[1]
                            

    def get_file_by_id(self, id: str) -> Mapping:
        return self._export_file(self._get_file_by_id(id))

    def _get_file_by_id(self, id: str) -> Mapping:
        try:
            return self._files[id]
        except KeyError:
            raise ObjectNotFound(id)

    def get_file_by_path(self, path: str) -> Mapping:
        if not path:
            raise ValueError("get_file_path(): No path specified")
        return self._export_file(self._get_file_by_path(path))

    def _export_file(self, fmd):
        out = OrderedDict([copy.deepcopy(m) for m in fmd.items() if not m[0].startswith("__")])
        if self.is_collection(out):
            out['has_member'] = [OrderedDict([('@id', m[1]), ('name', m[0])])
                                 for m in fmd.get("__children",{}).items()]
        return out

    def _get_file_by_path(self, path: str) -> Mapping:
        return self._get_file_by_relpath(self._children, path.split('/'), path)

    def _get_file_by_relpath(self, children: Mapping, steps: [str], origpath):
        top = steps.pop(0)
        if top not in children:
            raise ObjectNotFound(origpath)
        child = self._get_file_by_id(children[top])  # may raise ObjectNotFound
        if not steps:
            return child
        
        if not self.is_collection(child):
            raise ObjectNotFound(origpath)
        return self._get_file_by_relpath(child.get('__children',{}), steps, origpath)

    @property
    def ids(self):
        return [f.get('@id', '') for f in self.iter_files()]

    def iter_files(self):
        return iter(self._FileIterator(self))

    @property
    def count(self) -> int:
        return len(self._files)

    def get_data(self) -> [Mapping]:
        return [f for f in self.iter_files()]

    class _FileIterator:
        def __init__(self, fstore, children=None):
            self._fs = fstore
            if children is None:
                children = list(fstore._children.values())
            self.descendents = children
        def __iter__(self):
            return self
        def __next__(self):
            if self.descendents:
                desc = self._fs._get_file_by_id(self.descendents.pop(0))
                if desc.get('__children'):
                    self.descendents.extend(desc.get('__children', {}).values())
                return desc
            raise StopIteration()
            
    def get_ids_in_subcoll(self, collpath: str) -> [str]:
        children = self._children
        if collpath != "":
            try:
                coll = self._get_file_by_path(collpath)
            except ObjectNotFound:
                return []
            else:
                children = coll.get('__children', [])

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
            if '__children' not in coll:
                coll['__children'] = OrderedDict()
            children = coll['__children']

        # create an inverted child map
        byid = OrderedDict( [(itm[1], itm[0]) for itm in children.items()] )
        ids = list(ids)
        for id in byid:
            if id not in ids:
                ids.append(id)

        # reorder the original map
        children.clear()
        for id in ids:
            if id in byid:
                children[byid[id]] = id

    def exists(self, id):
        return id in self._files

    def delete_file(self, id: str) -> bool:
        if id not in self._files:
            return False

        fmd = self._get_file_by_id(id)

        # deregister it with its parent
        self._deregister_from_parent(fmd['filepath'])

        # now forget the file entry
        del self._files[id]
        return True

    def _deregister_from_parent(self, filepath):
        if '/' in filepath:
            try:
                parent = self._get_file_by_path(self._dirname(filepath))
                name = self._basename(filepath)
                if name in parent.get('__children',{}):
                    del parent['__children'][name]
            except ObjectNotFound:
                pass
        else:
            if filepath in self._children:
                del self._children[filepath]

    def _register_with_parent(self, filepath, id):
        children = self._children
        name = filepath
        if '/' in filepath:
            parent = self._get_file_by_path(self._dirname(filepath))
            name = self._basename(filepath)
            if not self.is_collection(parent):
                raise  ObjectNotFound(parent, message=self._dirname(filepath)+": Not a subcollection")
            if '__children' not in parent:
                parent['__children'] = OrderedDict()
            children = parent['__children']

        children[name] = id
        

    def set_file_at(self, md, filepath: str=None, id=None, as_coll: bool=None) -> str:
        """
        add or update a file component.  If `id` is given (or otherwise included in the metadata as 
        the `@id` property) and it already exists in the file list, its metadata will be replaced
        with the data provided; if it does not exist, then the `filepath` will be used to locate and 
        update an existing file.  If a file matching either the `id` nor the `filepath` does not exist,
        a new file is added with the given file path (or with the path given in the metadata); if the 
        file does not have an identifier, a new one will be assigned.  If the previously existing file
        with the given identifier has a file path different from the given `filepath`; the file component 
        will be effectively moved to that file with the new metadata.  

        The implementation may require that the parent subcollection referenced in `filepath` exist already.

        :return: the identifier assigned to the file component
                 :rtype: str
        :raises FilepathNotSpecified: if the file path is not set via `filepath` parameter and cannot 
                 otherwise be determined via the previous or updated metadata.  
        :raises ObjectNotFound:  if the implementation requires that parent subcollections already exist
                 but does not.  
        """
        # first, make sure we have both an id and a filepath for the input metadata
        if not id:
            id = md.get('@id')
        oldfile = None
        if id:
            try:
                oldfile = self._get_file_by_id(id)
            except ObjectNotFound:
                pass

        if not filepath:
            filepath = md.get('filepath')
        if not filepath and oldfile:
            filepath = oldfile.get('filepath')
        if not filepath:
            raise ValueError("set_file_at(): filepath must be provided")

        destfile = None
        try:
            destfile = self._get_file_by_path(filepath)
        except ObjectNotFound:
            pass
        if not oldfile:
            oldfile = destfile

        if oldfile and not id:
            id = oldfile.get('@id')

        as_coll = [SUBCOLL_TYPE] if as_coll is True else None

        md = self._import_file(md, filepath, id, as_coll)  # assigns an @id if needed

        # Note: at this point,
        #   oldfile = existing file with same id as md
        #  destfile = existing file with same filepath as md

        # ensure the parent collection exists
        if '/' in filepath and not self.path_is_collection(self._dirname(filepath)):
            raise ObjectNotFound(self._dirname(filepath))

        # 
        deldestfile = False
        if destfile and self.is_collection(destfile) and \
           (destfile['@id'] != md['@id'] or not self.is_collection(md)):
            if destfile.get('__children'):
                # destination is a non-empty collection: don't clobber collections
                raise CollectionRemovalDissallowed(destfile['filepath'], "collection is not empty")
            deldestfile = True

        if oldfile:
            if self.is_collection(oldfile) and self.is_collection(md):
                # updating a collection; preserve its contents
                md['__children'] = oldfile.get('__children')
                if md['__children'] is None:
                    md['__children'] = OrderedDict()

            if filepath != oldfile.get('filepath'):
                # this is a file move; deregister it from its old parent
                self._deregister_from_parent(oldfile['filepath'])

        # save this record
        self._files[md['@id']] = md

        if deldestfile and destfile['@id'] in self._files:
            # delete the old destination file
            del self._files[destfile['@id']]

        # register the new file with its parent
        self._register_with_parent(md['filepath'], md['@id'])

        return md['@id']

    def exists(self, id: str) -> bool:
        return id in self._files

    def path_exists(self, filepath) -> bool:
        try:
            return bool(self._get_file_by_path(filepath))
        except ObjectNotFound:
            return False
            
    def path_is_collection(self, filepath) -> bool:
        try:
            return self.is_collection(self._get_file_by_path(filepath))
        except ObjectNotFound:
            return False

    def _import_file(self, fmd: Mapping, filepath: str=None, id: str=None, astype=None):
        # Copy and convert the file metadata into the form that is held internally
        out = OrderedDict([copy.deepcopy(m) for m in fmd.items() if m[0] != 'has_member'])
        if filepath:
            out['filepath'] = filepath
        if id:
            out['@id'] = id
        if astype:
            if isinstance(astype, str):
                astype = [astype]
            if isinstance(astype, (tuple, list)):
                out['@type'] = list(astype)

        if out.get('@id'):
            m = _idre.search(out['@id'])
            if m:
                # the id was set by a previous call to this class's minter
                # extract the number to ensure future ids are unique
                n = int(m.group(1))
                if n > self._ididx:
                    self._ididx = n + 1
        else:
            out['@id'] = self._get_default_id_for(out)

        if not out.get('filepath'):
            # Missing a filepath (avoid this); set to default
            out['filepath'] = self._basename(out['@id'])

        if not out.get('@type'):
            # Assume that this should be a regular file
            out['@type'] = [DATAFILE_TYPE, DOWNLOADABLEFILE_TYPE]
                
        # if self.is_collection(fmd) and 'has_member' in fmd:
        #     # convert 'has_member' to '__children'
        #     out['__children'] = OrderedDict()
        #     for child in fmd['has_member']:
        #         if '@id' in child and 'filepath' in child:
        #             out['__children'][self._basename(child['filepath'])] = child['@id']
        return out

class InMemoryResource(NERDResource):
    """
    an in-memory implementation of the NERDResource interface
    """
    _subprops = "authors references components".split()

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
        rec = OrderedDict()
        for itm in md.items():
            if itm[0] not in self._subprops:
                rec[itm[0]] = copy.deepcopy(itm[1])
        self._data = rec

    @property
    def deleted(self):
        return self._data is None

    def delete(self):
        if not self.deleted:
            self._data = None
            self._files = None
            self._nonfiles = None
            self._refs = None
            self._auths = None

    def get_res_data(self):
        if self._data is None:
            return None
        out = copy.deepcopy(self._data)
        if '@id' not in out:
            out['@id'] = self.id
        return out
        
    def get_data(self, inclfiles=True) -> Mapping:
        out = self.get_res_data()
        if out is None:
            return None

        if self._auths.count > 0:
            out['authors'] = self._auths.get_data()
        if self._refs.count > 0:
            out['references'] = self._refs.get_data()
        if self._nonfiles.count > 0 or self._files.count > 0:
            out['components'] = []
            if self._nonfiles.count > 0:
                out['components'].extend(self._nonfiles.get_data())
            if self._files.count > 0:
                out['components'].extend(self._files.get_files())
        return out
        
class InMemoryResourceStorage(NERDResourceStorage):
    """
    a factory for opening records stored in memory
    """

    @classmethod
    def from_config(cls, config: Mapping, logger: Logger):
        """
        an class method for creatng an FSBasedResourceStorage instance from configuration data.

        Recognized configuration paramters include:
        ``default_shoulder``
             (str) _optional_. The shoulder that new identifiers are minted under.  This is not 
             normally used as direct clients of this class typically choose the shoulder on a 
             per-call basis.  The default is "nrd".

        :param dict config:  the configuraiton for the specific type of storage
        :param Logger logger:  the logger to use to capture messages
        """
        return cls(config.get("default_shoulder", "nrd"), logger=logger)
        
    def __init__(self, newidprefix: str="nrd", existing: List[Mapping]=[], logger: Logger=None):
        """
        initialize a factory with some existing in-memory NERDm records
        :param str  newidprefix:  a prefix to use when minting new identifiers
        :param [Mapping] existing:  a list of NERDm records that should be made available via 
                                    :py:method:`open`.
        """
        self.log = logger
        self._pfx = newidprefix
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
            m = _idre.search(id) if id else None
            if m:
                n = int(m.group(1))
                if n >= self._ididx:
                    self._ididx = n + 1
        if not id:
            id = self._new_id()
        if id in self._recs:
            self._recs[id].replace_all_data(rec)
        else:
            self._recs[id] = InMemoryResource(id, rec, self.log)

    def _new_id(self):
        out = "%s_%d" % (self._pfx, self._ididx)
        self._ididx += 1
        return out

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
            self._recs[id] = InMemoryResource(id, OrderedDict(), self.log)
        return self._recs[id]

    def exists(self, id: str) -> bool:
        return id in self._recs

