"""
an implementation of the NERDResource storage interface that stores the data in JSON files on disk

This is provided for development purposes that requires only simple logical storage operations.  See 
:py:mod:`nerdstore.base<nistoar.pdr.draft.nerdstore.base` for full interface documentation.  
"""

# See .base.py for function documentation

import os, copy, re, math, json, shutil, logging
from collections import OrderedDict
from collections.abc import Mapping
from logging import Logger
from typing import Iterable, Iterator
from pathlib import Path

from .base import *
from .base import _NERDOrderedObjectList, DATAFILE_TYPE, SUBCOLL_TYPE, DOWNLOADABLEFILE_TYPE

from nistoar.pdr.utils import read_json, write_json

_idre = re.compile(r"^\w+_(\d+)$")
_arkre = re.compile(r'^ark:/\d+/')

class FSBasedObjectList(_NERDOrderedObjectList):
    """
    A file-based implementation of the _NERDOrderedObjectList interface where the list is kept in JSON files
    """
    _pfx = "obj"
    _idfile = "_ids.json"
    _seqfile = "_seq.json"

    def __init__(self, resource: NERDResource, objdir: str):
        super(FSBasedObjectList, self).__init__(resource)
        self._dir = Path(objdir)
        if not self._dir.is_dir():
            raise StorageFormatException("%s: does not exist as a directory" % str(objdir))
        self._idp = self._dir / self._idfile
        self._seqp = self._dir / self._seqfile
        self._order = self._read_ids()
        self._nxtseq = self._read_next_seq()

    def _read_ids(self):
        if not self._idp.exists():
            # No ID listing file, yet; create one based on the directory contents
            ids = list(self._discover_ids())
            self._cache_ids(ids)

        else:
            try:
                ids = read_json(str(self._idp))
            except (ValueError, IOError) as ex:
                raise StorageFormatException("%s: failed to load JSON data: %s"
                                           % (str(self._idp), str(ex)))
            if not isinstance(ids, list) or any([not isinstance(i, str) for i in ids]):
                raise StorageFormatException("%s: does not contain a list of str")

        return ids

    def _discover_ids(self):
        for jf in os.listdir(self._dir):
            if jf.endswith(".json") and not jf.startswith(".") and not jf.startswith("_"):
                yield os.path.splitext(jf)[0].replace("::", os.sep)

    def _cache_ids(self, ids=None):
        if ids is None:
            ids = self._order
        write_json(ids, str(self._idp))

    def _obj_file(self, id):
        id = id.replace(os.sep,'::')
        return self._dir / (id + ".json")

    def empty(self):
        if self._res.deleted:
            raise RecordDeleted(self._res.id, "empty")
        self._order = []
        if self._idp.is_file():
            self._idp.unlink()

        for id in self._discover_ids():
            file = self._obj_file(id)
            if file.is_file():
                file.unlink()

        self._cache_ids()

    def _read_next_seq(self):
        nxt = 0
        if self._seqp.is_file():
            try:
                nxt = read_json(self._seqp)
            except (ValueError, IOError) as ex:
                raise StorageFormatException("%s: Failed to read file as JSON: %s" 
                                             % (str(self._seqp), str(ex)))
        if not isinstance(nxt, int):
            raise StorageFormatException("%s: ID sequence file does not contain an integer")

        return nxt

    def _cache_next_seq(self, nxt: int = None):
        if nxt is None:
            nxt = self._nxtseq
        try:
            write_json(nxt, self._seqp)
        except IOError as ex:
            raise StorageFormatException("%s: Failed to write ID sequence file: %s"
                                         % (str(self._seqp), str(ex)))        

    def _new_id(self):
        out = "%s_%d" % (self._pfx, self._nxtseq)
        nxt = self._nxtseq + 1
        self._cache_next_seq(nxt)
        self._nxtseq = nxt
        return out

    def _reserve_id(self, id):
        m = _idre.search(id)
        if m:
            # the id was set by a previous call to this class's minter
            # extract the number to ensure future ids are unique
            n = int(m.group(1))
            if n >= self._nxtseq:
                self._idseq = n + 1
                self._cache_next_seq()

    @property
    def ids(self) -> [str]:
        return list(self._order)

    @property
    def count(self) -> int:
        return len(self._order)

    def _get_item_by_id(self, id: str) -> Mapping:
        objf = self._obj_file(id)
        if not objf.exists():
            raise ObjectNotFound(id)

        try:
            return read_json(str(objf))
        except (ValueError, IOError) as ex:
            raise StorageFormatException("%s: Failed to read file as JSON: %s" 
                                         % (str(self._seqp), str(ex)))

    def _get_item_by_pos(self, pos: int) -> Mapping:
        try: 
            return self._get_item_by_id(self._order[pos])
        except IndexError:
            raise ObjectNotFound("position="+str(pos))

    def set_order(self, ids: Iterable[str]): 
        neworder = []
        for id in ids:
            if id not in neworder and id in self._order:
                neworder.append(id)
        for id in self._order:
            if id not in neworder:
                neworder.append(id)
        self._order = neworder
        self._cache_ids()

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
            # ensure existence of data
            jf = self._obj_file(idorpos)
            if not jf.exists():
                raise ObjectNotFound(idorpos)
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
        if pos == oldpos:
            return pos

        id = self._order.pop(oldpos)
        if pos > len(self._order):
            self._order.append(id)
            return len(self._order) -1

        elif pos < 0:
            pos = 0
            
        self._order.insert(pos, id)
        self._cache_ids()
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

        try:
            write_json(md, self._obj_file(id))
        except (TypeError, IOError) as ex:
            raise StorageFormatException("%s: Failed to write object file: %s"
                                         % (str(self._seqp), str(ex)))
        self._order = neworder
        self._cache_ids()

    def _remove_item(self, id: str):
        out = self._get_item_by_id(id)   # may raise ObjectNotFound

        if id in self._order:
            self._order.remove(id)
        self._cache_ids()

        return out

class FSBasedAuthorList(FSBasedObjectList, NERDAuthorList):
    """
    an file-based implementation of the NERDAuthorList interface
    """
    _pfx = "auth"
    def __init__(self, resource: NERDResource, authdir: str):
        FSBasedObjectList.__init__(self, resource, authdir)

    def load_authors(self, authors: Iterable[Mapping]) -> int:
        for auth in authors:
            # any validity checking?
            self.append(auth)

class FSBasedRefList(FSBasedObjectList, NERDRefList):
    """
    an file-based implementation of the NERDRefList interface
    """
    _pfx = "ref"
    def __init__(self, resource: NERDResource, refdir: str):
        FSBasedObjectList.__init__(self, resource, refdir)

    def load_references(self, refs: Iterable[Mapping]) -> int:
        for ref in refs:
            # any validity checking?
            self.append(ref)

class FSBasedNonFileComps(FSBasedObjectList, NERDNonFileComps):
    """
    an file-based implementation of the NERDNonFileComps interface
    """
    _pfx = "cmp"
    def __init__(self, resource: NERDResource, nfcmpdir: str):
        FSBasedObjectList.__init__(self, resource, nfcmpdir)

    def load_nonfile_components(self, cmps: Iterable[Mapping]) -> int:
        for cmp in cmps:
            if 'filepath' not in cmp:
                # any more validity checking?
                self.append(cmp)


class FSBasedFileComps(NERDFileComps):
    """
    an file-based implementation of the NERDFileComps interface
    """
    _pfx = "file"
    _chldfile = "_children.json"
    _seqfile = "_seq.json"
    
    def __init__(self, resource: NERDResource, filedir: str, iscollf=None):
        super(FSBasedFileComps, self).__init__(resource, iscollf)
        self._dir = Path(filedir)
        if not self._dir.is_dir():
            raise StorageFormatException("%s: does not exist as a directory" % str(filedir))
        self._chldp = self._dir / self._chldfile      # map: top-level file names to their IDs
        self._seqp = self._dir / self._seqfile        # the path where the next sequence is cached
        self._nxtseq = self._read_next_seq()          # the next available sequence # for assigned IDs
        self._children = self._read_toplevel_files()  # the files at the top of the hierarchy

    def _read_next_seq(self):
        nxt = 0
        if self._seqp.is_file():
            try:
                nxt = read_json(self._seqp)
            except (ValueError, IOError) as ex:
                raise StorageFormatException("%s: Failed to read file as JSON: %s" 
                                             % (str(self._seqp), str(ex)))
        if not isinstance(nxt, int):
            raise StorageFormatException("%s: ID sequence file does not contain an integer")

        return nxt

    # identifiers and sequence numbers
    #
    def _cache_next_seq(self, nxt: int = None):
        if nxt is None:
            nxt = self._nxtseq
        try:
            write_json(nxt, self._seqp)
        except IOError as ex:
            raise StorageFormatException("%s: Failed to write ID sequence file: %s"
                                         % (str(self._seqp), str(ex)))        

    def _new_id(self):
        out = "%s_%d" % (self._pfx, self._nxtseq)
        nxt = self._nxtseq + 1
        self._cache_next_seq(nxt)
        self._nxtseq = nxt
        return out

    def _reserve_id(self, id):
        m = _idre.search(id)
        if m:
            # the id was set by a previous call to this class's minter
            # extract the number to ensure future ids are unique
            n = int(m.group(1))
            if n >= self._nxtseq:
                self._nxtseq = n + 1
                self._cache_next_seq()

    # self._children: the contents of the implicit, top-level collection
    #
    def _cache_children(self, children=None):
        if children is None:
            children = self._children
        try:
            write_json(children, str(self._chldp))
        except IOError as ex:
            raise StorageFormatException("%s: Failed to write top collection data: %s"
                                         % (str(self._chldp), str(ex)))        

    def _read_toplevel_files(self):
        if not self._chldp.exists():
            # child listing file, yet; create one based on the directory contents
            children = OrderedDict(self._discover_toplevel_files())
            self._cache_children(children)

        else:
            try:
                children = read_json(str(self._chldp))
            except (ValueError, IOError) as ex:
                raise StorageFormatException("%s: failed to load JSON data: %s"
                                           % (str(self._chldp), str(ex)))
            if not isinstance(children, OrderedDict) or \
               any([not isinstance(v, str) for v in children.values()]):
                raise StorageFormatException("%s: does not contain a name-id map")

        return children

    def _discover_toplevel_files(self):
        failed = []
        found = []
        for jf in os.listdir(self._dir):
            if jf.endswith(".json") and not jf.startswith(".") and not jf.startswith("_"):
                found.append(jf)
                try:
                    md = read_json(str(self._dir / jf))
                except (ValueError, IOError) as ex:
                    failed.append(jf)
                if '/' not in md.get('filepath', '/'):
                    id = md.get('@id')
                    if not id:
                        id = os.path.splitext(md['filepath'])[0]
                    yield (md['filepath'], id)

    # find file metadata
    #
    def _fmd_file(self, id, is_coll: bool):
        id = id.replace(os.sep,'::')
        pfx = "c:" if is_coll else "f:"
        return self._dir / (pfx + id + ".json")

    def _is_coll_mdfile(self, mdfile: Path):
        if not mdfile:
            return False
        return mdfile.name.startswith("c:")

    def _find_fmd_file(self, id):
        loc = self._fmd_file(id, False)
        if not loc.exists():
            loc = self._fmd_file(id, True)
            if not loc.exists():
                loc = None
        return loc

    def exists(self, id: str) -> bool:
        return bool(self._find_fmd_file(id))

    def get_file_by_id(self, id: str) -> Mapping:
        return self._export_file(self._get_file_by_id(id))

    def _get_file_by_id(self, id: str) -> Mapping:
        mdf = self._find_fmd_file(id)
        if not mdf:
            raise ObjectNotFound(id)
        return self._read_file_md(mdf)

    def _read_file_md(self, mdfile: Path) -> Mapping:
        try:
            return read_json(str(mdfile))
        except (ValueError, IOError) as ex:
            raise StorageFormatException("%s: Failed to read file metadata as JSON: %s" 
                                         % (str(mdf), str(ex)))

    def _export_file(self, fmd):
        out = OrderedDict([m for m in fmd.items() if not m[0].startswith("__")])
        if self.is_collection(out):
            out['has_member'] = [OrderedDict([('@id', m[1]), ('name', m[0])])
                                 for m in fmd.get("__children",{}).items()]
        return out

    def get_file_by_path(self, path: str) -> Mapping:
        if not path:
            raise ValueError("get_file__path(): No path specified")
        return self._export_file(self._get_file_by_path(path))
        
    def _get_file_by_path(self, path: str) -> Mapping:
        return self._get_file_by_id(self._find_fmd_id_by_path(path))

    def _find_fmd_id_by_path(self, path: str) -> str:
        return self._find_fmd_id_by_relpath(self._children, path.split('/'), path)

    def _find_fmd_id_by_relpath(self, children: Mapping, steps: [str], origpath):
        top = steps.pop(0)
        if top not in children:
            raise ObjectNotFound(origpath)

        if not steps:
            return children[top]

        mdf = self._find_fmd_file(children[top])
        if not self._is_coll_mdfile(mdf):
            raise ObjectNotFound(origpath)
        fmd = self._read_file_md(mdf)

        return self._find_fmd_id_by_relpath(fmd.get("__children", {}), steps, origpath)

    def path_exists(self, filepath) -> bool:
        try:
            return self.exists(self._find_fmd_id_by_path(filepath))
        except ObjectNotFound:
            return False

    def path_is_collection(self, filepath) -> bool:
        try:
            id = self._find_fmd_id_by_path(filepath)
            if not id:
                return False
            return self._is_coll_mdfile(self._find_fmd_file(id))
        except ObjectNotFound:
            return False

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

    @property
    def ids(self):
        return list(self.iter_ids())

    def iter_ids(self):
        return iter(self._IDIterator(self))

    class _IDIterator:
        def __init__(self, fstore, children=None):
            self._fs = fstore
            if children is None:
                children = list(fstore._children.values())
            self.descendents = children
        def __iter__(self):
            return self
        def __next__(self):
            if self.descendents:
                desc = self.descendents.pop(0)
                mdf = self._fs._find_fmd_file(desc)
                if mdf and self._fs._is_coll_mdfile(mdf):
                    descmd = self._fs._read_file_md(mdf)
                    if descmd.get('__children'):
                        self.descendents.extend(descmd.get('__children', {}).values())
                return desc
            raise StopIteration()

    def iter_files(self):
        for id in self.iter_ids():
            yield self.get_file_by_id(id)

    # manipulate files (via their metadata
    #
    def _cache_file_md(self, fmd):
        fmdf = self._fmd_file(fmd['@id'], self.is_collection(fmd))
        try:
            write_json(fmd, fmdf)
        except IOError as ex:
            raise StorageFormatException("%s: Failed to write file metadata: %s" % (str(fmdf), str(ex)))

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
            self._reserve_id(out['@id'])
        else:
            out['@id'] = self._new_id()

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

        # Are we "writing over" an existing file?
        deldestfile = False
        if destfile and \
           (destfile['@id'] != md['@id'] or self.is_collection(destfile) != self.is_collection(md)):
            if destfile.get('__children'):
                # destination is a non-empty collection: won't clobber it
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
        self._cache_file_md(md)

        if deldestfile:
            # delete the old destination file
            mdf = self._find_fmd_file(destfile['@id'])
            if mdf:
                mdf.unlink()

        # register the new file with its parent
        self._register_with_parent(md['filepath'], md['@id'])

        return md['@id']

    def _register_with_parent(self, filepath, id):
        if '/' in filepath:
            parent = self._get_file_by_path(self._dirname(filepath))
            name = self._basename(filepath)
            if not self.is_collection(parent):
                raise  ObjectNotFound(parent, message=self._dirname(filepath)+": Not a subcollection")
            if '__children' not in parent:
                parent['__children'] = OrderedDict()
            parent['__children'][name] = id
            self._cache_file_md(parent)

        else:
            self._children[filepath] = id
            self._cache_children()

    def _deregister_from_parent(self, filepath):
        if '/' in filepath:
            try:
                parent = self._get_file_by_path(self._dirname(filepath))
                name = self._basename(filepath)
                if name in parent.get('__children',{}):
                    del parent['__children'][name]
                    self._cache_file_md(parent)
            except ObjectNotFound:
                pass
        else:
            if filepath in self._children:
                del self._children[filepath]
                self._cache_children()

    def load_file_components(self, cmps):
        # Note that this implementation makes no assumptions about what order the components
        # appear in.
        
        # Once through to load all files by their ID
        for cmp in cmps:
            if cmp.get('filepath'):
                if cmp.get('@id'):
                    self._reserve_id(cmp.get('@id'))
                    self.set_file_at(cmp, cmp['filepath'], cmp['@id'])

        # Go through again to (1) assign ids to file components that are missing one,
        # and (2) create a map from parent subcollections to their children
        children = {'': []}
        saved = set()
        subcolls = []
        for cmp in cmps:
            if cmp.get('filepath'):
                id = cmp.get('@id')
                if not id:
                    # assign an ID to file component missing one
                    id = self.set_file_at(cmp, cmp['filepath'])

                # build parent-children map
                if '/' in cmp['filepath']:
                    parent= self._dirname(cmp['filepath'])
                    if parent not in children:
                        children[parent] = []
                    children[parent].append( (self._basename(cmp['filepath']), id) )
                else:
                    children[''].append( (cmp['filepath'], id) )
                    self._children[cmp['filepath']] = id

                saved.add(id)

                # remember subcollections
                if self.is_collection(cmp):
                    cmp['@id'] = id
                    subcolls.append(cmp)

        # Go through a last time to set the subcollection content info into each subcollection component
        for cmp in subcolls:
            if cmp.get('filepath') in children:
                if '__children' not in cmp:
                    cmp['__children'] = OrderedDict()

                # base subcollection contents first on 'has_member' list as this captures order info
                if cmp.get('has_member'):
                    if isinstance(cmd.get('has_member',[]), str):
                        cmp['has_member'] = [cmp['has_member']]
                    for child in cmp['has_member']:
                        if child.get('@id') in saved and child.get('name'):
                            cmp['__children'][child['name']] = child.get('@id')

                # capture any that got missed by 'has_member'
                for child in children[cmp['filepath']]:
                    if child[0] not in cmp['__children']:
                        cmp['__children'][child[0]] = child[1]

                self.set_file_at(cmp)

    def delete_file(self, id: str) -> bool:
        if self._res.deleted:
            raise RecordDeleted(self._res.id, "empty")

        try:
            fmd = self._get_file_by_id(id)
        except ObjectNotFound:
            return False

        # deregister it with its parent
        self._deregister_from_parent(fmd['filepath'])

        # now delete the file entry
        self._find_fmd_file(id).unlink()
        return True

    def empty(self):
        if self._res.deleted:
            raise RecordDeleted(self._res.id, "empty")
        self._children = OrderedDict()
        if self._chldp.is_file():
            self._chldp.unlink()

        for id in os.listdir(self._dir):
            if jf.endswith(".json") and not jf.startswith(".") and not jf.startswith("_"):
                if file.is_file():
                    file.unlink()

        self._cache_children()

    def set_order_in_subcoll(self, collpath: str, ids: Iterable[str]) -> Iterable[str]:
        if self._res.deleted:
            raise RecordDeleted(self._res.id, "empty")
        children = self._children
        coll = None
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

        if coll:
            self._cache_file_md(coll)
        else:
            self._cache_children()


class FSBasedResource(NERDResource):
    """
    a file-based implementation of the NERDResource interface in which all data are stored in JSON 
    files on disk.
    """

    _subprops = "authors references components @id".split()

    def __init__(self, id: str, storeroot: str, create: bool=True, parentlog: Logger=None):
        super(FSBasedResource, self).__init__(id, parentlog)
        storeroot = Path(storeroot)
        if not storeroot.is_dir():
            raise StorageFormatException("%s: does not exist as a directory" % str(storeroot))
        if not os.access(storeroot, os.R_OK|os.W_OK|os.X_OK):
            raise StorageFormatException("%s: directory not writeable" % str(storeroot))

        self._dir = storeroot / _arkre.sub('', self.id).replace(os.sep, '::')
        self._resmdfile = self._dir / "res.json"

        self._auths = None
        self._refs  = None
        self._files = None
        self._nonfiles = None

        if create and not self._dir.exists():
            self._create_empty()

    def _create_empty(self):
        if not self._dir.exists():
            self._dir.mkdir()
        self._cache_res_md({ "@id": self.id })

    @property
    def authors(self):
        if self.deleted:
            raise RecordDeleted(self.id, "get metadata")
        if not self._auths:
            dir = self._dir / "auths"
            if not dir.exists():
                dir.mkdir()
            self._auths = FSBasedAuthorList(self, dir)
        return self._auths

    @property
    def references(self):
        if self.deleted:
            raise RecordDeleted(self.id, "get metadata")
        if not self._refs:
            dir = self._dir / "refs"
            if not dir.exists():
                dir.mkdir()
            self._refs = FSBasedRefList(self, dir)
        return self._refs

    @property
    def nonfiles(self):
        if self.deleted:
            raise RecordDeleted(self.id, "get metadata")
        if not self._nonfiles:
            dir = self._dir / "nonfiles"
            if not dir.exists():
                dir.mkdir()
            self._nonfiles = FSBasedNonFileComps(self, dir)
        return self._nonfiles

    @property
    def files(self):
        if self.deleted:
            raise RecordDeleted(self.id, "get metadata")
        if not self._files:
            dir = self._dir / "files"
            if not dir.exists():
                dir.mkdir()
            self._files = FSBasedFileComps(self, dir)
        return self._files

    @property
    def deleted(self):
        return not self._dir.exists()

    def delete(self):
        if not self.deleted:
            self._data = None
            self._files = None
            self._nonfiles = None
            self._refs = None
            self._auths = None
            shutil.rmtree(self._dir)

    def _cache_res_md(self, md):
        if self.deleted:
            self._create_empty()
        try:
            write_json(md, self._resmdfile)
        except IOError as ex:
            raise StorageFormatException("%s: Failed to write file metadata: %s"
                                         % (str(self._seqp), str(ex)))

    def replace_res_data(self, md): 
        self._cache_res_md(md)

    def get_res_data(self) -> Mapping:
        if self.deleted:
            return None
        try:
            return read_json(self._resmdfile)
        except (ValueError, IOError) as ex:
            raise StorageFormatException("%s: Failed to read resource metadata as JSON: %s" 
                                         % (str(self._resmdfile), str(ex)))

    def get_data(self, inclfiles=True) -> Mapping:
        out = self.get_res_data()
        if out is None:
            return None

        if self.authors.count > 0:
            out['authors'] = self.authors.get_data()
        if self.references.count > 0:
            out['references'] = self.references.get_data()
        if self.nonfiles.count > 0 or self.files.count > 0:
            out['components'] = []
            if self.nonfiles.count > 0:
                out['components'].extend(self.nonfiles.get_data())
            if self.files.count > 0:
                out['components'].extend(self.files.get_files())
        return out
        

class FSBasedResourceStorage(NERDResourceStorage):
    """
    a factory for opening records stored in the JSON files on disk
    """
    _seqfile = "_seq.json"
    _idre = re.compile(r'^\w+\d*:0*(\d+)$')

    @classmethod
    def from_config(cls, config: Mapping, logger: Logger):
        """
        an class method for creatng an FSBasedResourceStorage instance from configuration data.

        Recognized configuration paramters include:

        ``store_dir``
             (str) _required_. The root directory under which all resource data will be stored.
        ``default_shoulder``
             (str) _optional_. The shoulder that new identifiers are minted under.  This is not 
             normally used as direct clients of this class typically choose the shoulder on a 
             per-call basis.  The default is "nrd".

        :param dict config:  the configuraiton for the specific type of storage
        :param Logger logger:  the logger to use to capture messages
        """
        if not config.get('store_dir'):
            raise ConfigurationException("Missing required configuration parameter: store_dir")
        
        return cls(config['store_dir'], config.get("default_shoulder", "nrd"), logger)
        
    def __init__(self, storeroot: str, newidprefix: str="nrd", logger: Logger=None):
        """
        initialize a factory with with the resource data storage rooted at a given directory
        :param str  newidprefix:  a prefix to use when minting new identifiers
        """
        self._dir = Path(storeroot)
        if not self._dir.is_dir():
            raise StorageFormatException("%s: does not exist as a directory" % str(self._dir))
        if not os.access(self._dir, os.R_OK|os.W_OK|os.X_OK):
            raise StorageFormatException("%s: directory not writeable" % str(self._dir))

        self._pfx = newidprefix
        self._seqp = self._dir / self._seqfile        # the path where the next sequence is cached
        self._nxtseq = self._read_next_seq()          # the next available sequence # for assigned IDs

        if not logger:
            logger = logging.getLogger("nerdstore")
        self._log = logger

    def _new_id(self):
        out = "{0}:{1:04d}".format(self._pfx, self._nxtseq)
        nxt = self._nxtseq + 1
        self._cache_next_seq(nxt)
        self._nxtseq = nxt
        return out

    def _reserve_id(self, id):
        m = self._idre.search(_arkre.sub('', id))
        if m:
            n = int(m.group(1))
            if n >= self._nxtseq:
                self._nxtseq = n + 1
                self._cache_next_seq()

    def _read_next_seq(self):
        nxt = 1
        if self._seqp.is_file():
            try:
                nxt = read_json(self._seqp)
            except (ValueError, IOError) as ex:
                raise StorageFormatException("%s: Failed to read file as JSON: %s" 
                                             % (str(self._seqp), str(ex)))
        if not isinstance(nxt, int):
            raise StorageFormatException("%s: ID sequence file does not contain an integer")

        return nxt

    def _cache_next_seq(self, nxt: int = None):
        if nxt is None:
            nxt = self._nxtseq
        try:
            write_json(nxt, self._seqp)
        except IOError as ex:
            raise StorageFormatException("%s: Failed to write ID sequence file: %s"
                                         % (str(self._seqp), str(ex)))        

    def delete(self, id: str) -> bool:
        fn = self._dir / id.replace(os.sep, '::')
        if fn.is_dir():
            shutil.rmtree(fn)
            return True
        return False
    
    def open(self, id: str=None) -> NERDResource:
        if not id:
            id = self._new_id()
        return FSBasedResource(id, self._dir, True, self._log)
    

    def load_from(self, rec: Mapping, id: str=None):
        """
        load a NERDm record into this storage.  If the record exist
        :param Mapping rec:  the NERDm Resource record to load, given as a JSON-ready dictionary
        :param str id:       the ID to assign to the record; if not given, the value of the record's
                             `@id` property will be used or one will be created for it.
        """
        if not id:
            id = rec.get('@id')
        if id:
            self._reserve_id(id)
        else:
            id = self._new_id()

        res = self.open(id)
        res.replace_all_data(rec)

    def exists(self, id: str) -> bool:
        dir = self._dir / _arkre.sub('', id).replace(os.sep, "::")
        return dir.is_dir()



