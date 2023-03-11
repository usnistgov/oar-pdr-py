"""
Abstract base classes providing the interface to metadata storage.
"""
import logging, re
from abc import ABC, ABCMeta, abstractproperty, abstractmethod
from collections.abc import MutableMapping, Mapping, MutableSequence
from typing import Iterable, Iterator, NewType, List
from logging import Logger

import nistoar.nerdm.utils as nerdmutils
from nistoar.pdr.preserve.bagit.builder import (DATAFILE_TYPE, SUBCOLL_TYPE, DOWNLOADABLEFILE_TYPE)

__all__ = [ "NERDResource", "NERDAuthorList", "NERDRefList", "NERDNonFileComps", "NERDFileComps",
            "NERDStorageException", "MismatchedIdentifier", "RecordDeleted", "ObjectNotFound",
            "CollectionRemovalDissallowed", "NERDResourceStorage" ]

NERDResource     = NewType("NERDResource", ABC)
NERDAuthorList   = NewType("NERDAuthorList", NERDResource)
NERDRefList      = NewType("NERDRefList", NERDResource)
NERDFileComps    = NewType("NERDFileComps", NERDResource)
NERDNonFileComps = NewType("NERDNonFileComps", NERDResource)

class NERDResource(ABC):
    """
    an abstract base class representing a NERDm Resource record in storage.

    When instance construction is complete, it is assumed that the draft record now exists.  If it 
    did not exist before, it should be created and filled with some initial data.  Updates to the 
    metadata can be made via :py:method:`replace_res_data`, :py:method:`replace_all_data`, or more
    surgically via the constituent :py:property:`authors`, :py:property:`references`, 
    :py:property:`files`, and :py:property:`nonfiles` properties.  This interface does attempt to 
    ensure compliance with any standards or conventions or otherwise initialize any property values 
    by default (apart from ensuring all constituents have unique identifiers); this is the job of the 
    client.  Once an update is made via this interface, it is expected to be immediately persisted to
    the underlying storage.  
    """
    def __init__(self, id: str, parentlog: logging.Logger = None):
        self._id = id

        if not id:
            raise ValueError("NERDResource: base init requires id")
        if not parentlog:
            parentlog = logging.getLogger("nerdstore")
        self.log = parentlog.getChild(id)

    @property
    def id(self):
        """
        the identify for the draft record that this NERDm data describes.  This cannot be changed 
        once assigned and the record data is persisted to storage.  
        """
        return self._id

    @abstractproperty
    def authors(self) -> NERDAuthorList:
        """
        the interface to the list of authors
        """
        raise NotImplementedError()

    @abstractproperty
    def files(self) -> NERDFileComps:
        """
        the interface to the list of files
        """
        raise NotImplementedError()

    @abstractproperty
    def nonfiles(self) -> NERDNonFileComps:
        """
        the interface to the non-file components
        """
        raise NotImplementedError()

    @abstractproperty
    def references(self) -> NERDRefList:
        """
        the interface to the list of references
        """
        raise NotImplementedError()

    @abstractmethod
    def replace_res_data(self, md): 
        """
        replace all resource-level properties excluding `components`, `authors`, and `references`
        from the provided metadata.  
        """
        raise NotImplementedError()

    def replace_all_data(self, md):
        """
        replace all data provide in the given metadata model
        """
        self.replace_res_data(md)
        if isinstance(md.get('authors'), list):
            self.authors.empty()
            for auth in md.get('authors', []):
                self.authors.append(auth)
        if isinstance(md.get('references'), list):
            self.references.empty()
            for ref in md.get('references', []):
                self.references.append(auth)
        if isinstance(md.get('components'), list):
            self.nonfiles.empty()
            self.files.empty()
            for cmp in md['components']:
                if 'filepath' in cmp:
                    self.files.set_file_at(cmp, cmp['filepath'])
                else:
                    self.nonfiles.append(cmp)

    @abstractmethod
    def delete(self):
        """
        delete this resource record from storage.  After this is called, the record cannot be 
        saved or updated further.  If the the record is already deleted, this method does nothing.
        """
        raise NotImplementedError()

    @abstractproperty
    def deleted(self):
        """
        True if the :py:method:`delete` method was called on this record.  If True,
        calling :py:method:`replace_res_data` or :py:method:`resplace_all_data` will result in 
        a :py:class:`RecordDeleted` exception.
        """
        return False

    @abstractmethod
    def get_data(self, inclfiles=True) -> Mapping:
        """
        return the resource metadata as a JSON-ready dictionary

        :param bool inclfiles: if False, exclude the file-based components from the components list
        """
        raise NotImplementedError()

    @abstractmethod
    def get_res_data(self) -> Mapping:
        """
        return the resource metadata, excluding the authors, references, and all components
        """
        raise NotImplementedError()
        
class _NERDOrderedObjectList(metaclass=ABCMeta):
    """
    an abstract interface for updating the list of objects associated with a NERD Resource property
    """

    def __init__(self, resource: NERDResource):
        self._res = resource

    def get_data(self) -> [Mapping]:
        """
        return the current record as a NERDm dictionary
        """
        return [self._get_item_by_id(d) for d in self.ids]

    @abstractmethod
    def empty(self):
        """
        delete all items from this list
        """
        raise NotImplementedError()

    @abstractmethod
    def _get_item_by_id(self, id: str):
        """
        return the author description with the given identifier assigned to it
        :raise KeyError: if an author with the given identifier is not part of this list
        """
        raise NotImplementedError()
        
    @abstractmethod
    def _get_item_by_pos(self, pos: int):
        """
        return the author description with the given identifier assigned to it.  The given position
        can be negative; it will interpreted with normal Python list index semantics.
        :raise IndexError: if pos is out of range of the list of authors
        """
        raise NotImplementedError()

    @abstractproperty
    def ids(self) -> [str]:
        """
        return the list of identifiers for the items in this list in the order that they 
        should appear.
        """
        raise NotImplementedError()

    @abstractproperty
    def count(self) -> int:
        """
        return the number of items in this list.
        """
        raise NotImplementedError()

    @abstractmethod
    def set_order(self, ids: Iterable[str]):
        """
        set the order of the items.  After calling this function, the :py:property:`ids` return 
        the identifiers in this order.
        :param [str] ids:  the list of identifers contained in this instance's :py:property:`ids`,
                           rearranged into the desired order.  Any ids not in the list will be 
                           appended at the end of the list (in an arbitrary order)
        :raises KeyError:  if the given list contains an identifier that is not currently in this list
        """
        raise NotImplementedError()

    @abstractmethod
    def _set_item(self, id: str, md: Mapping, pos: int=None):
        """
        commit the given metadata into storage with the specified key and position
        :param str     id:  the identifier to assign to the item being added; if this item exists 
                            in the list already it will be replaced with the given metadata
        :param Mapping md:  the metadata to save
        :param int    pos:  the position in the list to insert the metadata item; items at that 
                            position and after it will increase its position by one.  If None and
                            `id` is already in the list, the metadata will replace that item with 
                            the ID at its current position.  If item is not in the list and `pos` 
                            is None or equal to the current number of items in the list (as given by 
                            :py:property:`count`), the metadata item will be appended.  `pos` can be 
                            negative; it will be interpreted with the negative index semantics of 
                            normal Python lists.
        :raises IndexError: if the position is greater than the current value of :py:property:`count`,
                            (the number of elements currently in the list).  
        """
        raise NotImplementedError()

    @abstractmethod
    def _remove_item(self, id: str):
        """
        delete and return the item from the list with the given identiifer
        """
        raise NotImplementedError()

    def __len__(self):
        return len(self.ids)

    def get(self, key):
        """
        return one of the items from this list.  Here, the key is either a string, indicating the 
        item identifier, or a an integer, indicating its position
        """
        if isinstance(key, int):
            return self._get_item_by_pos(key)
        return self._get_item_by_id(key)

    def __contains__(self, key):
        if isinstance(key, int):
            return key >= -1*self.count and key < self.count
        return key in self.ids

    def __iter__(self):
        ids = self.ids
        for id in ids:
            yield self._get_item_by_id(id)

    def set(self, key, md):
        if isinstance(key, int):
            itm = self._get_item_by_pos(key)
            key = itm.get('@id') if itm else None
            
        self._set_item(key, md)

    def _select_id_for(self, md):
        id = md.get('@id')
        if not id:
            id = self._get_default_id_for(md)
        return id

    @abstractmethod
    def _get_default_id_for(self, md):
        """
        determine an appropriate identifier for the given list item metadata; this is usually the 
        value of a particular, custom property.  If a value cannot be determined based on the metadata,
        a new unique identifier should be created and returned.  
        """
        raise NotImplementedError()

    @abstractmethod
    def _new_id(self):
        raise NotImplementedError()

    @abstractmethod
    def _reserve_id(self, id):
        """
        if necessary, ensure that :py:method:`_new_id` will not create an identifier that 
        matches the given one.  This allows method :py:method:`append` and :py:method:`insert` to 
        accept identifier's given to it.  
        """
        raise NotImplementedError()
        
    def insert(self, pos, md):
        """
        inserts a new item into the specified position in the list.  If the item has an '@id' property
        and that identifier is already in the list, the identifier will be replaced with a new one. (Use
        :py:method:`move` to move an item already in the list to that position.)  
        
        :return:  the ID assigned to metadata
                  :rtype: str
        """
        id = md.get('@id')
        if not id or id in self:
            id = self._get_default_id_for(md)
        else:
            self._reserve_id(id)
        self._set_item(id, md, pos)
        return id

    def append(self, md: Mapping) -> str:
        """
        add a new item to the end of this list.  If the item has an '@id' property and that 
        identifier is already in the list, the identifier will be replaced with a new one. (Use 
        :py:method:`move` to move an item already in the list to the end.)

        :param Mapping md:  a dictionary containing the metadata describing a single item
        :return:  string giving the identifier assigned to this item.
        """
        return self.insert(self.count, md)

    def replace_all_with(self, md: List[Mapping]):
        """
        replace the current list of items with the given list.  The currently saved items will 
        first be removed, and then the given items will be added in order.
        """
        if not isinstance(md, list):
            raise TypeError("replace_all_with(): md is not a list")
        self.empty()
        for item in md:
            self.append(item)

    def pop(self, key):
        """
        remove and return an item from the list.  This method, along with :py:method:`insert` or 
        :py:method:`append`, can be used to move an item in the list; however, :py:method:`move` 
        would be more efficient.  
        :param str|int key:  the id or position of the item to be removed
        """
        if isinstance(key, int):
            itm = self.get_author_by_pos(key)
            key = itm.get('@id')
        return self._remove_item(key)

    @abstractmethod
    def move(self, idorpos: str, pos: int = None, rel: int = 0) -> int:
        """
        move an item currently in the list to a new position.  The `rel` parameter allows one to 
        push an item up or down in the order.  

        :param idorpos:  the target item to move, either as the string identifier or its current 
                         position
        :param int pos:  the new position of the item (where `rel` controls whether this is an 
                         absolute or relative position).  If the absolute position is zero or less,
                         the item will be moved to the beginning of the list; if it is a value greater
                         or equal to the number of items in the list, it will be move to the end of the
                         list.  Zero as an absolute value is the first position in the list.  If `pos`
                         is set to `None`, the item will be moved to the end of the list (regardless,
                         of the value of rel.  
        :param int|bool rel:  if value evaluates as False (default), then `pos` will be interpreted
                         as an absolute value.  Otherwise, it will be treated as a position 
                         relative to its current position.  A positive number will cause the item 
                         to be move `pos` places toward the end of the list; a negative number moves 
                         it toward the beginning of the list.  Any other non-numeric value that 
                         evaluates to True behaves as a value of +1.  
        :raises KeyError:  if the target item is not found in the list
        :return:  the new (absolute) position of the item after the move (taking into `rel`).
                  :rtype: int
        """
        raise NotImplementedError()

        
class NERDAuthorList(_NERDOrderedObjectList):
    """
    an interface to the list of authors of a NERDm Resource.  

    This list supports the semantics of both a list (i.e. a `MutableSequence`), where an author can 
    be accessed via its position index, and a dictionary (i.e. a `MutableMapping`), where an author 
    can be accessed via its unique identifier.  (The :py:method:`__contains__` method follows 
    dictionary semantics; that is, `id in authors` evaluates to `True` if the string identifier, `id`,
    is currently assigned to an author in the the :py:class:`NERDAuthorList`, `authors`. 
    
    The current order of the authors is given by the :py:property:`ids` property, a list of the saved 
    author identifiers in their intended order.  Generally, new authors are added to 
    the end of the list, but they can be reordered via the in a two-step process: first, retrieving the 
    list of authors via :py:property:`ids`, followed by a call to :py:method:`set_order`, passing in 
    the reordered list of identifiers.  
    """
    def get_author_by_id(self, id: str):
        """
        return the author description with the given identifier assigned to it.  
        :raise KeyError: if an author with the given identifier is not part of this list
        """
        return self._get_item_by_id(id)

    def get_author_by_pos(self, pos: int):
        """
        return the author description with the given identifier assigned to it.  
        :raise KeyError: if an author with the given identifier is not part of this list
        """
        return self._get_item_by_pos(pos)

    def _get_default_id_for(self, md):
        out = md.get('orcid')
        if out:
            out = re.sub(r'^https://orcid.org/', 'doi:', out)
        if not out:
            out = self._new_id()
        return out


class NERDRefList(_NERDOrderedObjectList):
    """
    an interface to the list of authors of a NERDm Resource

    This list supports the semantics of both a list (i.e. a `MutableSequence`), where a reference can 
    be accessed via its position index, and a dictionary (i.e. a `MutableMapping`), where a reference
    can be accessed via its unique identifier.  (The :py:method:`__contains__` method follows 
    dictionary semantics; that is, `id in refs` evaluates to `True` if the string identifier, `id`,
    is currently assigned to a reference in the the :py:class:`NERDRefList`, `refs`. 
    
    The current order of the references is given by the :py:property:`ids` property, a list of the saved 
    author identifiers in their intended order.  Generally, new references are added to 
    the end of the list, but they can be reordered via the in a two-step process: first, retrieving the 
    list of references via :py:property:`ids`, followed by a call to :py:method:`set_order`, passing in 
    the reordered list of identifiers.  
    """

    def get_reference_by_id(self, id: str):
        """
        return the author description with the given identifier assigned to it.  
        :raise KeyError: if an author with the given identifier is not part of this list
        """
        return self._get_item_by_id(id)

    def get_reference_by_pos(self, pos: int):
        """
        return the author description with the given identifier assigned to it.  
        :raise KeyError: if an author with the given identifier is not part of this list
        """
        return self._get_item_by_pos(pos)

    def _get_default_id_for(self, md):
        out = md.get('doi')
        if out:
            out = re.sub(r'^https://doi.org/', 'doi:', out)
        if not out:
            out = self._new_id()
        return out

    @abstractmethod
    def _new_id(self):
        raise NotImplementedError()
        

class NERDNonFileComps(_NERDOrderedObjectList):
    """
    an interface to the list of non-file NERDm components.  A non-file component is any component that 
    is neither a downloadable file or a Subcollection folder; non-file components do not have a 
    `filepath` property.

    This list supports the semantics of both a list (i.e. a `MutableSequence`), where a component can 
    be accessed via its position index, and a dictionary (i.e. a `MutableMapping`), where a component
    can be accessed via its unique identifier.  (The :py:method:`__contains__` method follows 
    dictionary semantics; that is, `id in cmps` evaluates to `True` if the string identifier, `id`,
    is currently assigned to a component in the the :py:class:`NERDNonFileComps`, `cmps`. 
    
    The current order of the non-file components is given by the :py:property:`ids` property, a list 
    of the saved component identifiers in their intended order.  Generally, new components are added to 
    the end of the list, but they can be reordered via the in a two-step process: first, retrieving the 
    list of components via :py:property:`ids`, followed by a call to :py:method:`set_order`, passing in 
    the reordered list of identifiers.  
    """
    def get_component_by_id(self, id: str):
        """
        return the author description with the given identifier assigned to it.  
        :raise KeyError: if an author with the given identifier is not part of this list
        """
        return self._get_item_by_id(id)

    def get_component_by_pos(self, pos: int):
        """
        return the author description with the given identifier assigned to it.  
        :raise KeyError: if an author with the given identifier is not part of this list
        """
        return self._get_item_by_pos(pos)

    def _get_default_id_for(self, md):
        return self._new_id()


class NERDFileComps(metaclass=ABCMeta):
    """
    an interface to the list of file components. A file component is any component that 
    is either a downloadable file or a Subcollection folder; a file component must have a 
    `filepath` property.

    This list supports the semantics of both a list (i.e. a `MutableSequence`), where a component can 
    be accessed via its position index, and a dictionary (i.e. a `MutableMapping`), where a component
    can be accessed via its unique identifier.  (The :py:method:`__contains__` method follows 
    dictionary semantics; that is, `id in cmps` evaluates to `True` if the string identifier, `id`,
    is currently assigned to a component in the the :py:class:`NERDFileComps`, `cmps`. 
    
    The current order of the file components is given by the :py:property:`ids` property, a list 
    of the saved component identifiers in their intended order.  Files can have a specified, preferred 
    order within its subcollection; it is updated with :py:method:`set_order_in_subcoll`.  

    This interface does not automatically ensure parent subcollections for files with deep file paths.
    In particular, most applications will require that the parent subcollection exists first before 
    the files can be put into it. 
    """
    def __init__(self, resource: NERDResource, iscollf=None):
        self._res = resource
        if not iscollf:
            iscollf = self.file_object_is_subcollection
        self.is_collection = iscollf

    @staticmethod
    def file_object_is_subcollection(md):
        """
        return True if the given description is recognized as describing a subcollection.  
        False is returned if the object lacks any marker indicating its type.
        """
        return nerdmutils.is_type(md, "Subcollection")

    @abstractmethod
    def get_file_by_id(self, id: str) -> Mapping:
        """
        return the component in the file list that has the given (location-independent) identifier
        :raises ObjectNotFound:  if no file exists in this set with the given identifier
        """
        raise NotImplementedError()

    @abstractmethod
    def get_file_by_path(self, path: str) -> Mapping:
        """
        return the component that is currently at the given path location, or None if no file is 
        currently found there.  
        :raises ObjectNotFound:  if no file exists in this set with the given identifier
        """
        raise NotImplementedError()

    @abstractproperty
    def ids(self) -> [str]:
        """
        the list of identifiers for the files in this list 
        """
        raise NotImplementedError()

    @property
    def count(self) -> int:
        """
        the total number of file components (including subcollections) in this dataset
        """
        return len(self.ids)

    def get_files(self) -> [Mapping]:
        """
        return the full hierarchy of files as a single flat list
        """
        return [self.get(d) for d in self.ids]

    def get(self, idorpath):
        try:
            return self.get_file_by_id(idorpath)
        except ObjectNotFound:
            return self.get_file_by_path(idorpath)

    @abstractmethod
    def get_ids_in_subcoll(self, collpath: str) -> [str]:
        """
        return a list of the identifiers of the direct members of the specified collection in the 
        prefered order of display.  If no order has been set via :py:method:`set_order_in_subcoll`,
        the returned order will be arbitrary.  
        :param str collpath:  the file path to the subcollection or an empty string for the top 
                              collection to provide the members for.  If the path does not exist or 
                              is not a subcollection, an empty list is returned.
        """
        raise NotImplementedError()

    def get_subcoll_members(self, collpath: str) -> Iterator[Mapping]:
        """
        return a list of the metadata descriptions of a subcollection's direct members
        :param str collpath:  the file path to the subcollection or an empty string for the top 
                              collection to provide the members for.  If the path does not exist or 
                              is not a subcollection, an empty list is returned.
        """
        for id in self.get_ids_in_subcoll(collpath):
            yield self.get_file_by_id(id)

    @abstractmethod
    def set_order_in_subcoll(self, collpath: str, ids: Iterable[str]) -> Iterable[str]:
        """
        set the preferred order of the files within a subcollection.  One should first call 
        :py:method:`get_ids_in_subcoll` or :py:method:`get_subcoll_members` to ensure having a 
        complete list.  Identifiers for files that are not direct members of the specified 
        subcollection will be ignored, and missing identifiers will be appended in an arbitrary
        order after the given list.  
        :param str collpath:  the file path to the subcollection or an empty string for the top 
                              collection to set the member order for.  
        :param Iterable[str] ids:  the list of identifiers of the subcollection in the desired order
        :return:   the newly ordered list of identifiers (accounting for missing or inappropriate ids)
                   :rtype: Iterable[str]
        """
        raise NotImplementedError()

#    @abstractmethod
#    def unset_order_in_subcollection(self, collpath: str):
#        """
#        remove the order specification for the member files of a specified subcollection so that the 
#        preferred listing is arbitrary.
#        :param str collpath:  the file path to the subcollection or an empty string for the top 
#                              collection to unset the member order for.  
#        """
#        raise NotImplementedError()

    @abstractmethod
    def set_file_at(md, filepath: str=None, id=None, as_coll: bool=None) -> str:
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

        :param Mapping   md:  the file component metadata for the file to add
        :param str filepath:  the file path to insert the file into.  If provided, it will override
                              the `md`'s `filepath` property; otherwise, the `filepath` property
                              will be assumed as the insert location.  
        :param str       id:  the identifier to assign to the file metadata.  If provided, it will 
                              override the `@id` property in `md`; otherwise, the `@id` will be used.
                              If neither is provided, one will be assigned to it.
        :param bool as_coll:  True if the metadata is meant to describe a subcollection.  If True, 
                              the `@type` property will be set appropriately for a subcollection.
        :return: the identifier assigned to the file component
                 :rtype: str
        :raises FilepathNotSpecified: if the file path is not set via `filepath` parameter and cannot 
                 otherwise be determined via the previous or updated metadata.  
        :raises ObjectNotFound:  if the implementation requires that parent subcollections already exist
                 but does not.  
        """
        raise NotImplementedError()

    def move(self, idorpath: str, filepath: str) -> str:
        """
        move a file currently in the list to a new location in the hierarchy by giving it a new 
        file path.
        :param str idorpath:  the file component to move, referred to either by its identifier or
                              its file path.  The file is first searched for by identifier; if 
                              not found, the value is interpreted as a file path string.
        :return: the identifier for the moved file
                 :rtype: str
        """
        try:
            comp = self.get_file_by_id(idorpath)
        except ObjectNotFound:
            comp = None
        if not comp:
            try:
                comp = self.get_file_by_path(idorpath)
                if not comp:
                    raise ObjectNotFound(idorpath)
            except ObjectNotFound as ex:
                raise ObjectNotFound(idorpath, message="Failed to move file: "+str(ex))
            
        if comp.get('filepath') == filepath:
            return comp.get('@id')

        try:
            dest = self.get_file_by_path(filepath)  # may raise ObjectNotFound
            if self.is_collection(dest):
                filepath = "/".join([ dest['filepath'], self._basename(comp['filepath']) ])
        except ObjectNotFound:
            pass

        # there may well be a more efficient way to do this (via the subclass)
        self.set_file_at(comp, filepath, comp.get('@id'))
        # member order?
        return comp.get('@id')

    @abstractmethod
    def delete_file(self, id: str) -> bool:
        """
        remove a file from this set.  
        :returns:  False if the file was not found in this collection; True, otherwise
                   :rtype: bool
        :raises CollectionRemovalDissallowed:  if the id points to a non-empty collection
        """
        raise NotImplementedError()

    @abstractmethod
    def empty(self):
        """
        remove all files and folders from this collection of file components
        """
        raise NotImplementedError()

    @abstractmethod
    def exists(self, id: str) -> bool:
        """
        return True if the stored files include one with the given identifier
        """
        raise NotImplementedError()

    @abstractmethod
    def path_exists(self, filepath: str) -> bool:
        """
        return True if the stored files include one with the given filepath
        """
        raise NotImplementedError()

    def __contains__(self, idorpath: str) -> bool:
        return self.exists(idorpath) or self.path_exists(idorpath)

    @abstractmethod
    def path_is_collection(self, filepath: str) -> bool:
        """
        return True if the stored files include a collection with the given filepath
        """
        raise NotImplementedError()

    @staticmethod
    def _basename(filepath):
        return filepath.rsplit('/', 1)[-1]

    @staticmethod
    def _dirname(filepath):
        return filepath.rsplit('/', 1)[0]

    def __iter__(self):
        return iter(self.ids)

    def __len__(self):
        return self.count
    

class NERDStorageException(Exception):
    """
    a general base exception for problems storing metadata
    """
    pass

class MismatchedIdentifier(NERDStorageException):
    """
    an exception indicating that a stated or requested identifier does not match the identifier 
    for the record being manipulated.
    """
    def __init__(self, reqid=None, curid=None, message=None):
        """
        instantiate the exception
        :param str reqid:  the requested identifier (or the one attached to an input record)
        :param str curid:  the identifier currently associated with the record or item
        :param str message:  a custom message explaining the problem; if not given, a custom one
                           is generated
        """
        if not message:
            message = "Given id"
            if self.reqid:
                message += "=%s" % self.reqid
            message += " does not match current record id"
            if self.curid:
                message += ", %s" % self.curid
        super(MismatchedIdentifier, self).__init__(message)
        self.requested_id = reqid
        self.current_id = curid

class RecordDeleted(NERDStorageException):
    """
    an exception resulting from an attempt to update or save metadata after the record has been deleted
    """
    def __init__(self, id=None, opname=None, message=None):
        """
        initialize the exception
        :param str id:      the identifier of the deleted record
        :param str opname:  the name of the operation being attempted after deletion
        :param str message: a custom message explaining the problem; if not given, one is generated.
        """
        if not message:
            message = ""
            if id:
                message = "%s: " % id
            message += "Unable to perform operation"
            if opname:
                message += ", %s" % opname
            message += "; record has been deleted"
        super(RecordDeleted, self).__init__(message)
        self.id = id
        self.opname = opname


class ObjectNotFound(NERDStorageException):
    """
    an exception indicating that a requested object within a record could be found
    """
    def __init__(self, key=None, message=None):
        """
        :param str id: the requested object key (identifier or other index) used to locate the object
        """
        if not message:
            message = "NERDm record object not found"
            if key:
                message += ": %s" % key
        super(ObjectNotFound, self).__init__(message)

class CollectionRemovalDissallowed(NERDStorageException):
    """
    an exception indicating an attempt to delete or overwrite a collection is disallowed.  This is typically 
    disallowed if doing so would subsequently throw away the contents in the collection.
    """
    def __init__(self, path: str=None, reason: str=None, message: str=None):
        if not message:
            message = ""
            if path:
                message += "%s: " % path
            message += "Removal of collection is disallowed"
            if reason:
                message += ": %s" % reason
        super(CollectionRemovalDissallowed, self).__init__(message)
        self._path = path

class StorageFormatException(NERDStorageException):
    """
    an exception indicating that data in the underlying storage appears missing when expected or 
    otherwise corrupted.
    """
    pass

class NERDResourceStorage(ABC):
    """
    a factory function that creates or opens existing stored NERDm Resource records
    """

    @classmethod
    def from_config(cls, config: Mapping, logger: Logger):
        """
        an abstract class method for creatng NERDResourceStorage instances
        :param dict config:  the configuraiton for the specific type of storage
        :param Logger logger:  the logger to use to capture messages
        """
        raise NotImplementedError()
        
    @abstractmethod
    def open(self, id: str=None) -> NERDResource:
        """
        Open a resource record having the given identifier.  If a record with `id` does not exist (or 
        was deleted), a new record should be created and the identifier assigned to it.  If `id` is 
        None, a new identifier is created and assigned.
        """
        raise NotImplementedError()

    @abstractmethod
    def load_from(self, rec: Mapping, id: str=None):
        """
        place an existing NERDm Resource record into storage.
        :param Mapping rec:  the NERDm Resource record as a JSON-ready dictionary
        :param str id:       the identifier to assign to this record; if None, the `@id` property
                             will be used or a new one is created and assigned if it is not set.
                             If provided and a record with that identifier already exists in the 
                             storage, it will be replaced.
        """
        raise NotImplementedError()

    @abstractmethod
    def exists(self, id: str) -> bool:
        """
        return True if there is a record in the storage with the given identifier
        """
        raise NotImplementedError()

    @abstractmethod
    def delete(self, id: str) -> bool:
        """
        delete the record with the given identifier from the storage.  If the record with that identifer
        does not exist, do nothing (except return False)
        :return:  True if the record existed before being deleted; False, otherwise.
        """
        raise NotImplementedError()

    
