"""
A simple archive database that stores NERDm records as flat files
"""
import json, re, os
from pathlib import Path
from copy import deepcopy

from nistoar.id.versions import OARVersion
from nistoar.pdr.describe.rmm import MetadataClient as rmm

releasesets = rmm.COLL_RELEASES
versions = rmm.COLL_VERSIONS
records = rmm.COLL_LATEST
_collections = [records, versions, releasesets]
arkpfxre = re.compile(r'^ark:/\d+/')
dotre = re.compile(r'\.')

class SimDb():

    def __init__(self, archdir, readonly=True):
        self.dir = Path(archdir)
        if not self.dir.is_dir():
            raise ValueError(f"{archdir}: archive path does not exist as a directory")

        self.ro = readonly
        if not readonly:
            for subdir in [records, versions, releasesets]:
                subpath = self.dir/subdir
                if not subpath.exists():
                    subpath.mkdir()

        self.loadall()

    def loadlu(self, colldir):
        lu = {}
        if not (self.dir/colldir).is_dir():
            return lu
        for recf in [f for f in os.listdir(self.dir/colldir) if f.endswith(".json")]:
            try:
                mdf = self.dir/colldir/recf
                with open(mdf) as fd:
                    data = json.load(fd)
                if "@id" in data:
                    lu[data['@id']] = mdf
                    if colldir != versions:
                        lu[arkpfxre.sub('', data['@id']).split('/')[0]] = mdf
                        if data.get('ediid'):
                            lu[arkpfxre.sub('', data['ediid'])] = mdf

            except:
                pass

        return lu

    def loadall(self):
        self.records = self.loadlu(rmm.COLL_LATEST)
        self.versions = self.loadlu(rmm.COLL_VERSIONS)
        self.releasesets = self.loadlu(rmm.COLL_RELEASES)

    def add_rec_from_file(self, recf):
        with open(recf) as fd:
            self.add_rec(json.load(fd))

    def add_to_coll(self, coll, rec):
        if self.ro:
            raise RuntimeError("DB is set read-only")
        colldir = self.dir/coll
        if not colldir.is_dir():
            raise ValueError(f"{coll}: not a recognized collection")
        aipid = arkpfxre.sub('', rec.get('ediid', ''))
        if not aipid:
            raise ValueError("No ediid property set in input record")
        if not rec.get('@id'):
            raise ValueError("No ediid property set in input record")
        mdf = aipid
        if coll == versions:
            vers = rec.get('version')
            if not vers:
                raise ValueError("No version property set")
            mdf += f"-v{dotre.sub('_', str(vers))}"
        mdf += ".json"
        mdf = colldir/mdf

        with open(mdf, 'w') as fd:
            json.dump(rec, fd, indent=2)

        getattr(self, coll)[rec['@id']] = mdf
        if coll != versions:
            getattr(self, coll)[aipid] = mdf
            getattr(self, coll)[arkpfxre.sub('', rec['@id'])] = mdf

    def add_rec(self, rec):
        if self.ro:
            raise RuntimeError("DB is set read-only")
        try:
            pdrid = rec['@id']
            aipid = arkpfxre.sub('', rec['ediid'])
            vers = OARVersion(rec['version'])
        except KeyError as ex:
            raise ValueError(f"Not a NERDm record; missing {str(ex)} property")

        rec = deepcopy(rec)
        if not rec.get('releaseHistory'):
            rec['releaseHistory'] = {
                "@id": rec['@id']+"/pdr:v",
                "version": rec['version'],
                "hasRelease": [
                    {
                        "version": rec['version'],
                        "issued": rec.get("issued", "2026-01-01"),
                        "@id": rec['@id']+"/pdr:v"+rec['version'],
                        "location": "https://data.nist.gov/od/id/"+rec['@id']+"/pdr:v"+rec['version'],
                        "description": "initial version"
                    }
                ]
            }
        
        # load version-specific record
        rec['@id'] += "/pdr:v/"+rec['version']
        self.add_to_coll(versions, rec)
        # vertag = f"-v{dotre.sub('_', str(vers))}"
        # mdf = self.dir/versions/(aipid+vertag+".json")
        # with open(mdf, 'w') as fd:
        #     json.dump(rec, fd, indent=2)
        # self.versions[rec['@id']] = mdf
        rec['@id'] = pdrid

        # load latest record
        latestf = self.dir/records/(aipid+".json")
        load = True
        if latestf.is_file():
            with open(latestf) as fd:
                latestr = json.load(fd)
            if vers < OARVersion(latestr['version']):
                load = False
        if load:
            # self.add_to_coll(records, rec)
            with open(latestf, 'w') as fd:
                json.dump(rec, fd, indent=2)
            self.records[aipid] = latestf
            self.records[rec['@id']] = latestf
            self.records[arkpfxre.sub('', rec['@id'])] = latestf
                    
        # load release history record
        latestf = self.dir/releasesets/(aipid+".json")
        load = True
        if latestf.is_file():
            with open(latestf) as fd:
                latestr = json.load(fd)
                if vers < OARVersion(latestr['version']):
                    load = False
        if load:
            rec['@id'] += "/pdr:v"
            if rec.get('components'):
                del rec['components']
            rec.update(rec['releaseHistory'])
            del rec['releaseHistory']

            # self.add_to_coll(releasesets, rec)
            with open(latestf, 'w') as fd:
                json.dump(rec, fd, indent=2)
            self.releasesets[aipid] = latestf
            self.releasesets[rec['@id']] = latestf
            self.records[arkpfxre.sub('', rec['@id'])] = latestf

    def get_rec_from(self, coll, id):
        fname = getattr(self, coll).get(id)
        if fname and fname.is_file():
            return self._get_from_file(fname)
        return None

    def _get_from_file(self, fname):
        with open(fname) as fd:
            return json.load(fd)

    def _record_files_from(self, coll):
        colldir = self.dir/coll
        if not colldir.is_dir():
            raise StopIteration()
        for f in os.listdir(colldir):
            if f.endswith(".json"):
                yield colldir/f

    def iter(self, coll=records):
        for f in self._record_files_from(coll):
            yield self._get_from_file(f)

    def get_all_from(self, coll=records):
        return list(self.iter(coll))

    def get_aipids_from(self, coll=records):
        if coll==versions:
            return [arkpfxre.sub('', r.get('ediid', ''))+f"-v{dotre.sub('_', r.get('version',''))}"
                    for r in self.iter(coll)]
        else:
            return [arkpfxre.sub('', r.get('ediid', '')) for r in self.iter(coll)]

