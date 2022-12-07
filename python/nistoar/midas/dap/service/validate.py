"""
validation utilities specialized for DAP editing
"""
from nistoar.nerdm.validate import *
from nistoar.nerdm.constants import core_schema_base as CORE_SCHEMA_BASE

PUB_SCHEMA_BASE = CORE_SCHEMA_BASE + "pub/"

class LenientSchemaLoader(ejs.SchemaLoader):
    """
    this modifies the schema definitions on selected schemas to be more lenient for records 
    intended for use in the DAP Authoring API.
    """
    def load_schema(self, uri):
        out = super().load_schema(uri)

        if out.get("id"):
            if out["id"].startswith(CORE_SCHEMA_BASE+"v"):
                # this is the core NERDm schema: drop the "required" property from the
                # Resource schema definition
                sch = out.get("definitions",{}).get("Resource",{})
                if "required" in sch:
                    del sch["required"]

            elif out["id"].startswith(CORE_SCHEMA_BASE+"rls/"):
                # this is the pub NERDm extension schema: drop the "required" property from the
                # PublicDataResource schema definition
                sch = out.get("definitions",{}).get("ReleasedResource",{}).get("allOf", [{},{}])
                if len(sch) > 1 and "required" in sch[1]:
                    del sch[1]["required"]

            elif out["id"].startswith(PUB_SCHEMA_BASE):
                # this is the pub NERDm extension schema: drop the "required" property from the
                # PublicDataResource schema definition
                sch = out.get("definitions",{}).get("PublicDataResource",{}).get("allOf", [{},{}])
                if len(sch) > 1 and "required" in sch[1]:
                    del sch[1]["required"]

        return out

def create_lenient_validator(schemadir, ejsprefix="_"):
    """
    return a validator instance (ejsonschema.ExtValidator) that can validate
    NERDm records, but which is slightly more lenient for NERDm schemas.  
    This is intended for use with the DAP Authoring Service in which 
    records are permitted to be more incomplete.

    The Validator assumes a particular prefix (usually "_" or "$") for 
    identifying the so-called "metaproperties" that are used for validation.
    This can be set by the forprefix parameter.  

    :param str schemadir:  the directory where the NERDm schemas are cached
    :param forprefix:      Either a single character ("_" or "$") or a NERDm 
                           data record used to determine the metaproperty 
                           convention.  If the value is a Mapping, it is 
                           assumed to be a NERDm record that contains 
                           metaproperties beginning either with "_" or "$";
                           which ever convention this record appears to be 
                           using will be the prefix assumed.  
    """
    if isinstance(forprefix, Mapping):
        forprefix = get_mdval_flavor(forprefix) or "_"
    if not isinstance(forprefix, (str, unicode)):
        raise TypeError("create_validator: forprefix: not a str or dict")

    loader = LenientSchemaLoader.from_directory(schemadir)

    return ejs.ExtValidator.with_schema_dir(loader, forprefix)


    


