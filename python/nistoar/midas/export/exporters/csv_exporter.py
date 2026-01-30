from __future__ import annotations
from typing import Any, Optional
from collections.abc import Mapping as MappingABC
from .base import Exporter
import preppy

DEFAULT_CSV_TEMPLATE = "dmp_csv_template.prep"  # Fallback to DMP template for compatibility


class CSVExporter(Exporter):
    format_name = "csv"
    file_extension = ".csv"

    def render(self, input_type: str, payload: Any, filename: str, template_name: str = None):
        """
        Render a single input payload into a single output file in the format requested.
        Input type must match one of the cases or a type error is raised.

        Args:
            input_type: Initial format of the data.
            payload: Content the exporter can handle.
            filename: Base name for the rendered result (without extension).
            template_name: Optional template filename the exporter uses.

        Returns: A dictionary.
        """
        if input_type == "json":
            return self.render_json(payload, filename, template_name)

        raise TypeError("CSVExporter.render: unsupported payload type.")

    def render_json(self, json_payload: Any, filename: str, template_name: str = None):
        """
        Render a single JSON input into a single CSV output
        """
        import csv
        import io
        
        # Handle both single record and array of records
        if isinstance(json_payload, list) and len(json_payload) > 0:
            # Take the first record from the array
            record = json_payload[0]
        elif isinstance(json_payload, MappingABC):
            record = json_payload
        else:
            # Handle object records (like _FakeProjectRecord) - should be rare since normalize_input handles this
            if hasattr(json_payload, 'data'):
                record = {
                    'id': getattr(json_payload, 'id', 'N/A'),
                    'name': getattr(json_payload, 'name', 'N/A'),
                    'owner': getattr(json_payload, 'owner', 'N/A'),
                    'status': getattr(json_payload, 'status', {}),
                    'data': getattr(json_payload, 'data', {}),
                    'meta': getattr(json_payload, 'meta', {})
                }
            else:
                raise TypeError("CSVExporter expects a mapping-like payload, array of records, or object with data attribute")

        # Handle legacy data-only format for backward compatibility (should be rare now)
        if 'data' in record and len(record) == 1:
            # This is the old data-only format, reconstruct record structure from data
            data = record['data']
            record = {
                'id': data.get('id', data.get('@id', 'N/A')),  # Try @id for DAP records
                'name': data.get('name', data.get('title', 'N/A')),  # Use title as fallback for name
                'owner': 'N/A',  # Cannot recover from data-only
                'status': {},  # Cannot recover from data-only
                'data': data,
                'meta': {}  # Cannot recover from data-only
            }
        
        # Determine record type from template name or ID
        is_dap = (template_name and "dap" in template_name.lower()) or (record.get("id", "").startswith("mds3"))
        
        # Create CSV content
        output = io.StringIO()
        writer = csv.writer(output)
        
        if is_dap:
            # DAP CSV headers and data
            headers = [
                "Name", "ID", "Type", "Owner", "Status_State", "Status_Action", 
                "Status_CreatedDate", "Status_ModifiedDate", "Status_Message", 
                "Title", "DOI", "Schema", "ContactPoint_Name", "ContactPoint_Email", 
                "Keywords", "Theme", "Authors", "References", "File_Count", 
                "Nonfile_Count", "Meta_ResourceType", "Meta_AssocPageType"
            ]
            writer.writerow(headers)
            
            data = record.get("data", {})
            status = record.get("status", {})
            meta = record.get("meta", {})
            
            # Handle status object properly - it might be a RecordStatus object or dict
            if hasattr(status, 'get'):
                # It's a dictionary
                status_state = status.get("state", "N/A")
                status_action = status.get("action", "N/A")
                status_created = status.get("createdDate", "N/A")
                status_modified = status.get("modifiedDate", "N/A")
                status_message = status.get("message", "N/A")
            else:
                # It's a RecordStatus object
                status_state = getattr(status, "state", "N/A")
                status_action = getattr(status, "action", "N/A")
                status_created = getattr(status, "createdDate", "N/A")
                status_modified = getattr(status, "modifiedDate", "N/A")
                status_message = getattr(status, "message", "N/A")
            
            row = [
                record.get("name", "N/A"),
                record.get("id", "N/A"),
                "DAP",
                record.get("owner", "N/A"),
                status_state,
                status_action,
                status_created,
                status_modified,
                status_message,
                data.get("title", "N/A"),
                data.get("doi", "N/A"),
                data.get("_schema", "N/A"),
                data.get("contactPoint", {}).get("fn", "N/A"),
                data.get("contactPoint", {}).get("hasEmail", "N/A"),
                "; ".join(data.get("keywords", [])),
                "; ".join(data.get("theme", [])),
                "; ".join([author.get("fn", "N/A") for author in data.get("authors", [])]),
                "; ".join([ref.get("title", ref.get("citation", "N/A")) for ref in data.get("references", [])]),
                data.get("file_count", 0),
                data.get("nonfile_count", 0),
                meta.get("resourceType", "N/A"),
                meta.get("assocPageType", "N/A")
            ]
        else:
            # DMP CSV headers and data
            headers = [
                "Name", "ID", "Type", "Owner", "Status_State", "Status_Action", 
                "Status_CreatedDate", "Status_ModifiedDate", "Status_Message", 
                "Title", "ProjectDescription", "StartDate", "DmpSearchable", 
                "Grant_Source", "Grant_ID", "Keywords", "DataSize", "SizeUnit", 
                "SoftwareDevelopment", "TechnicalResources", "DataDescription", 
                "DataCategories", "PreservationDescription"
            ]
            writer.writerow(headers)
            
            data = record.get("data", {})
            status = record.get("status", {})
            
            # Handle status object properly - it might be a RecordStatus object or dict
            if hasattr(status, 'get'):
                # It's a dictionary
                status_state = status.get("state", "N/A")
                status_action = status.get("action", "N/A")
                status_created = status.get("createdDate", "N/A")
                status_modified = status.get("modifiedDate", "N/A")
                status_message = status.get("message", "N/A")
            else:
                # It's a RecordStatus object
                status_state = getattr(status, "state", "N/A")
                status_action = getattr(status, "action", "N/A")
                status_created = getattr(status, "createdDate", "N/A")
                status_modified = getattr(status, "modifiedDate", "N/A")
                status_message = getattr(status, "message", "N/A")
            
            row = [
                record.get("name", "N/A"),
                record.get("id", "N/A"),
                "DMP",
                record.get("owner", "N/A"),
                status_state,
                status_action,
                status_created,
                status_modified,
                status_message,
                data.get("title", "N/A"),
                data.get("projectDescription", "N/A"),
                data.get("startDate", "N/A"),
                data.get("dmpSearchable", "N/A"),
                data.get("funding", {}).get("grant_source", "N/A"),
                data.get("funding", {}).get("grant_id", "N/A"),
                "; ".join(data.get("keywords", [])),
                data.get("dataSize", "N/A") if data.get("dataSize") is not None else "N/A",
                data.get("sizeUnit", "N/A"),
                data.get("softwareDevelopment", {}).get("development", "N/A"),
                "; ".join(data.get("technicalResources", [])),
                data.get("dataDescription", "N/A"),
                "; ".join(data.get("dataCategories", [])),
                data.get("preservationDescription", "N/A")
            ]
        
        writer.writerow(row)
        csv_text = output.getvalue()
        output.close()

        return {
            "format": self.format_name,
            "filename": f"{filename}{self.file_extension}",
            "mimetype": "text/csv",
            "text": csv_text,
            "file_extension": self.file_extension,
        }
