# How To Use This Space To Manage Your Files

The folder containing this file is the space where you can upload and organize files that will be part of your data publication.

### Quick Guide

  * To add files, drag-and-drop them into the folder window or click the "New+" button.
  * To add a subfolder, click on the "New+" button.
  * To move a file to a subfolder, drag-and-drop them on the subfolder icon.
  * To hide a file and keep it from being included it in the publication, move it to the `#HIDE` subfolder.
  * To schedule a file for future automatic removal, move it to the `#TRASH` subfolder.

## Disallowed and Ignored Files

A publication is prohibited from including any files that begin with "." or "#"; thus, any files uploaded into this space beginning with those characters will be ignored.

You may see two folders in your space called `#HIDE` and `#TRASH`.  Because their names begin with "#", they will be ignored and not considered part of your publication.  If you've uploaded a file that you later decide you can delete, move it into the `#TRASH` folder; it will be automatically deleted after a grace period.  If you do not want to delete a file but rather just exclude it from being considered part of the publication, move the file to the `#HIDE` folder.

You can add `#HIDE` or `#TRASH` folders inside any of the subfolders you create, and they will behave in the same way.  

## Revising the file contents

If your Digital Asset Publication (DAP) has been published once already and you are now revising, this space will look a little different.  Any folders that were part of the previously published version will be recreated in your space; however, the files they contained have _not_ been restored.  Instead, these folders will contain special files called `#previously_published_files.tsv`; these list the names of the files that were published previously in that folder.

_Do not re-upload files that have not changed since your last publication._  Instead, do any of the following:
  * To add a new file, just upload it to the folder where you want it to appear
  * To update a file, upload the new version of the file in the same folder it was published in before.  Make sure that it has the same name, or it will be treated as a new file.
  * To remove a previously published file from the publication:
     1.  edit the `#previously_published_files.tsv` file in the folder containing the file to remove,
     2.  in the line listing the name of the file you want to remove, change `#keep` to `#remove`,
     3.  click the check mark icon to save the file.
  * To move a file to a different folder, you must remove it from `#previously_published_files.tsv` in its old folder, and then upload a copy of the file to new folder.

Note that after you publish a DAP, the files in this space are kept on disk afterwards for a limited grace period before they are removed.  If you try to revise the record before the end of the grace period, the files will get moved to a folder called `#OLD`.  This allows you to update selected files and move them into appropriate folders as part of your revision.  

## Other Tips and Recommendations


