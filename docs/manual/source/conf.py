# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'nistoar'
copyright = '2025, NIST'
author = 'Ray Plante et al.'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.apidoc',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
]

templates_path = ['_templates']
exclude_patterns = []

apidoc_modules = [
    {'path': '../../metadata/python/nistoar', 'destination': 'source/apidoc/'},
    {
        'path': '../../python/nistoar',
        'destination': 'source/apidoc/',
        'exclude_patterns': ['**/test*'],
        'max_depth': 4,
        'follow_links': False,
        'separate_modules': False,
        'include_private': False,
        'no_headings': False,
        'module_first': False,
        'implicit_namespaces': False,
        'automodule_options': {
            'members', 'show-inheritance', 'undoc-members'
        },
    },
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'nature'
html_static_path = ['_static']
