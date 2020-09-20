# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# -*- coding: utf-8 -*-

import sys
import os
from datetime import date

# eventlet/gevent should not monkey patch anything.
os.environ["GEVENT_NOPATCH"] = "yes"
os.environ["EVENTLET_NOPATCH"] = "yes"
#os.environ["CELERY_LOADER"] = "default"

this = os.path.dirname(os.path.abspath(__file__))

# If your extensions are in another directory, add it here. If the directory
# is relative to the documentation root, use os.path.abspath to make it
# absolute, like shown here.
sys.path.append(os.path.join(os.pardir, "tests"))
sys.path.append(os.path.join(this, "_ext"))
#import celery

# General configuration
# ---------------------

# Add any Sphinx extension module names here, as strings. They can be extensions
# coming with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = [
  'sphinx.ext.autodoc',
  #~ 'sphinx.ext.autosummary',
  'sphinx.ext.inheritance_diagram',
  'sphinx.ext.todo',
  'sphinx.ext.extlinks',
  'sphinx.ext.graphviz',
  'sphinx.ext.intersphinx',
  #'rst2pdf.pdfbuilder',
  #~ 'sphinx.ext.doctest',
]

html_show_sphinx = False

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The suffix of source filenames.
source_suffix = '.rst'

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = 'Apache Flume'
copyright = '2009-%s The Apache Software Foundation. Apache Flume, Flume, Apache, the Apache feather logo, and the Apache Flume project logo are trademarks of The Apache Software Foundation.' % date.today().year

keep_warnings = True

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
#version = ".".join(map(str, celery.VERSION[0:2]))
# The full version, including alpha/beta/rc tags.
#release = celery.__version__

exclude_trees = ['.build']

# If true, '()' will be appended to :func: etc. cross-reference text.
add_function_parentheses = True

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
#add_module_names = True

#intersphinx_mapping = {
#  "http://docs.python.org/dev": None,
#  "http://kombu.readthedocs.org/en/latest/": None,
#  "http://django-celery.readthedocs.org/en/latest": None,
#}

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'trac'
highlight_language = 'none'

html_theme_path = ['_themes']

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
#html_static_path = ['../resources/images']
html_static_path = ['images/feather-small.png']

html_logo = 'images/flume-logo.png'

html_use_smartypants = True

# If false, no module index is generated.
html_use_modindex = False

# If false, no index is generated.
html_use_index = False

html_theme = 'flume'

html_theme_options = {
    'sidebarbgcolor': '#3076B3',
    'sidebartextcolor': 'White',
    'headtextcolor': '#4466AA',
    'footerbgcolor': 'White',
    'footertextcolor': 'Black',
    'headerbgcolor': '#f9f9f9',
    'headertextcolor': '#4466AA',
}

html_title = 'Apache Flume'

#html_sidebars = {
#  '**': ['localtoc.html', 'relations.html', 'sourcelink.html'],
#}

# Custom sidebar templates, maps document names to template names.
html_sidebars = {
   '**': ['globaltoc.html', 'extranav.html', 'localtoc.html'],
}

pdf_documents = [
  ('FlumeUserGuide', 'FlumeUserGuide.tex', 'Flume User Guide', 'Apache Flume'),
  ('FlumeDeveloperGuide', 'FlumeDeveloperGuide.tex', 'Flume Developer Guide', 'Apache Flume'),
]
