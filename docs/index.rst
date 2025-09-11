brodata Documentation
=====================

**brodata** is a Python package for downloading and processing subsurface data from **DINO** and **BRO**.

The "Basisregistratie Ondergrond" (BRO) is the Dutch national database for subsurface data, maintained by the **TNO Geological Survey of the Netherlands**, on behalf of the Dutch government. The growing database is accessible via web services that provides data in XML format. The **brodata** package simplifies the process of querying, downloading, and processing this data.

**brodata** is build and maintained by **Artesia**. The source-code is available on `GitHub <https://github.com/ArtesiaWater/brodata>`__, where users can post issues or suggest improvements. In the summer of 2025, the "**ministerie van Volkshuisvesting en Ruimtelijke Ordening**" (VRO) made a one-time donation to improve the documentation of this package.

.. raw:: html

    <div align="center" style="display: flex; gap: 40px; justify-content: center;">

.. image:: _static/logo_bro.jpg
    :width: 200
    :alt: The logo of the BRO
    :target: none

.. image:: _static/logo_rijksoverheid.svg
    :width: 200
    :alt: The logo of the Rijksoverheid
    :target: none

.. raw:: html

    </div>

Installation
------------

You can install the package using `pip` from `PyPI <https://pypi.org/project/brodata/>`__:

.. code-block:: shell

    pip install brodata

About This Documentation
------------------------

This website serves as the documentation for the **brodata** package. Use the menu on the left to navigate through the following sections:

- **Package** – General information about the package setup.
- **Examples** – Demonstrations of how to use the package, including how to download groundwater time-series and drilling data, and how to visualize them.
- **API Docs** – Detailed documentation of classes and methods available in **brodata**, also accessible via your Python interpreter.

Contents
--------

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   Package <package>
   Examples <examples>
   API Docs <modules>

Indices and Tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
