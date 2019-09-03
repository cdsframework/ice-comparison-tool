# ice-comparison-tool

A tool built using the Python ICE Client (pyiceclient) to compare the
output of ICE to another immunization forecaster.

This tool is primarily intended to serve as an example for how an
Immunization Information System might compare its existing forecaster
to ICE.

Installation
============

* Python 3.6+ environment
* pip install cx_Oracle
* pip install xmltodict
* pip install requests
* Install https://bitbucket.org/cdsframework/pyiceclient per its README
* Copy ice-compare.sample.ini to ice-compare.ini and set values

Database
========

Expects registry data to be in an Oracle database, with child,
immunization, evaluation, and recommendation tables. Expects
registry's forecaster results to be cached in the evaluation and
recommendation tables.

Running the tool
================

python ice-compare.py

