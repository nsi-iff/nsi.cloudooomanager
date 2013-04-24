from setuptools import setup, find_packages
import sys, os

version = '0.2.5'

setup(name='nsi.cloudooomanager',
      version=version,
      description="A package to intercept all calls to a CloudOOO Server.",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Douglas Camata',
      author_email='d.camata@gmail.com',
      url='',
      license='GPL',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=True,
      install_requires=[
          # -*- Extra requirements: -*-
          'twisted',
          'zope.interface',
          'nsi.metadataextractor'
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
