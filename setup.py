#!/usr/bin/env python

from setuptools import setup, find_packages

dependencies = ['sqlalchemy', 'pandas']

setup(
    name="surechembl_mini_client",
    version="1.0",
    url='',
    license='MIT',
    author='Aretas Gaspariunas',
    author_email='aretasgasp@gmail.com',
    description='SureChEMBL data mini client for retrieval of compound structures.',
    platforms='any',
    zip_safe=False,
    long_description=open('README.md').read(),
    packages=['surechembl_mini_client'],
    install_requires=dependencies,
    python_requires='>=3.7.2',
    entry_points={
    'console_scripts':'surechembl_mini_client = surechembl_mini_client:main'
    },
    classifiers=[
        'Development Status :: Beta',
        'Intended Audience :: Developers :: Scientists',
        'Intended Audience :: Science',
        'Operating System :: Linux :: macOS :: Windows',
        'Programming Language :: Python :: 3',
        'Topic :: Scientific/Engineering :: chemical patents :: database :: EBI :: SureChEMBL'
    ]
)
