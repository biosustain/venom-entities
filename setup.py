from setuptools import find_packages, setup


setup(
    name='Venom-Entities',
    version='0.0.0',
    packages=find_packages(exclude=['*tests*']),
    license='MIT',
    author='Lars Schöning',
    author_email='lays@biosustain.dtu.dk',
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=[
        'flask-venom',
        'flask-sqlalchemy'
    ],
    test_suite='nose.collector',
    tests_require=[
        'flask-testing'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
