from setuptools import setup, find_packages

with open('requirements.txt') as reqs_file:
    requirements = reqs_file.read().splitlines()

setup(
    name='timeseries_replay',
    version='0.1dev',
    license='MIT',
    author='Brian Law',
    long_description=open('README.md').read(),

    ### requirements
    install_requires=requirements,

    ### Packages for packaging up
    packages=find_packages(
        include=['timeseries_replay'],
        exclude=['tests*']
        )
)