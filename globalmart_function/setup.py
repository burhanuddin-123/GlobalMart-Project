from setuptools import setup


setup(
    name='globalmart_api',
    version='0.1',
    description='Fetch the data from the GlobalMart API and helps us to ingest and process the data via dataframe',
    author='Burhanuddin',
    packages=['globalmart_api'],
    install_requires=['pandas', 'numpy']
)