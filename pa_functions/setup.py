from setuptools import setup, find_packages

setup(
    name='pa_functions',
    version='1.0.1',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'numpy',
        'requests',
        'gspread',
        'google-auth',
        'google-api-python-client',
        'seaborn',
        'matplotlib'
    ],
)