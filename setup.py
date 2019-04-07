from os import path
from setuptools import setup, find_packages

NAME = 'youtubewatched'

requires = [
    'beautifulsoup4==4.7.1',
    'dash==0.40.0',
    'Flask==1.0.2',
    'google-api-python-client==1.7.8',
    'lxml==4.3.3',
    'numpy==1.16.2',
    'pandas==0.24.2',
    'plotly==3.7.1'
]

with open(path.join(path.dirname(__file__), 'README.md'), 'r') as readme:
    long_description = readme.read()

setup(
    name=NAME,
    version='0.1.0',
    author='Vladimir Belitskiy',
    author_email='belitskiy@gmail.com',
    description='Visualization of Youtube watch history from Google '
                'Takeout',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/VldmrB/youtube-watched',
    packages=find_packages(),
    license='MIT',
    python_requires='>=3.6.0,<3.8',
    install_requires=requires,
    package_data={NAME: ['static/*', 'templates/*']},
    include_package_data=True,
    keywords='visualization youtube takeout',
    entry_points='''[console_scripts]
                    youtubewatched=youtubewatched.main:launch''',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Framework :: Flask'
    ],
)

