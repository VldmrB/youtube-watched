import subprocess
import sys
from os import path
from shutil import rmtree

from setuptools import setup, find_packages, Command

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

cur_dir = path.abspath(path.dirname(__file__))


class UploadCommand(Command):

    # based on https://github.com/kennethreitz/setup.py/blob/master/setup.py
    description = 'Build and publish the package.'
    user_options = []

    def initialize_options(self):
        pass

    def run(self):
        dist_dir_path = path.join(cur_dir, 'dist')
        if path.exists(dist_dir_path):
            print('Removing old dist/ directory')
            try:
                if not self.dry_run:
                    rmtree(dist_dir_path)
            except OSError:
                print('Failed to delete the dist/ directory')
                raise

        print('Building source distribution and wheel')
        if self.dry_run:
            subprocess.run(f'{sys.executable} setup.py -n sdist bdist_wheel')
        else:
            subprocess.run(f'{sys.executable} setup.py sdist bdist_wheel')

        print('Uploading to test PyPi...')
        if not self.dry_run:
            subprocess.run(f'twine upload --repository testpypi dist/*')

    def finalize_options(self):
        pass


setup(
    name=NAME,
    version='0.1.0rc1',
    author='Vladimir Belitskiy',
    author_email='belitskiy@gmail.com',
    description='Visualization of Youtube watch history from Google Takeout',
    long_description=open(path.join(cur_dir, 'README.md'), 'r').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/VldmrB/youtube-watched',
    packages=find_packages(),
    license='MIT',
    python_requires='>=3.6.0',
    install_requires=requires,
    include_package_data=True,
    keywords='visualization youtube takeout',
    entry_points=f'''[console_scripts]
                     {NAME}={NAME}.__main__:launch''',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Operating System :: OS Independent',
        'Framework :: Flask'
    ],
    cmdclass={'upload': UploadCommand}
)
