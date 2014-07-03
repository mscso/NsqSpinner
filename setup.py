import setuptools
import os.path

import nsq

app_path = os.path.dirname(nsq.__file__)

with open(os.path.join(app_path, 'resources', 'README.rst')) as f:
      long_description = f.read()

with open(os.path.join(app_path, 'resources', 'requirements.txt')) as f:
      install_requires = map(lambda s: s.strip(), f)

setuptools.setup(
      name='nsq',
      version=nsq.__version__,
      description="A greenlet-based NSQ client.",
      long_description=long_description,
      classifiers=[],
      keywords='',
      author='Dustin Oprea',
      author_email='myselfasunder@gmail.com',
      url='https://github.com/dsoprea/NsqSpinner',
      license='GPL 2',
      packages=setuptools.find_packages(exclude=['dev']),
      include_package_data=True,
      zip_safe=False,
      install_requires=install_requires,
      package_data={
          'nsq': ['resources/README.rst',
                  'resources/requirements.txt'],
      },
      scripts=[
      ],
)
