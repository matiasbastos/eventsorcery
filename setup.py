from setuptools import setup

setup(name='eventsorcery',
      version='0.1',
      description='Simple and pythonic eventsourcing ORM.',
      url='http://github.com/matiasbastos/eventsorcery',
      author='Matias Bastos, Imre Guzmics',
      author_email='matias.bastos@gmail.com, gizmo.mac@gmail.com',
      license='MIT',
      packages=[],
      extras_require={
        'sqlalchemy':  ['SQLAlchemy==1.2.14', 'pudb==2018.1']
      },
      zip_safe=False)
