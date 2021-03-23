
from setuptools import find_packages, setup

with open("requirements-production.txt", 'r') as f:
    req = f.readlines()

setup(
    name='marketing-fatigue',
    packages=find_packages(exclude=['data', 'output', 'notebooks', 'model_objects']),
    version='0.1.0',
    description='Model customer marketing fatigue and return predictions as to whether they will be eligible comm activity in the coming days',
    author='Damian Rumble',
    install_requires=req
)
