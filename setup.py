import re
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="IMC",
    version='1.0.0',
    author="Andrew Lahiff",
    author_email="andrew.lahiff@ukaea.uk",
    description="Cloud deployment for PROMINENCE",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://prominence-eosc.github.io/docs",
    platforms=["any"],
    install_requires=["requests", "paramiko", "psycopg2", "psutil", "flask"],
    package_dir={'': '.'},
    scripts=["bin/imc-cleaner", "bin/imc-deployer", "bin/imc-destroyer", "bin/imc-manager", "bin/imc-restapi"],
    packages=['imc'],
    package_data={"": ["README.md"]},
)
