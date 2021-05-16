import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="mysql-simplelib",
    version="0.1",
    author="Daniel E. Caballero",
    author_email="danielcaballero88@gmail.com",
    description="Simple MySQL library for Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/danielcaballero88/mysql-simplelib",
    project_urls={
        "Bug Tracker": "https://github.com/danielcaballero88/mysql-simplelib/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
    install_requires=[
        'mysql-connector-python',
        'python-dotenv'
    ]
)