from setuptools import setup, find_packages

setup(
    name="hhnk-fewspy",
    version="2023.1",
    description="HHNK FEWS tools",
    url="https://github.com/hhnk/hhnk-fewspy",
    author="Wietse van Gerwen",
    author_email="w.vangerwen@hhnk.nl",
    maintainer="Wietse van Gerwen",
    project_urls={
        "Bug Tracker": "https://github.com/hhnk/hhnk-fewspy/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    # package_dir={'':'hhnk_threedi_tools'},
    packages=find_packages(),
    # packages=find_packages("", exclude=['tests']),
    python_requires=">=3.7",
    # install_requires=["hhnk-research-tools==2023.1", "xarray", "pytest"],
    # setup_requires=['setuptools_scm'],
    include_package_data=True,
)
