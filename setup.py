#!/usr/bin/env python

import setuptools

requirements = ["apache-airflow", "requests"]

setuptools.setup(
    name="airflow_mixpanel",
    version="0.1.0",
    description="Hooks and operators for Mixpanel API.",
    author="Uziel Linares",
    author_email="uziel.linares@protonmail.com",
    install_requires=requirements,
    packages=setuptools.find_packages("src"),
    package_dir={"": "src"},
    url="https://github.com/ulinares/airflow-mixpanel",
    license="MIT license",
)
