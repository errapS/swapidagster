from setuptools import find_packages, setup

setup(
    name="dagster_python",
    packages=find_packages(exclude=["dagster_python_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
