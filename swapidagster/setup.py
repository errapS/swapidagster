from setuptools import find_packages, setup

setup(
    name="swapidagster",
    version="0.0.1",
    packages=find_packages(),
    package_data={
        "swapidagster": [
            "dbt-project/**/*",
        ],
    },
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-core>=1.4.0",
        "dbt-postgres",
        "dbt-fal",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)

