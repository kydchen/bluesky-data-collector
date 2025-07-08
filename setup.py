from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="bluesky-data-collector",
    version="1.2.4",
    author="Bluesky Data Collection Tool",
    description="A comprehensive tool for collecting data from Bluesky using the ATP API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kydchen/bluesky-data-collector",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Internet :: WWW/HTTP :: Dynamic Content",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "bluesky-collector=main:main",
        ],
    },
    keywords="bluesky, atp, api, data collection, social media, research",
    project_urls={
        "Bug Reports": "https://github.com/kydchen/bluesky-data-collector/issues",
        "Source": "https://github.com/kydchen/bluesky-data-collector",
        "Documentation": "https://github.com/kydchen/bluesky-data-collector#readme",
    },
) 