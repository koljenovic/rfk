import setuptools

with open("README", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
	name = "rfkadapter",
	version = "0.2.1",
	author = "Malik Koljenović",
	author_email = "malik@mekom.ba",
	description = "RFKAdapter emulira API nalik SQL jeziku (SELECT, UPDATE, CREATE) za DBF \"baze podataka\"",
	long_description = long_description,
	long_description_content_type = "text/markdown",
	url = "https://gitea.lab.ba/mekom/rfk",
	project_urls = {
		"Bug Tracker": "https://gitea.lab.ba/mekom/rfk/issues"
	},
	classifiers =[
		"Programming Language :: Python :: 3",
		"License :: OSI Approved :: MIT License",
		"Operating System :: OS Independent",
	],

	package_dir = { "": "src" },
	packages = setuptools.find_packages(where="src"),
    install_requires = [
        "aenum",
    ],
	python_requires = ">=3.7",
)
