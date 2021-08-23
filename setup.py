import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
	name = "rfkadapter",
	version = "0.3.1",
	author = "Malik Koljenović",
	author_email = "malik@mekom.ba",
	description = "RFKAdapter emulates a SQL-like CRUD API for DBF",
	long_description = long_description,
	long_description_content_type = "text/markdown",
	url = "https://github.com/koljenovic/rfk",
	project_urls = {
		"Bug Tracker": "https://github.com/koljenovic/rfk/issues"
	},
	classifiers =[
		"Programming Language :: Python :: 3",
		"License :: OSI Approved :: MIT License",
		"Operating System :: OS Independent",
		"Development Status :: 4 - Beta",
		"Natural Language :: Bosnian",
		"Natural Language :: English",
		"Programming Language :: Other",
		"Topic :: Software Development :: Libraries :: Python Modules",
		"Topic :: Database :: Front-Ends"
	],

	package_dir = { "": "src" },
	packages = setuptools.find_packages(where="src"),
	python_requires = ">=3.7",
)