VENV_PATH?=venv
PYTHON?=python3

clean:
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -delete
	rm -rf dist build
	rm -rf .pytest_cache
	rm -rf .tox
	rm -rf "$(VENV_PATH)"

venv:
	"$(PYTHON)" -m venv "$(VENV_PATH)"
	"$(VENV_PATH)"/bin/pip install --upgrade wheel tox pytest pytest-xdist pytest-httpserver
	"$(VENV_PATH)"/bin/pip install --upgrade wheel tox pytest flake8 isort
	"$(VENV_PATH)"/bin/pip install --editable .
	"$(VENV_PATH)"/bin/pip install --editable ../torf-cli
	# Dependencies for `setup.py check -r -s`
	"$(VENV_PATH)"/bin/pip install --upgrade docutils pygments

test: venv
	. "$(VENV_PATH)"/bin/activate ; \
	  "$(VENV_PATH)"/bin/pytest --exitfirst tests --file-counts 1,2
	# Check if README.org converts correctly to rst for PyPI
	. "$(VENV_PATH)"/bin/activate ; \
	  "$(PYTHON)" setup.py check -r -s >/dev/null

fulltest: venv
	. "$(VENV_PATH)"/bin/activate ; \
	  tox
	. "$(VENV_PATH)"/bin/activate ; \
	  flake8 torf tests
	. "$(VENV_PATH)"/bin/activate ; \
	  isort --recursive torf tests

release:
	pyrelease CHANGELOG ./torf/__init__.py
