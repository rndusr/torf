VENV_PATH?=venv

clean:
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -delete
	rm -rf dist build
	rm -rf .pytest_cache
	rm -rf "$(VENV_PATH)"

venv:
	python3 -m venv "$(VENV_PATH)"
	"$(VENV_PATH)"/bin/pip install --upgrade pytest wheel
	"$(VENV_PATH)"/bin/pip install --editable .
	"$(VENV_PATH)"/bin/pip install --editable ../torf-cli

test: venv
	. "$(VENV_PATH)"/bin/activate ; \
	"$(VENV_PATH)"/bin/pytest --exitfirst tests

release:
	pyrelease CHANGELOG ./torf/_version.py
