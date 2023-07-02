VENV_PATH?=venv
PYTHON?=python3

clean:
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -delete
	rm -rf dist build
	rm -rf .pytest_cache
	rm -rf .tox
	rm -rf .coverage .coverage.*
	rm -rf "$(VENV_PATH)" *.egg-info

venv:
	"$(PYTHON)" -m venv "$(VENV_PATH)"
	"$(VENV_PATH)"/bin/pip install --editable '.[dev]'
