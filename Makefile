VENV_PATH?=venv
PYTHON?=python3
PYTHON36_VERSION?=3.6.10
PYTHON36_PREFIX?=$(shell pwd)/python36
PYTHON36_BUILD?=python36.build

clean:
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -delete
	rm -rf dist build
	rm -rf .pytest_cache
	rm -rf "$(VENV_PATH)"
	rm -rf "$(PYTHON36_BUILD)"

venv:
	"$(PYTHON)" -m venv "$(VENV_PATH)"
	"$(VENV_PATH)"/bin/pip install --upgrade pytest pytest-xdist pytest-httpserver
	"$(VENV_PATH)"/bin/pip install --editable .
	"$(VENV_PATH)"/bin/pip install --editable ../torf-cli

python36:
	sudo apt-get install -y wget xz-utils build-essential llvm \
	  libssl-dev zlib1g-dev libbz2-dev liblzma-dev \
	  libreadline-dev libncurses5-dev libncursesw5-dev libffi-dev
	mkdir -p "$(PYTHON36_BUILD)" ; cd "$(PYTHON36_BUILD)" ; \
	  wget -c https://www.python.org/ftp/python/"$(PYTHON36_VERSION)"/Python-"$(PYTHON36_VERSION)".tar.xz ; \
	  [ -e Python-"$(PYTHON36_VERSION)" ] || tar xvf Python-"$(PYTHON36_VERSION)".tar.xz ; \
	  cd Python-"$(PYTHON36_VERSION)" ; \
	  ./configure --prefix="$(PYTHON36_PREFIX)" ; make build_all -j8 ; make altinstall

test: venv
	. "$(VENV_PATH)"/bin/activate ; \
	"$(VENV_PATH)"/bin/pytest

release:
	pyrelease CHANGELOG ./torf/_version.py
