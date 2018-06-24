clean:
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -delete
	rm -rf dist build
	rm -rf .pytest_cache .cache

test:
	python3 -m pytest --tb no tests

release:
	pyrelease CHANGELOG ./torf/_version.py
