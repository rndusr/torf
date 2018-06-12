clean:
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -delete
	rm -rf dist
	rm -rf .pytest_cache .cache

test:
	python3 -m pytest --tb no tests

