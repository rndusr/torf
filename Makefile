clean:
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -delete
	rm -rf dist
	rm -rf .cache  # py.test junk
	rm -rf docs/_build

test:
	python3 -m pytest --exitfirst tests
