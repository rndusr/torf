clean:
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -delete
	rm -rf dist
	rm -rf .cache  # py.test junk
	rm -rf docs/_build

test:
	python3 -m pytest --exitfirst tests

doc:
	@rm -rf ./build
	@rm -r ./docs/index.html ./docs/_static/*
	@sphinx-build -M singlehtml ./docs ./build
	@mv ./build/singlehtml/index.html ./build/singlehtml/_static ./docs
	@rm -r ./build
