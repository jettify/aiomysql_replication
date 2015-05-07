# Some simple testing tasks (sorry, UNIX only).

FLAGS=


flake:
	flake8 aiomysql_replication tests examples

test: export PYTHONASYNCIODEBUG=1
test: flake
	nosetests -s $(FLAGS) ./tests/

vtest: export PYTHONASYNCIODEBUG=1
vtest:
	nosetests -s -v $(FLAGS) ./tests/

cov cover coverage: flake
	nosetests -s --with-cover --cover-html --cover-branches $(FLAGS) --cover-package aiomysql_replication ./tests/
	@echo "open file://`pwd`/cover/index.html"

clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -f `find . -type f -name '*~' `
	rm -f `find . -type f -name '.*~' `
	rm -f `find . -type f -name '@*' `
	rm -f `find . -type f -name '#*#' `
	rm -f `find . -type f -name '*.orig' `
	rm -f `find . -type f -name '*.rej' `
	rm -f .coverage
	rm -rf coverage
	rm -rf build
	rm -rf cover
	rm -rf dist

doc:
	make -C docs html
	@echo "open file://`pwd`/docs/_build/html/index.html"

.PHONY: all flake test vtest cov clean doc
