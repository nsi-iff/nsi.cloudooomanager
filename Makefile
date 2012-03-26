PYTHON=python

all: install run_unit_test

install_cyclone:
	@rm -Rf cyclone
	git clone git://github.com/fiorix/cyclone.git
	cd cyclone && $(PYTHON) setup.py install
	@rm -Rf cyclone

install_restfulie:
	@rm -Rf restfulie
	git clone git://github.com/caelum/restfulie-py.git restfulie
	cd restfulie && $(PYTHON) setup.py install
	@rm -Rf restfulie

install: install_cyclone install_nsimultimedia
	$(PYTHON) setup.py develop

run_unit_test:
	cd nsicloudooomanager/tests && $(PYTHON) testInterface.py && $(PYTHON) testAuth.py

