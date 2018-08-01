.PHONY: all deb package-xenial clean


all: deb

##
## Submodules
##

submodules:
	git submodule sync --recursive ${submodule}
	git submodule update --init --recursive ${submodule}

##
## DEB packaging
##

deb:
	docker run -v `pwd`:/data onedata/ioreplay_builder \
    bash -c 'cd /data && \
             python3 setup.py --command-packages=stdeb.command bdist_deb'

package-xenial: deb
	mkdir -p package/xenial/binary-amd64
	mkdir -p package/xenial/source
	mkdir -p package/
	cp deb_dist/*.deb package/xenial/binary-amd64/
	cp deb_dist/*.changes package/xenial/source
	cp deb_dist/*.tar.[gx]z package/xenial/source
	cp deb_dist/*.dsc package/xenial/source
	tar -zcvf xenial.tar.gz package

##
## Clean
##

clean:
	rm -rf deb_dist || true
	rm -rf package || true
	rm -rf dist || true
	rm -rf ioreplay.egg-info || true
