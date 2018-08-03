# distro for package building (oneof: wily, fedora-23-x86_64)
DISTRIBUTION        	?= centos-7-x86_64
FPM_DOCKER_IMAGE        ?= docker.onedata.org/fpm:1.9.3
PKG_BUILD       	?= 1

IOREPLAY_VERSION = 0.2


.PHONY: all
all: deb

##
## Submodules
##

.PHONY: submodules
submodules:
	git submodule sync --recursive ${submodule}
	git submodule update --init --recursive ${submodule}

.PHONY: tarball
tarball:
	docker run -u=`id -u`:`id -g` -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro \
		   -v `pwd`:/data onedata/ioreplay_builder:latest \
		   bash -c "cd /data/ ; pyinstaller -F ./ioreplay/ioreplay.py ; cd ./dist ; tar -czvf ioreplay.tar.gz ioreplay"

##
## ioreplay self contained packages
##
## These targets create self-contained ioreplay packages for various distributions,
## and packages them into /opt/ioreplay/... tree.
##

## Determine the RPM dist tag based on distribution name
ifeq ($(strip $(DISTRIBUTION)),centos-6-x86_64)
RPM_DIST     := el6.centos
endif
ifeq ($(strip $(DISTRIBUTION)),centos-7-x86_64)
RPM_DIST     := el7.centos
endif
ifeq ($(strip $(DISTRIBUTION)),fedora-23-x86_64)
RPM_DIST     := f23
endif
ifeq ($(strip $(DISTRIBUTION)),fedora-25-x86_64)
RPM_DIST     := f25
endif
ifeq ($(strip $(DISTRIBUTION)),fedora-26-x86_64)
RPM_DIST     := f26
endif
ifeq ($(strip $(DISTRIBUTION)),scientific-6-x86_64)
RPM_DIST     := el6
endif
ifeq ($(strip $(DISTRIBUTION)),scientific-7-x86_64)
RPM_DIST     := el7
endif

#
# Build ioreplay self-contained deb
#
.PHONY: ioreplay_deb
ioreplay_deb: tarball
	# Build DEB package for the distribution specified using FPM
	cp pkg_config/fpm/ioreplay_deb.pre ./dist/
	cp pkg_config/fpm/ioreplay_deb.post ./dist/
	docker run -u=`id -u`:`id -g` -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro \
		   -v `pwd`/dist:/data \
		   -t $(FPM_DOCKER_IMAGE) \
		   fpm -t deb -s tar \
		   --architecture=amd64 \
		   --prefix=/opt/ioreplay -n ioreplay -v $(IOREPLAY_VERSION) \
		   --iteration $(PKG_BUILD) --license "Apache 2.0" \
		   --after-install=/data/ioreplay_deb.pre \
		   --after-remove=/data/ioreplay_deb.post \
		   --maintainer "Onedata Package Maintainers <info@onedata.org>" \
		   --description "Self-contained Onedata ioreplay command-line client package" \
		   /data/ioreplay.tar.gz

.PHONY: package_xenial
package_xenial: ioreplay_deb
	rm -rf package
	mkdir -p package/xenial/binary-amd64
	cp dist/*.deb package/xenial/binary-amd64/
	tar -zcvf xenial.tar.gz package

#
# Build ioreplay self-contained rpm
#
.PHONY: ioreplay_rpm
ioreplay_rpm: tarball
	# Build RPM package for the distribution specified using FPM
	cp pkg_config/fpm/ioreplay_rpm.pre ./dist/
	cp pkg_config/fpm/ioreplay_rpm.post ./dist/
	docker run -u=`id -u`:`id -g` -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro \
		   -v `pwd`/dist:/data \
		   -t $(FPM_DOCKER_IMAGE) \
		   fpm -t rpm --rpm-dist $(RPM_DIST) -s tar \
		   --prefix=/opt/ioreplay -n ioreplay -v $(IOREPLAY_VERSION) \
		   --architecture=x86_64 \
		   --iteration $(PKG_BUILD) --license "Apache 2.0" \
		   --after-install=/data/ioreplay_rpm.pre \
		   --after-remove=/data/ioreplay_rpm.post \
		   --maintainer "Onedata Package Maintainers <info@onedata.org>" \
		   --description "Self-contained Onedata ioreplay command-line client package" \
		   /data/ioreplay.tar.gz

.PHONY: package_centos
package_centos: ioreplay_rpm
	rm -rf package
	mkdir -p package/centos-7-x86_64/x86_64
	mkdir -p package/centos-7-x86_64/SRPMS
	cp dist/*.rpm package/centos-7-x86_64/x86_64/
	tar -zcvf centos7.tar.gz package

##
## Clean
##

.PHONY: clean
clean:
	rm -rf deb_dist || true
	rm -rf package || true
	rm -rf build || true
	rm -rf dist || true
	rm -rf ioreplay.egg-info || true
