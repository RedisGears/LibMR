#!/usr/bin/env python2

import sys
import os
import argparse

ROOT = HERE = os.path.abspath(os.path.dirname(__file__))
READIES = os.path.join(ROOT, "deps/readies")
sys.path.insert(0, READIES)
import paella

#----------------------------------------------------------------------------------------------

class LibMRSetup(paella.Setup):
    def __init__(self, nop=False, with_python=True):
        paella.Setup.__init__(self, nop=nop)
        self.with_python = with_python

    def common_first(self):
        self.install_downloaders()

        self.pip_install("wheel")
        self.pip_install("setuptools --upgrade")

        self.install("git openssl")

    def debian_compat(self):
        self.run("%s/bin/getgcc" % READIES)
        self.install("autotools-dev autoconf libtool")

        self.install("lsb-release")

    def redhat_compat(self):
        self.run("%s/bin/getgcc --modern" % READIES)
        self.install("autoconf automake libtool")

        self.install("redhat-lsb-core")
        self.install("zip unzip")
        self.install("libatomic file")
        self.install("python2-devel")

    def fedora(self):
        self.run("%s/bin/getgcc" % READIES)

    def linux_last(self):
        self.install("valgrind")

    def macos(self):
        self.install("make libtool autoconf automake")

        self.install("openssl readline coreutils")
        if not self.has_command("redis-server"):
            self.install("redis")
        self.install("binutils") # into /usr/local/opt/binutils
        self.install_gnu_utils()

    def common_last(self):
        self.run("{PYTHON} {READIES}/bin/getrmpytools".format(PYTHON=self.python, READIES=READIES))

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for LibMR build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
args = parser.parse_args()

LibMRSetup(nop = args.nop, with_python=args.with_python).setup()
